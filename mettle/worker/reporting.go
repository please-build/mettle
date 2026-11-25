package worker

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/shirou/gopsutil/mem"

	lpb "github.com/thought-machine/please-servers/proto/lucidity"
)

// Report reports to Lucidity how this worker currently considers itself.
// If a Lucidity server hasn't been configured, calling this has no effect.
func (w *worker) Report(healthy, busy, alive bool, status string, args ...interface{}) {
	if w.lucidChan != nil {
		w.lucidChan <- &lpb.Worker{
			Name:          w.name,
			Version:       w.version,
			StartTime:     w.startTime.Unix(),
			Healthy:       healthy,
			Busy:          busy,
			Alive:         alive,
			Status:        fmt.Sprintf(status, args...),
			LastTask:      w.lastURL,
			CurrentTask:   w.currentTaskID(),
			TaskStartTime: w.taskStartTime.Unix(),
		}
	}
}

// currentTaskID returns the ID of the currently executing task, if there is one.
func (w *worker) currentTaskID() string {
	if w.actionDigest != nil {
		return w.actionDigest.Hash
	}
	return ""
}

// sendReports sends reports to Lucidity indefinitely.
func (w *worker) sendReports(ctx context.Context) {
	hostname, err := os.Hostname()
	if err != nil {
		log.Error("Failed to retrieve hostname: %s", err)
	}
	t := time.NewTicker(5 * time.Minute)
	var last *lpb.Worker
	for {
		select {
		case report := <-w.lucidChan:
			report.Hostname = hostname
			w.sendReport(ctx, report)
			last = report
		case <-t.C:
			if last != nil {
				w.sendReport(ctx, last)
			}
		}
	}
}

func (w *worker) sendReport(ctx context.Context, report *lpb.Worker) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if resp, err := w.lucidity.Update(ctx, report); err != nil {
		log.Warning("Failed to report status to Lucidity: %s", err)
	} else if resp.ShouldDisable {
		if !w.disabled {
			log.Warning("Server has disabled us!")
		}
		w.disabled = resp.ShouldDisable
	}
}

// waitForFreeResources checks the currently available disk space and reports unhealthy until it is under a threshold.
func (w *worker) waitForFreeResources() {
	if w.checkFreeResources() {
		return
	}
	for range time.NewTicker(1 * time.Minute).C {
		if w.checkFreeResources() {
			return
		}
	}
}

// checkFreeResources returns true if the worker currently has sufficient free disk space and memory.
func (w *worker) checkFreeResources() bool {
	return w.checkFreeSpace() && w.checkFreeMemory()
}

// checkFreeSpace returns true if the worker currently has sufficient free space.
// If not it reports unhealthy.
func (w *worker) checkFreeSpace() bool {
	statfs := syscall.Statfs_t{}
	if err := syscall.Statfs(w.rootDir, &statfs); err != nil {
		log.Error("Failed to statfs %s: %s", w.rootDir, err)
		w.Report(false, false, true, "Failed statfs: %s", err)
		return false
	} else if (statfs.Flags & rdOnly) == rdOnly {
		// This should really be ST_RDONLY but syscall doesn't define it and they happen to be the same.
		log.Error("Read-only file system")
		w.Report(false, false, true, "Filesystem has gone read-only")
		return false
	} else if avail := int64(statfs.Bsize) * int64(statfs.Bavail); avail < w.diskSpace { //nolint:unconvert
		log.Warning("Disk free space %d is under healthy threshold %d, will not accept new jobs until resolved", avail, w.diskSpace)
		w.Report(false, false, true, "Low disk space: %s free", humanize.Bytes(uint64(avail)))
		return false
	} else {
		log.Debug("Disk free space %d is over healthy threshold %d", avail, w.diskSpace)
		return true
	}
}

// checkFreeMemory returns true if we consider we have enough memory available
func (w *worker) checkFreeMemory() bool {
	if w.memoryThreshold >= 100.0 {
		log.Debug("Skipping memory check, threshold is set to %0.1f%%", w.memoryThreshold)
		return true // must always be enabled
	}
	vm, err := mem.VirtualMemory()
	if err != nil {
		log.Error("Error getting memory usage: %s", err)
		w.Report(false, false, true, "Error getting memory usage: %s", err)
		return false
	} else if vm.UsedPercent > w.memoryThreshold {
		log.Warning("Memory usage %0.1f%% is over healthy threshold %0.1f%%, will not accept new jobs until it decreases", vm.UsedPercent, w.memoryThreshold)
		w.Report(false, false, true, "High memory usage: %0.1f%% used", vm.UsedPercent)
		return false
	}
	log.Debug("Memory usage %0.1f%% is under healthy threshold %0.1f%%", vm.UsedPercent, w.memoryThreshold)
	return true
}

// waitIfDisabled waits until the server marks this worker as enabled again.
func (w *worker) waitIfDisabled() {
	if w.disabled {
		log.Warning("Waiting until we are re-enabled to accept another build...")
		for range time.NewTicker(10 * time.Second).C {
			if !w.disabled {
				log.Notice("Server has re-enabled us, continuing")
				return
			}
		}
	}
}

// waitForLiveConnection waits for the connection to be alive to the remote servers.
func (w *worker) waitForLiveConnection(ctx context.Context) {
	if w.checkLiveConnection(ctx) {
		return
	}
	t := time.NewTicker(1 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if w.checkLiveConnection(ctx) {
				return
			}
		}
	}
}

func (w *worker) checkLiveConnection(ctx context.Context) bool {
	if err := w.client.Healthcheck(ctx); err != nil {
		log.Errorf("Failed to contact remote server: %s", err)
		return false
	}
	return true
}

// checkConnectivity tries to check connectivity to an external checker. It dies if it doesn't work.
func (w *worker) checkConnectivity(check string) {
	switch check {
	case "gstatic":
		resp, err := http.Get("https://connectivitycheck.gstatic.com/generate_204")
		if err != nil {
			log.Fatalf("Failed to complete connectivity check: %s", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			log.Fatalf("Connectivity check returned unexpected status: %s", resp.Status)
		}
	case "firefox":
		resp, err := http.Get("https://detectportal.firefox.com/canonical.html")
		if err != nil {
			log.Fatalf("Failed to complete connectivity check: %s", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Fatalf("Connectivity check returned unexpected status: %s", resp.Status)
		}
	case "":
		return // no check
	default:
		log.Fatalf("unknown connectivity check type: %s", check)
	}
	log.Notice("%s connectivity check succeeded", check)
}
