package worker

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

// cgroupSampleInterval is how often memory is sampled while an action runs. A sample reads two small files, which
// takes about 15 microseconds, and an action that ends before the first sample reports no rise.
var cgroupSampleInterval = 25 * time.Millisecond

// memoryCgroupDir is the directory of this process's cgroup v2 memory files, or "" if unavailable.
var memoryCgroupDir = sync.OnceValue(func() string {
	dir := resolveCgroupDir("/sys/fs/cgroup", "/proc/self/cgroup")
	if dir == "" {
		log.Warning("cgroup v2 memory files not found; per-action cgroup memory sampling is disabled")
	} else {
		log.Notice("Sampling per-action memory from cgroup %s", dir)
	}
	return dir
})

// resolveCgroupDir finds this process's cgroup directory under root, using the "0::" (v2) line of procCgroup.
// With a private cgroup namespace that is root itself; otherwise root is the node's hierarchy and the path is needed.
func resolveCgroupDir(root, procCgroup string) string {
	b, err := os.ReadFile(procCgroup)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		if p, ok := strings.CutPrefix(line, "0::"); ok {
			dir := filepath.Join(root, p)
			if _, err := os.Stat(filepath.Join(dir, "memory.current")); err != nil {
				return ""
			}
			return dir
		}
	}
	return ""
}

// cgroupSnapshot is a point-in-time reading of a cgroup's memory, all in bytes.
type cgroupSnapshot struct {
	current, anon, shmem, inactiveFile int64
}

// workingSet matches the kubelet's definition: usage minus inactive file cache.
func (s cgroupSnapshot) workingSet() int64 {
	return max(s.current-s.inactiveFile, 0)
}

// cgroupMemSample is the memory behaviour of the worker's cgroup over one action.
// The worker runs one action at a time, so this is the action's memory plus the worker's own.
type cgroupMemSample struct {
	limitBytes      int64 // 0 if the cgroup has no limit
	baselineWSBytes int64
	peakWSBytes     int64
	baselineAnon    int64
	peakAnonBytes   int64
	peakShmemBytes  int64
	// anon + shmem (which includes tmpfs) taken from the same reading, so it is not the sum of the two peaks.
	baselineAnonShmem  int64
	peakAnonShmemBytes int64
	maxEventsDelta     int64
	oomKillDelta       int64
}

// readCgroupMemory reads memory.current and memory.stat from dir.
func readCgroupMemory(dir string) (cgroupSnapshot, error) {
	var s cgroupSnapshot
	cur, err := os.ReadFile(filepath.Join(dir, "memory.current"))
	if err != nil {
		return s, err
	}
	if s.current, err = strconv.ParseInt(strings.TrimSpace(string(cur)), 10, 64); err != nil {
		return s, err
	}
	stat, err := os.ReadFile(filepath.Join(dir, "memory.stat"))
	if err != nil {
		return s, err
	}
	err = parseKeyed(string(stat), map[string]*int64{
		"anon":          &s.anon,
		"shmem":         &s.shmem,
		"inactive_file": &s.inactiveFile,
	})
	return s, err
}

// readCgroupEvents returns the cumulative "max" and "oom_kill" counts from memory.events.
func readCgroupEvents(dir string) (maxEvents, oomKill int64, err error) {
	b, err := os.ReadFile(filepath.Join(dir, "memory.events"))
	if err != nil {
		return 0, 0, err
	}
	err = parseKeyed(string(b), map[string]*int64{"max": &maxEvents, "oom_kill": &oomKill})
	return maxEvents, oomKill, err
}

// readCgroupLimit returns memory.max, or 0 if there is no limit or it can't be read.
func readCgroupLimit(dir string) int64 {
	b, err := os.ReadFile(filepath.Join(dir, "memory.max"))
	if err != nil {
		return 0
	}
	limit, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64) // fails for "max"
	if err != nil {
		return 0
	}
	return limit
}

// parseKeyed parses "key value" lines, storing the values of the wanted keys.
func parseKeyed(data string, want map[string]*int64) error {
	for _, line := range strings.Split(data, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		if dst, ok := want[fields[0]]; ok {
			v, err := strconv.ParseInt(fields[1], 10, 64)
			if err != nil {
				return fmt.Errorf("bad value for %s: %w", fields[0], err)
			}
			*dst = v
		}
	}
	return nil
}

// cgroupSampler measures a cgroup's memory over one action.
type cgroupSampler struct {
	dir                        string
	baseline                   cgroupSnapshot
	baseMaxEvents, baseOOMKill int64
	limit                      int64
}

// newCgroupSampler takes the baseline before the action starts.
// It returns nil if the cgroup can't be read, which start handles.
func newCgroupSampler(dir string) *cgroupSampler {
	if dir == "" {
		return nil
	}
	base, err := readCgroupMemory(dir)
	if err != nil {
		log.Warning("Failed to read cgroup memory: %s", err)
		return nil
	}
	s := &cgroupSampler{dir: dir, baseline: base, limit: readCgroupLimit(dir)}
	// The events are best effort; a missing file leaves the deltas at zero.
	s.baseMaxEvents, s.baseOOMKill, _ = readCgroupEvents(dir)
	return s
}

// start samples in the background until the returned function is called, which returns the result.
// On a nil sampler the function returns nil.
func (s *cgroupSampler) start() func() *cgroupMemSample {
	if s == nil {
		return func() *cgroupMemSample { return nil }
	}
	stop := make(chan struct{})
	done := make(chan cgroupMemSample, 1)
	// The goroutine owns the sample until it sends it on done.
	go func() {
		sample := cgroupMemSample{
			limitBytes:      s.limit,
			baselineWSBytes: s.baseline.workingSet(),
			peakWSBytes:     s.baseline.workingSet(),
			baselineAnon:    s.baseline.anon,
			peakAnonBytes:   s.baseline.anon,
			peakShmemBytes:  s.baseline.shmem,

			baselineAnonShmem:  s.baseline.anon + s.baseline.shmem,
			peakAnonShmemBytes: s.baseline.anon + s.baseline.shmem,
		}
		ticker := time.NewTicker(cgroupSampleInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				done <- sample
				return
			case <-ticker.C:
				snap, err := readCgroupMemory(s.dir)
				if err != nil {
					continue
				}
				sample.peakWSBytes = max(sample.peakWSBytes, snap.workingSet())
				sample.peakAnonBytes = max(sample.peakAnonBytes, snap.anon)
				sample.peakShmemBytes = max(sample.peakShmemBytes, snap.shmem)
				sample.peakAnonShmemBytes = max(sample.peakAnonShmemBytes, snap.anon+snap.shmem)
			}
		}
	}()
	return func() *cgroupMemSample {
		close(stop)
		sample := <-done
		// The process has exited so its memory is already freed; this only picks up the events.
		if maxEvents, oomKill, err := readCgroupEvents(s.dir); err == nil {
			sample.maxEventsDelta = maxEvents - s.baseMaxEvents
			sample.oomKillDelta = oomKill - s.baseOOMKill
		}
		return &sample
	}
}

const mb = 1 << 20

// anonRiseBytes is how far the cgroup's anonymous memory rose above its baseline while the action ran.
func (s *cgroupMemSample) anonRiseBytes() int64 {
	return max(s.peakAnonBytes-s.baselineAnon, 0)
}

// memoryRiseBytes is how far the cgroup's anonymous and shared memory rose above their baseline while the action ran.
// Shared memory includes the files in the sandbox's tmpfs, which live in memory.
func (s *cgroupMemSample) memoryRiseBytes() int64 {
	return max(s.peakAnonShmemBytes-s.baselineAnonShmem, 0)
}

// peakMemoryMB returns the memory the action used, in MB. That is the rise in the cgroup's anonymous and shared
// memory, or the largest process's maxrss if the cgroup couldn't be sampled.
func (w *worker) peakMemoryMB(rusage *syscall.Rusage) float64 {
	if s := w.memSample; s != nil {
		return float64(s.memoryRiseBytes()) / mb
	}
	return float64(rusage.Maxrss) / 1024.0 // maxrss is in kb
}

// observeCgroupMemory records the memory sampled for the action that just ran.
func (w *worker) observeCgroupMemory() {
	s := w.memSample
	if s == nil {
		return
	}
	anonRise := s.anonRiseBytes()
	logr.WithFields(logrus.Fields{
		"hash":           w.actionDigest.GetHash(),
		"stage":          "cgroup_memory",
		"limitMB":        s.limitBytes / mb,
		"baselineWSMB":   s.baselineWSBytes / mb,
		"peakWSMB":       s.peakWSBytes / mb,
		"peakAnonMB":     s.peakAnonBytes / mb,
		"anonRiseMB":     anonRise / mb,
		"peakShmemMB":    s.peakShmemBytes / mb,
		"memoryRiseMB":   s.memoryRiseBytes() / mb,
		"maxEventsDelta": s.maxEventsDelta,
		"oomKillDelta":   s.oomKillDelta,
	}).Info("Cgroup memory")
}
