// Package worker implements the worker side of Mettle.
package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"slices"
	"strings"
	"syscall"
	"time"

	psraw "cloud.google.com/go/pubsub/apiv1"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/dgraph-io/ristretto"
	"github.com/dustin/go-humanize"
	"github.com/go-redis/redis/v8"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/peterebden/go-cli-init/v4/logging"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"github.com/prometheus/common/expfmt"
	"github.com/sirupsen/logrus"
	"gocloud.dev/pubsub"
	pspb "google.golang.org/genproto/googleapis/pubsub/v1"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	mettlecli "github.com/thought-machine/please-servers/cli"
	elan "github.com/thought-machine/please-servers/elan/rpc"
	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/mettle/common"
	lpb "github.com/thought-machine/please-servers/proto/lucidity"
	"github.com/thought-machine/please-servers/rexclient"
	bbcas "github.com/thought-machine/please-servers/third_party/proto/cas"
	bbru "github.com/thought-machine/please-servers/third_party/proto/resourceusage"
)

var log = logging.MustGetLogger()
var logr = logrus.StandardLogger()

var totalBuilds = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "builds_total",
})
var currentBuilds = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "mettle",
	Name:      "builds_current",
}, []string{"mettle_instance"})
var executeDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "build_durations_secs",
	Buckets:   []float64{1, 5, 10, 30, 60, 120, 300, 600, 900, 1200},
})
var fetchDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "fetch_durations_secs",
	Buckets:   []float64{1, 2, 5, 10, 20, 50, 200, 500},
})
var uploadDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "upload_durations_secs",
	Buckets:   []float64{1, 2, 5, 10, 20, 50, 200, 500},
})
var peakMemory = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "peak_memory_usage_mb",
	Buckets:   []float64{50, 200, 500, 1000, 5000, 10000},
})
var cpuUsage = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "cpu_usage_per_sec",
	Buckets:   []float64{0.2, 0.5, 1.0, 2.0, 5.0},
})
var cacheHits = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "cache_hits_total",
})
var cacheMisses = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "cache_misses_total",
})
var blobNotFoundErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "blob_not_found_errors_total",
}, []string{"version"})
var packsDownloaded = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "packs_downloaded_total",
})
var packBytesRead = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "pack_bytes_read_total",
})
var collectOutputErrors = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "collect_output_errors_total",
})

var nackedMessages = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "nacked_messages_total",
})

var actionTimeout = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "action_timeout",
})

// ErrTimeout is returned when the build action exceeds the action timeout
var ErrTimeout = errors.New("action execution timed out")

var metrics = []prometheus.Collector{
	totalBuilds,
	currentBuilds,
	executeDurations,
	fetchDurations,
	uploadDurations,
	peakMemory,
	cpuUsage,
	cacheHits,
	cacheMisses,
	blobNotFoundErrors,
	redisHits,
	redisMisses,
	redisBytesRead,
	redisLatency,
	packsDownloaded,
	packBytesRead,
	collectOutputErrors,
	nackedMessages,
	actionTimeout,
}

func init() {
	for _, metric := range metrics {
		prometheus.MustRegister(metric)
	}
}

// RunForever runs the worker, receiving jobs until terminated.
func RunForever(instanceName, requestQueue, responseQueue, name, storage, dir, cacheDir, browserURL, sandbox, altSandbox, lucidity, promGatewayURL, tokenFile string, primaryRedis, readRedis *redis.Client, largeBlobSize int64, cachePrefix, cacheParts []string, clean, preflightAction, secureStorage bool, maxCacheSize, minDiskSpace int64, memoryThreshold float64, connCheck string, connCheckPeriod time.Duration, versionFile string, costs map[string]mettlecli.Currency, ackExtension time.Duration, immediateShutdown bool) {
	err := runForever(instanceName, requestQueue, responseQueue, name, storage, dir, cacheDir, browserURL, sandbox, altSandbox, lucidity, promGatewayURL, tokenFile, primaryRedis, readRedis, largeBlobSize, cachePrefix, cacheParts, clean, preflightAction, secureStorage, maxCacheSize, minDiskSpace, memoryThreshold, connCheck, connCheckPeriod, versionFile, costs, ackExtension, immediateShutdown)
	log.Fatalf("Failed to run: %s", err)
}

// RunOne runs one single request, returning any error received.
func RunOne(instanceName, name, storage, dir, cacheDir, sandbox, altSandbox, tokenFile string, primaryRedis, readRedis *redis.Client, largeBlobSize int64, cachePrefix, cacheParts []string, clean, secureStorage bool, digest *pb.Digest) error {
	// Must create this to submit on first
	topic := common.MustOpenTopic("mem://requests")
	w, err := initialiseWorker(instanceName, "mem://requests", "mem://responses", name, storage, dir, cacheDir, "", sandbox, altSandbox, "", "", tokenFile, primaryRedis, readRedis, largeBlobSize, cachePrefix, cacheParts, clean, secureStorage, 0, math.MaxInt64, 100.0, "", nil, 0)
	if err != nil {
		return err
	}
	defer w.ShutdownQueues()

	logr.WithFields(logrus.Fields{
		"hash": digest.Hash,
	}).Info("Queuing task")
	b, _ := proto.Marshal(&pb.ExecuteRequest{
		InstanceName: instanceName,
		ActionDigest: digest,
	})
	if err := topic.Send(context.Background(), &pubsub.Message{Body: b}); err != nil {
		log.Fatalf("Failed to submit job to internal queue: %s", err)
	}
	logr.WithFields(logrus.Fields{
		"hash": digest.Hash,
	}).Info("Queued task")

	if response, err := w.RunTask(context.Background()); err != nil {
		return fmt.Errorf("Failed to run task: %s", err)
	} else if response.Result == nil {
		return fmt.Errorf("Execution unsuccessful: %s", response.Status)
	} else if response.Result.ExitCode != 0 {
		return fmt.Errorf("Execution failed: %s", response.Message)
	}
	logr.WithFields(logrus.Fields{
		"hash": digest.Hash,
	}).Info("Completed execution successfully")
	return nil
}

func runForever(instanceName, requestQueue, responseQueue, name, storage, dir, cacheDir, browserURL, sandbox, altSandbox, lucidity, promGatewayURL, tokenFile string, primaryRedis, readRedis *redis.Client, largeBlobSize int64, cachePrefix, cacheParts []string, clean, preflightAction, secureStorage bool, maxCacheSize, minDiskSpace int64, memoryThreshold float64, connCheck string, connCheckPeriod time.Duration, versionFile string, costs map[string]mettlecli.Currency, ackExtension time.Duration, immediateShutdown bool) error {
	w, err := initialiseWorker(instanceName, requestQueue, responseQueue, name, storage, dir, cacheDir, browserURL, sandbox, altSandbox, lucidity, promGatewayURL, tokenFile, primaryRedis, readRedis, largeBlobSize, cachePrefix, cacheParts, clean, secureStorage, maxCacheSize, minDiskSpace, memoryThreshold, versionFile, costs, ackExtension)
	if err != nil {
		return err
	}
	defer w.ShutdownQueues()

	if preflightAction {
		if err := w.runPreflightAction(); err != nil {
			return err
		}
	}

	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		sig := <-ch
		if immediateShutdown {
			w.forceShutdown(fmt.Sprintf("Received shutdown signal %s, shutting down...", sig))
		}
		log.Warning("Received signal %s, shutting down when any in-progress task completes or times out", sig)
		cancel()

		for {
			select {
			case <-time.After(10 * time.Minute):
				w.forceShutdown("Exceeded timeout, shutting down immediately...")
			case sig = <-ch:
				w.forceShutdown(fmt.Sprintf("Received another signal %s, shutting down immediately...", sig))
			}
		}
	}()
	w.checkConnectivity(connCheck)
	go w.periodicallyPushMetrics()
	defer w.metricTicker.Stop()
	t := time.NewTicker(connCheckPeriod)
	defer t.Stop()
	for {
		w.waitForFreeResources()
		w.waitForLiveConnection()
		w.waitIfDisabled()

		// Run the connectivity check if the period has expired
		select {
		case <-t.C:
			w.checkConnectivity(connCheck)
		default:
		}

		// Run an explicit GC to clear up after the last task; ideally we leave as much free as
		// possible for the subprocesses.
		runtime.GC()
		w.Report(true, false, true, "Awaiting next task...")
		_, err := w.RunTask(ctx)
		if ctx.Err() != nil {
			// Error came from a signal triggered above. Give a brief period to send reports then die.
			time.Sleep(500 * time.Millisecond)
			return fmt.Errorf("terminated by signal")
		}
		if err != nil {
			// If we get an error back here, we have failed to communicate with one of
			// our queues or something else bad happened internally  so we are basically doomed
			// and should stop.
			err = fmt.Errorf("Failed to run task: %s", err)
			w.Report(false, false, false, "%s", err)
			return err
		}
	}
}

func initialiseWorker(instanceName, requestQueue, responseQueue, name, storage, dir, cacheDir, browserURL, sandbox, altSandbox, lucidity, promGatewayURL, tokenFile string, primaryRedis, readRedis *redis.Client, largeBlobSize int64, cachePrefix, cacheParts []string, clean, secureStorage bool, maxCacheSize, minDiskSpace int64, memoryThreshold float64, versionFile string, costs map[string]mettlecli.Currency, ackExtension time.Duration) (*worker, error) {
	// Make sure we have a directory to run in
	if err := os.MkdirAll(dir, os.ModeDir|0755); err != nil {
		return nil, fmt.Errorf("Failed to create working directory: %s", err)
	}
	if cacheDir != "" && (len(cachePrefix) > 0 || len(cacheParts) > 0) {
		// If we're gonna be copying to the cache dir we should make sure it exists first.
		if err := os.MkdirAll(cacheDir, os.ModeDir|0755); err != nil {
			return nil, fmt.Errorf("Failed to create cache directory: %s", err)
		}
	}
	// Remove anything existing within this directory.
	// We don't just do a RemoveAll above in case we don't have permissions to create it in the first place.
	if clean {
		files, err := ioutil.ReadDir(dir)
		if err != nil {
			return nil, err // If we can't even list the directory, may as well bomb out now.
		}
		for _, file := range files {
			if err := os.RemoveAll(path.Join(dir, file.Name())); err != nil {
				log.Warning("Failed to remove existing work directory: %s", err)
			}
		}
	}
	// If no name is given, default to the hostname.
	if name == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("Failed to determine hostname, must pass --name explicitly: %s", err)
		}
		name = hostname
		log.Notice("This is %s", name)
	}
	// Check this exists upfront
	if sandbox != "" {
		if _, err := os.Stat(sandbox); err != nil {
			return nil, fmt.Errorf("Error checking sandbox tool: %s", err)
		}
	}
	client, err := elan.New(storage, secureStorage, tokenFile)
	if err != nil {
		return nil, err
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	abspath, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("Failed to make path absolute: %s", err)
	}

	w := &worker{
		requests:        common.MustOpenSubscription(requestQueue, 1),
		responses:       common.MustOpenTopic(responseQueue),
		ackExtension:    ackExtension,
		client:          client,
		rclient:         rexclient.Uninitialised(),
		rootDir:         abspath,
		clean:           clean,
		home:            home,
		name:            name,
		sandbox:         mustAbs(sandbox),
		altSandbox:      mustAbs(altSandbox),
		limiter:         make(chan struct{}, downloadParallelism),
		iolimiter:       make(chan struct{}, ioParallelism),
		browserURL:      browserURL,
		promGatewayURL:  promGatewayURL,
		startTime:       time.Now(),
		diskSpace:       minDiskSpace,
		memoryThreshold: memoryThreshold,
		instanceName:    instanceName,
		costs:           map[string]*bbru.MonetaryResourceUsage_Expense{},
		metricTicker:    time.NewTicker(5 * time.Minute),
	}
	if primaryRedis != nil {
		w.client = newRedisClient(client, primaryRedis, readRedis, largeBlobSize)
	}
	if ackExtension > 0 {
		if !strings.HasPrefix(requestQueue, "gcppubsub://") {
			return nil, fmt.Errorf("Cannot specify a non-zero ack extension on subscription %s", requestQueue)
		}
		w.ackExtensionSub = strings.TrimPrefix(requestQueue, "gcppubsub://")
	}
	if cacheDir != "" {
		w.fileCache = newCache(cacheDir, cachePrefix, cacheParts)
	}
	if maxCacheSize > 0 {
		c, err := ristretto.NewCache(&ristretto.Config{
			NumCounters: maxCacheSize / 10, // bit of a guess
			MaxCost:     maxCacheSize,
			BufferItems: 64, // recommended by upstream
		})
		if err != nil {
			return nil, fmt.Errorf("Failed to create cache: %s", err)
		}
		w.cache = c
	}
	// Load the version file if given
	if versionFile != "" {
		if b, err := os.ReadFile(versionFile); err != nil {
			log.Errorf("Failed to read version file: %s", err)
		} else {
			w.version = strings.TrimSpace(string(b))
		}
	}
	if lucidity != "" {
		w.lucidChan = make(chan *lpb.Worker, 100)
		log.Notice("Dialling Lucidity...")
		conn, err := grpcutil.Dial(lucidity, true, "", tokenFile) // CA is currently not configurable.
		if err != nil {
			return nil, fmt.Errorf("Failed to dial Lucidity server: %s", err)
		}
		w.lucidity = lpb.NewLucidityClient(conn)
		go w.sendReports()
	}
	for name, cost := range costs {
		w.costs[name] = &bbru.MonetaryResourceUsage_Expense{Currency: cost.Denomination, Cost: cost.Amount}
	}

	// Vector metrics need initialising explicitly with labels. See https://github.com/prometheus/client_golang/blob/main/prometheus/examples_test.go#L51-L73
	currentBuilds.WithLabelValues(w.instanceName).Add(0)
	blobNotFoundErrors.WithLabelValues(w.version).Add(0)

	log.Notice("%s initialised with settings: CAS: %s cache dir: %s max cache size: %d sandbox: %s alt sandbox: %s", name, storage, cacheDir, maxCacheSize, w.sandbox, w.altSandbox)
	return w, nil
}

type worker struct {
	requests        *pubsub.Subscription
	responses       *pubsub.Topic
	currentMsg      *pubsub.Message
	ackExtension    time.Duration
	ackExtensionSub string
	client          elan.Client
	rclient         *client.Client
	lucidity        lpb.LucidityClient
	lucidChan       chan *lpb.Worker
	cache           *ristretto.Cache
	instanceName    string
	dir, rootDir    string
	home            string
	name            string
	version         string
	browserURL      string
	promGatewayURL  string
	sandbox         string
	altSandbox      string
	clean           bool
	disabled        bool
	fileCache       *cache
	startTime       time.Time
	diskSpace       int64
	memoryThreshold float64
	costs           map[string]*bbru.MonetaryResourceUsage_Expense
	metricTicker    *time.Ticker

	// These properties are per-action and reset each time.
	actionDigest    *pb.Digest
	metadata        *pb.ExecutedActionMetadata
	downloadedBytes int64
	cachedBytes     int64
	taskStartTime   time.Time
	metadataFetch   time.Duration
	dirCreation     time.Duration
	fileDownload    time.Duration
	lastURL         string // This is reset somewhat lazily.
	stdout, stderr  bytes.Buffer
	platformProps   map[string]string

	// For limiting parallelism during download / write actions
	limiter, iolimiter chan struct{}
}

// ShutdownQueues shuts down the internal topic & subscription when done.
func (w *worker) ShutdownQueues() {
	log.Notice("Shutting down queues")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.responses.Shutdown(ctx); err != nil {
		log.Warning("Failed to shut down topic: %s", err)
	}
	if err := w.requests.Shutdown(ctx); err != nil {
		log.Warning("Failed to shut down subscription: %s", err)
	}
}

// RunTask runs a single task.
// Note that it only returns errors for reasons this service controls (i.e. queue comms),
// failures at actually running the task are communicated back on the responses queue.
func (w *worker) RunTask(ctx context.Context) (*pb.ExecuteResponse, error) {
	msg, err := w.receiveTask(ctx)
	if err != nil {
		log.Error("Error receiving message: %s", err)
		return nil, err
	}
	w.currentMsg = msg
	w.downloadedBytes = 0
	w.cachedBytes = 0
	response := w.runTask(msg)
	if err = w.update(pb.ExecutionStage_COMPLETED, response); err != nil {
		msg.Nack()
		nackedMessages.Inc()
	} else {
		msg.Ack()
	}
	w.currentMsg = nil
	w.actionDigest = nil
	w.platformProps = nil
	return response, err
}

// receiveTask receives a task off the queue.
func (w *worker) receiveTask(ctx context.Context) (*pubsub.Message, error) {
	log.Debug("Waiting for next task...")
	for {
		msg, err := w.receiveOne(ctx)
		if err == context.DeadlineExceeded {
			log.Debug("Task receive timed out, retrying...")
			w.waitForFreeResources()
			w.waitForLiveConnection()
			w.waitIfDisabled()
			continue
		}
		return msg, err
	}
}

func (w *worker) receiveOne(ctx context.Context) (*pubsub.Message, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	return w.requests.Receive(ctx)
}

// runTask does the actual running of a task.
func (w *worker) runTask(msg *pubsub.Message) *pb.ExecuteResponse {
	if w.ackExtension > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.extendAckDeadline(ctx, msg)
	}
	req := &pb.ExecuteRequest{}
	if err := proto.Unmarshal(msg.Body, req); err != nil {
		log.Error("Badly serialised request: %s")
		return &pb.ExecuteResponse{
			Result: &pb.ActionResult{},
			Status: status(err, codes.FailedPrecondition, "Badly serialised request: %s", err),
		}
	}
	w.platformProps = copyPlatformProperties(msg.Metadata)
	return w.runTaskRequest(req)
}

// runTaskRequest runs a task from a proto request
func (w *worker) runTaskRequest(req *pb.ExecuteRequest) *pb.ExecuteResponse {
	totalBuilds.Inc()
	currentBuilds.WithLabelValues(w.instanceName).Inc()
	defer currentBuilds.WithLabelValues(w.instanceName).Dec()
	w.metadata = &pb.ExecutedActionMetadata{
		Worker:               w.name,
		WorkerStartTimestamp: ptypes.TimestampNow(),
	}
	w.taskStartTime = time.Now()
	w.actionDigest = req.ActionDigest
	logr.WithFields(logrus.Fields{
		"hash": w.actionDigest.Hash,
	}).Debug("Received task for action digest")
	w.logPlatformProperties()
	w.lastURL = w.actionURL()

	action, command, status := w.fetchRequestBlobs(req)
	if status != nil {
		log.Error("Bad request: %s", status)
		return &pb.ExecuteResponse{
			Result: &pb.ActionResult{},
			Status: status,
		}
	}
	if status := w.prepareDir(action, command); status != nil {
		logr.WithFields(logrus.Fields{
			"hash":   w.actionDigest.Hash,
			"status": status,
		}).Warn("Failed to prepare directory for action digest")
		ar := &pb.ActionResult{
			ExitCode:          255, // Not really but shouldn't look like it was successful
			ExecutionMetadata: w.metadata,
		}
		status.Message += w.writeUncachedResult(ar, status.Message)
		return &pb.ExecuteResponse{
			Result: ar,
			Status: status,
		}
	}
	return w.execute(req, action, command)
}

func copyPlatformProperties(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	ret := make(map[string]string, len(metadata))
	for k, v := range metadata {
		ret[k] = v
	}
	return ret
}

func formatPlatformProperties(props map[string]string) string {
	if len(props) == 0 {
		return ""
	}
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	formatted := make([]string, 0, len(keys))
	for _, key := range keys {
		formatted = append(formatted, key+"="+props[key])
	}
	return strings.Join(formatted, ", ")
}

func (w *worker) logPlatformProperties() {
	if len(w.platformProps) == 0 {
		return
	}
	logr.WithFields(logrus.Fields{
		"hash":               w.actionDigest.Hash,
		"platformProperties": formatPlatformProperties(w.platformProps),
	}).Info("Action specifies platform properties; would use them to select sandbox filesystem")
}

// forceShutdown sends any shutdown reports and calls log.Fatal() to shut down the worker
func (w *worker) forceShutdown(shutdownMsg string) {
	w.Report(false, false, false, "%s", shutdownMsg)
	log.Debug("Force shutting down worker")
	if w.currentMsg != nil {
		if w.actionDigest != nil {
			logr.WithFields(logrus.Fields{
				"hash": w.actionDigest.Hash,
			}).Debug("Nacking action")
		} else {
			log.Error("Nacking action but action digest is nil")
		}
		w.currentMsg.Nack()
		nackedMessages.Inc()
	}
	log.Fatal(shutdownMsg)
}

// extendAckDeadline continuously extends the ack deadline of a message until the
// supplied context expires.
func (w *worker) extendAckDeadline(ctx context.Context, msg *pubsub.Message) {
	var rm *pspb.ReceivedMessage
	if !msg.As(&rm) {
		log.Warning("Failed to convert message to a *ReceivedMessage, cannot extend ack deadline")
		return
	}
	ackID := rm.AckId
	var client *psraw.SubscriberClient
	if !w.requests.As(&client) {
		log.Warning("Failed to convert subscription to a *SubscriberClient, cannot extend ack deadline")
		return
	}
	w.extendAckDeadlineOnce(ctx, client, ackID)
	t := time.NewTicker(w.ackExtension / 2) // Extend twice as frequently as the deadline expiry
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.extendAckDeadlineOnce(ctx, client, ackID)
		}
	}
}

// extendAckDeadlineOnce extends the message's ack deadline one time.
func (w *worker) extendAckDeadlineOnce(ctx context.Context, client *psraw.SubscriberClient, ackID string) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := client.ModifyAckDeadline(ctx, &pspb.ModifyAckDeadlineRequest{
		Subscription:       w.ackExtensionSub,
		AckIds:             []string{ackID},
		AckDeadlineSeconds: int32(w.ackExtension.Seconds()),
	}); err != nil {
		logr.WithFields(logrus.Fields{
			"hash": w.actionDigest.Hash,
		}).WithError(err).Warn("Failed to extend ack deadline")
	}
}

// fetchRequestBlobs fetches and unmarshals the action and command for an execution request.
func (w *worker) fetchRequestBlobs(req *pb.ExecuteRequest) (*pb.Action, *pb.Command, *rpcstatus.Status) {
	action := &pb.Action{}
	command := &pb.Command{}
	if err := w.readBlobToProto(req.ActionDigest, action); err != nil {
		return nil, nil, status(err, codes.FailedPrecondition, "Invalid action digest %s/%d: %s", req.ActionDigest.Hash, req.ActionDigest.SizeBytes, err)
	} else if err := w.readBlobToProto(action.CommandDigest, command); err != nil {
		return nil, nil, status(err, codes.FailedPrecondition, "Invalid command digest %s/%d: %s", action.CommandDigest.Hash, action.CommandDigest.SizeBytes, err)
	} else if err := common.CheckOutputPaths(command); err != nil {
		return nil, nil, status(err, codes.InvalidArgument, "Invalid command outputs: %s", err)
	}
	return action, command, nil
}

// prepareDir prepares the directory for executing this request.
func (w *worker) prepareDir(action *pb.Action, command *pb.Command) *rpcstatus.Status {
	if status := w.prepareDirWithPacks(action, command, true); status != nil {
		logr.WithFields(logrus.Fields{
			"hash":   w.actionDigest.Hash,
			"status": status,
		}).Warn("Failed to prepare directory with packs, falling back to without")
		return w.prepareDirWithPacks(action, command, false)
	}
	return nil
}

// prepareDirWithPacks is like prepareDir but optionally uses pack files to optimise the download.
func (w *worker) prepareDirWithPacks(action *pb.Action, command *pb.Command, usePacks bool) *rpcstatus.Status {
	logr.WithFields(logrus.Fields{
		"hash": w.actionDigest.Hash,
	}).Info("Preparing directory")
	defer func() {
		w.metadata.InputFetchCompletedTimestamp = toTimestamp(time.Now())
	}()
	w.update(pb.ExecutionStage_EXECUTING, nil)
	if err := w.createTempDir(); err != nil {
		return status(err, codes.Internal, "Failed to create temp dir: %s", err)
	}
	start := time.Now()
	w.metadata.InputFetchStartTimestamp = toTimestamp(start)
	if err := w.downloadDirectory(action.InputRootDigest, usePacks); err != nil {
		if grpcstatus.Code(err) == codes.NotFound {
			blobNotFoundErrors.WithLabelValues(w.version).Inc()
		}
		return status(err, codes.Unknown, "Failed to download input root: %s", err)
	}
	// We are required to create directories for all the outputs.
	if !containsEnvVar(command, "_CREATE_OUTPUT_DIRS", "false") {
		for _, out := range command.OutputPaths {
			if dir := path.Dir(out); out != "" && out != "." {
				if err := os.MkdirAll(path.Join(w.dir, dir), os.ModeDir|0755); err != nil {
					return status(err, codes.Internal, "Failed to create directory: %s", err)
				}
			}
		}
	}
	fetchDurations.Observe(time.Since(start).Seconds())
	if total := w.cachedBytes + w.downloadedBytes; total > 0 {
		percentage := float64(w.downloadedBytes) * 100.0 / float64(total)
		logr.WithFields(logrus.Fields{
			"hash":       w.actionDigest.Hash,
			"bytes":      humanize.Bytes(uint64(w.downloadedBytes)),
			"total":      humanize.Bytes(uint64(total)),
			"percentage": fmt.Sprintf("%0.1f%%", percentage),
		}).Debug("Prepared directory")
	} else {
		logr.WithFields(logrus.Fields{
			"hash": w.actionDigest.Hash,
		}).Debug("Prepared directory")
	}
	log.Debug("Metadata fetch: %s, dir creation: %s, file download: %s", w.metadataFetch, w.dirCreation, w.fileDownload)
	return nil
}

// createTempDir creates the temporary workdir.
func (w *worker) createTempDir() error {
	dir := path.Join(w.rootDir, "mettle_"+w.actionDigest.Hash[:10])
	err := os.Mkdir(dir, os.ModeDir|0755)
	if err == nil {
		w.dir = dir
		return nil
	} else if os.IsExist(err) {
		// Attempt to remove and try again
		if err := os.RemoveAll(dir); err == nil {
			if err := os.Mkdir(dir, os.ModeDir|0755); err == nil {
				w.dir = dir
				return nil
			}
		}
	}
	log.Warning("Failed to create work dir: %s. Falling back to temp dir", err)
	dir, err = os.MkdirTemp(w.rootDir, "mettle_")
	w.dir = dir
	return err
}

// execute runs the actual commands once the inputs are prepared.
func (w *worker) execute(req *pb.ExecuteRequest, action *pb.Action, command *pb.Command) *pb.ExecuteResponse {
	if w.clean {
		defer func() {
			if err := os.RemoveAll(w.dir); err != nil {
				log.Error("Failed to clean workdir: %s", err)
			}
		}()
	}
	command.Arguments = w.addSandbox(command)
	start := time.Now()
	w.metadata.ExecutionStartTimestamp = toTimestamp(start)
	duration, _ := ptypes.Duration(action.Timeout)
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	logr.WithFields(logrus.Fields{
		"hash":    w.actionDigest.Hash,
		"stage":   "exec_action",
		"timeout": duration.String(),
		"argv0":   command.Arguments[0],
		"argc":    len(command.Arguments),
	}).Debug("Executing action - started")

	logr.WithFields(logrus.Fields{
		"hash":          w.actionDigest.Hash,
		"sandbox":       w.sandbox,
		"altSandbox":    w.altSandbox,
		"shouldSandbox": w.shouldSandbox(command),
		"argv0":         command.Arguments[0],
	}).Debug("Sandbox selection")

	cmd := exec.CommandContext(ctx, commandPath(command), command.Arguments[1:]...)
	// Setting Pdeathsig should ideally make subprocesses get kill signals if we die.

	if cmd.SysProcAttr = sysProcAttr(); cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true

	cmd.Dir = path.Join(w.dir, command.WorkingDirectory)
	cmd.Env = make([]string, len(command.EnvironmentVariables))
	for i, v := range command.EnvironmentVariables {
		// This is a crappy little hack; tool paths that are made relative don't always work
		// (notably for "go build" which needs an absolute path for -toolexec). For now, we
		// fix up here, but ideally we shouldn't need to know the detail of this.
		if strings.HasPrefix(v.Name, "TOOL") && !path.IsAbs(v.Value) && strings.ContainsRune(v.Value, '/') {
			v.Value = w.sandboxDir(v.Value)
		} else if v.Name == "PATH" {
			v.Value = strings.ReplaceAll(v.Value, "~", w.home)
		} else if v.Name == "TEST" {
			v.Value = w.sandboxDir(v.Value)
		}
		cmd.Env[i] = v.Name + "=" + v.Value
	}

	err := w.runCommand(ctx, cmd, duration)

	execEnd := time.Now()
	w.metadata.VirtualExecutionDuration = durationpb.New(duration)
	w.metadata.ExecutionCompletedTimestamp = toTimestamp(execEnd)
	w.metadata.OutputUploadStartTimestamp = w.metadata.ExecutionCompletedTimestamp
	execDuration := execEnd.Sub(start).Seconds()
	executeDurations.Observe(execDuration)

	logr.WithFields(logrus.Fields{
		"hash":  w.actionDigest.Hash,
		"stage": "exec_action",
		"took":  execDuration,
		"err":   fmt.Sprintf("%v", err),
	}).Debug("Executing action - completed")

	ar := &pb.ActionResult{
		ExitCode:          int32(cmd.ProcessState.ExitCode()),
		ExecutionMetadata: w.metadata,
	}

	stdoutDigest, uploadErr := w.client.WriteBlob(w.stdout.Bytes())
	if uploadErr != nil {
		logr.WithFields(logrus.Fields{
			"hash": w.actionDigest.Hash,
		}).WithError(uploadErr).Error("Failed to upload stdout")
		return &pb.ExecuteResponse{
			Status: status(uploadErr, codes.Internal, "Failed to upload stdout: %s", uploadErr),
			Result: ar,
		}
	}
	ar.StdoutDigest = stdoutDigest

	stderrDigest, uploadErr := w.client.WriteBlob(w.stderr.Bytes())
	if uploadErr != nil {
		logr.WithFields(logrus.Fields{
			"hash": w.actionDigest.Hash,
		}).WithError(uploadErr).Error("Failed to upload stderr")
		return &pb.ExecuteResponse{
			Status: status(uploadErr, codes.Internal, "Failed to upload stderr: %s", uploadErr),
			Result: ar,
		}
	}
	ar.StderrDigest = stderrDigest

	logr.WithFields(logrus.Fields{
		"hash": w.actionDigest.Hash,
	}).Info("Uploading outputs")
	w.observeSysUsage(cmd, execDuration)
	if err != nil {
		log.Debug("Execution failed.\nStdout: %s\nStderr: %s", w.stdout.String(), w.stderr.String())
		msg := "Execution failed for " + w.actionDigest.Hash + ": " + err.Error()
		msg += w.writeUncachedResult(ar, msg)
		// Attempt to collect outputs. They may exist and contain useful information such as a test.results file
		_ = w.collectOutputs(ar, command)
		// Still counts as OK on a status code.
		status := &rpcstatus.Status{Code: int32(codes.OK)}
		if err == ErrTimeout {
			// For timeouts, the execution did not complete, so we should return this status as the docs suggest.
			status.Code = int32(codes.DeadlineExceeded)
			status.Message = ErrTimeout.Error()
		}
		return &pb.ExecuteResponse{
			Status:  status,
			Result:  ar,
			Message: msg,
		}
	}
	if err := w.collectOutputs(ar, command); err != nil {
		collectOutputErrors.Inc()
		logr.WithFields(logrus.Fields{
			"hash": w.actionDigest.Hash,
		}).WithError(err).Error("Failed to collect outputs")
		return &pb.ExecuteResponse{
			Status: status(err, codes.Internal, "Failed to collect outputs: %s", err),
			Result: ar,
		}
	}
	end := time.Now()
	w.metadata.OutputUploadCompletedTimestamp = toTimestamp(end)
	uploadDurations.Observe(end.Sub(execEnd).Seconds())
	logr.WithFields(logrus.Fields{
		"hash": w.actionDigest.Hash,
	}).Debug("Uploaded outputs")
	w.metadata.WorkerCompletedTimestamp = toTimestamp(time.Now())
	w.observeCosts()

	// If the result was missing some output paths, we should still return it however avoid caching the result
	if !containsAllOutputPaths(command, ar) {
		logr.WithFields(logrus.Fields{
			"hash": w.actionDigest.Hash,
		}).Warn("Result missing some outputs")
		msg := "result was missing some outputs"
		w.writeUncachedResult(ar, msg)
		return &pb.ExecuteResponse{
			Status:  &rpcstatus.Status{Code: int32(codes.OK)}, // Still counts as OK on a status code.
			Result:  ar,
			Message: msg,
		}
	}

	ar, err = w.client.UpdateActionResult(&pb.UpdateActionResultRequest{
		InstanceName: w.instanceName,
		ActionDigest: w.actionDigest,
		ActionResult: ar,
		ResultsCachePolicy: &pb.ResultsCachePolicy{
			Priority: resultsCachePriority(req.SkipCacheLookup),
		},
	})
	if err != nil {
		logr.WithFields(logrus.Fields{
			"hash": w.actionDigest.Hash,
		}).WithError(err).Error("Failed to upload action result")
		return &pb.ExecuteResponse{
			Status: status(err, codes.Internal, "Failed to upload action result: %s", err),
			Result: ar,
		}
	}
	logr.WithFields(logrus.Fields{
		"hash": w.actionDigest.Hash,
	}).Debug("Uploaded action result")
	return &pb.ExecuteResponse{
		Status: &rpcstatus.Status{Code: int32(codes.OK)},
		Result: ar,
	}
}

func containsAllOutputPaths(cmd *pb.Command, ar *pb.ActionResult) bool {
	paths := make(map[string]bool, len(ar.OutputDirectories)+len(ar.OutputFiles)+len(ar.OutputSymlinks)+len(ar.OutputDirectorySymlinks)+len(ar.OutputFileSymlinks))

	// Build up the list of outputs that were generated
	for _, o := range ar.OutputFiles {
		paths[o.Path] = true
	}

	for _, o := range ar.OutputDirectories {
		paths[o.Path] = true
	}

	for _, o := range ar.OutputSymlinks {
		paths[o.Path] = true
	}

	for _, o := range ar.OutputDirectorySymlinks {
		paths[o.Path] = true
	}

	for _, o := range ar.OutputFileSymlinks {
		paths[o.Path] = true
	}

	// Check that we generated all the required outputs
	for _, p := range cmd.OutputPaths {
		if !paths[p] {
			log.Warningf("Action result doesn't contain output path %s. Action result object: %#v\n", p, ar)
			return false
		}
	}

	return true
}

// runCommand runs a command with a timeout, terminating it in a sensible manner
func (w *worker) runCommand(ctx context.Context, cmd *exec.Cmd, timeout time.Duration) error {
	w.stdout.Reset()
	w.stderr.Reset()

	cmd.Stdout = &w.stdout
	cmd.Stderr = &w.stderr
	gracePeriod := 5 * time.Second
	cmd.WaitDelay = gracePeriod

	cmd.Cancel = func() error {
		if cmd.Process == nil || cmd.Process.Pid <= 0 {
			return nil
		}

		// send SIGTERM to the entire group (-PID) created by Setpgid
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)

		// If the group doesn't exist yet or already gone, ignore the error
		if errors.Is(err, syscall.ESRCH) {
			return nil
		}
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	err := cmd.Wait()

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		actionTimeout.Inc()

		forceKilled := false
		if ps := cmd.ProcessState; ps != nil {
			if status, ok := ps.Sys().(syscall.WaitStatus); ok {
				if status.Signaled() && (status.Signal() == syscall.SIGTERM || status.Signal() == syscall.SIGKILL) {
					forceKilled = true
				}
			}
		}

		msg := "Terminating process due to timeout"
		if forceKilled {
			msg += "; grace period expired; process killed (SIGKILL)"
		}
		logr.WithFields(logrus.Fields{
			"hash":        w.actionDigest.Hash,
			"timeout":     timeout.String(),
			"gracePeriod": gracePeriod.String(),
			"forceKilled": forceKilled,
		}).Warn(msg)

		// log last line
		lastLine := func(s string) string {
			s = strings.TrimRight(s, "\n")
			if s == "" {
				return ""
			}
			if i := strings.LastIndexByte(s, '\n'); i >= 0 {
				return s[i+1:]
			}
			return s
		}
		stdoutStr := w.stdout.String()
		stderrStr := w.stderr.String()
		logr.WithFields(logrus.Fields{
			"hash":         w.actionDigest.Hash,
			"stdout_bytes": len(stdoutStr),
			"stderr_bytes": len(stderrStr),
			"stdout_last":  lastLine(stdoutStr),
			"stderr_last":  lastLine(stderrStr),
		}).Debug("Output summary at timeout")

		return ErrTimeout
	}
	return err
}

// writeUncachedResult attempts to write an uncached action result proto.
// This is an extension for buildbarn-browser that lets it display results of a failed build.
// It returns a string that, on success, contains an appropriate message about it (they are
// communicated back in the human-readable part of the response).
func (w *worker) writeUncachedResult(ar *pb.ActionResult, msg string) string {
	// No point if we don't know where the browser is.
	if w.browserURL == "" {
		return ""
	}
	b, _ := proto.Marshal(&bbcas.UncachedActionResult{
		ActionDigest: w.actionDigest,
		ExecuteResponse: &pb.ExecuteResponse{
			Status: status(nil, codes.Unknown, "%s", msg),
			Result: ar,
		},
	})
	digest, err := w.client.WriteBlob(b)
	if err != nil {
		log.Warning("Failed to save uncached action result: %s", err)
		return ""
	}
	w.lastURL = fmt.Sprintf("%s/uncached_action_result/%s/%s/%d/", w.browserURL, w.instanceName, digest.Hash, digest.SizeBytes)
	return "\nFailed action details: " + w.lastURL + "\n      Original action: " + w.actionURL()
}

// actionURL returns a browser URL for the currently executed action, or the empty string if no browser is configured.
func (w *worker) actionURL() string {
	if w.browserURL == "" {
		return ""
	}
	return fmt.Sprintf("%s/action/%s/%s/%d/", w.browserURL, w.instanceName, w.actionDigest.Hash, w.actionDigest.SizeBytes)
}

// shouldSandbox returns true if we should sandbox execution of the given command.
// This is determined by it having a SANDBOX environment variable set to "true".
func (w *worker) shouldSandbox(command *pb.Command) bool {
	for _, e := range command.EnvironmentVariables {
		if e.Name == "SANDBOX" && e.Value == "true" {
			return true
		}
	}
	return false
}

// addSandbox adds the sandbox to the given command arguments.
func (w *worker) addSandbox(command *pb.Command) []string {
	if w.sandbox != "" && w.shouldSandbox(command) {
		return append([]string{w.sandbox}, command.Arguments...)
	} else if w.altSandbox != "" {
		return append([]string{w.altSandbox}, command.Arguments...)
	}
	return command.Arguments
}

// sandboxDir puts a path within either the sandbox directory or the worker's directory.
func (w *worker) sandboxDir(v string) string {
	if w.sandbox != "" && w.altSandbox != "" {
		return path.Join("/tmp/plz_sandbox", v)
	}
	return path.Join(w.dir, v)
}

// observeSysUsage observes some stats from a running process.
// It's split to a separate function to handle panics; the docs are a little unclear under what
// circumstances this might happen, but we've definitely seen it.
func (w *worker) observeSysUsage(cmd *exec.Cmd, execDuration float64) {
	defer func() {
		if r := recover(); r != nil {
			log.Warning("Failed to observe process sys usage: %s", r)
		}
	}()
	if cmd.ProcessState == nil {
		return
	}
	rusage := cmd.ProcessState.SysUsage().(*syscall.Rusage)
	peakMemory.Observe(float64(rusage.Maxrss) / 1024.0) // maxrss is in kb, we use mb for convenience
	ssec, snsec := rusage.Stime.Unix()
	usec, unsec := rusage.Utime.Unix()
	cpuUsage.Observe((float64(ssec+usec) + float64(snsec+unsec)/1000000000) / execDuration)
	// Add this to the metadata.
	any, err := ptypes.MarshalAny(&bbru.POSIXResourceUsage{
		UserTime:                   toDuration(rusage.Utime),
		SystemTime:                 toDuration(rusage.Stime),
		MaximumResidentSetSize:     rusage.Maxrss * maximumResidentSetSizeUnit,
		PageReclaims:               rusage.Minflt,
		PageFaults:                 rusage.Majflt,
		Swaps:                      rusage.Nswap,
		BlockInputOperations:       rusage.Inblock,
		BlockOutputOperations:      rusage.Oublock,
		MessagesSent:               rusage.Msgsnd,
		MessagesReceived:           rusage.Msgrcv,
		SignalsReceived:            rusage.Nsignals,
		VoluntaryContextSwitches:   rusage.Nvcsw,
		InvoluntaryContextSwitches: rusage.Nivcsw,
	})
	if err != nil {
		log.Warning("Failed to serialise resource usage: %s", err)
		return
	}
	w.metadata.AuxiliaryMetadata = append(w.metadata.AuxiliaryMetadata, any)
}

// observeCosts attaches any costs to the completed action.
func (w *worker) observeCosts() {
	if len(w.costs) == 0 {
		return
	}
	duration := time.Since(w.taskStartTime).Seconds()
	msg := &bbru.MonetaryResourceUsage{
		Expenses: make(map[string]*bbru.MonetaryResourceUsage_Expense, len(w.costs)),
	}
	for name, cost := range w.costs {
		msg.Expenses[name] = &bbru.MonetaryResourceUsage_Expense{
			Currency: cost.Currency,
			Cost:     cost.Cost * duration,
		}
	}
	any, err := ptypes.MarshalAny(msg)
	if err != nil {
		log.Warning("Failed to serialise costs: %s", err)
		return
	}
	w.metadata.AuxiliaryMetadata = append(w.metadata.AuxiliaryMetadata, any)
}

func toDuration(tv syscall.Timeval) *bbru.Duration {
	return &bbru.Duration{
		Seconds: tv.Sec,
		Nanos:   int32(tv.Usec) * 1000,
	}
}

func (w *worker) markOutputsAsBinary(cmd *pb.Command) error {
	for _, o := range cmd.OutputPaths {
		filePath := filepath.Join(w.dir, o)
		info, err := os.Lstat(filePath)
		if err != nil {
			return err
		}

		if info.IsDir() {
			err := filepath.Walk(filePath, func(path string, info os.FileInfo, err error) error {
				if !info.Mode().IsRegular() || info.Mode() == 0555 {
					return nil
				}
				return os.Chmod(path, 0555)
			})
			if err != nil {
				return err
			}
		} else if err := os.Chmod(filePath, 0555); err != nil {
			return err
		}
	}
	return nil
}

// collectOutputs collects all the outputs of a command and adds them to the given ActionResult.
func (w *worker) collectOutputs(ar *pb.ActionResult, cmd *pb.Command) error {
	if containsEnvVar(cmd, "_BINARY", "true") {
		if err := w.markOutputsAsBinary(cmd); err != nil {
			return err
		}
	}

	m, ar2, err := w.rclient.ComputeOutputsToUpload(w.dir, ".", cmd.OutputPaths, filemetadata.NewNoopCache(), command.PreserveSymlink)
	if err != nil {
		return err
	}
	entries := make([]*uploadinfo.Entry, 0, len(m))
	compressors := make([]pb.Compressor_Value, 0, len(m))
	for _, e := range m {
		entries = append(entries, e)
		compressors = append(compressors, w.entryCompressor(e))
	}
	err = w.client.UploadIfMissing(entries, compressors)

	setRootDirectoryDigests(ar2.OutputDirectories, m)
	ar.OutputFiles = ar2.OutputFiles
	ar.OutputDirectories = ar2.OutputDirectories
	ar.OutputFileSymlinks = ar2.OutputFileSymlinks
	ar.OutputDirectorySymlinks = ar2.OutputDirectorySymlinks
	ar.OutputSymlinks = allOutputSymlinks(ar2)
	return err
}

// setRootDirectoryDigests populates root_directory_digest on each output directory (REAPI 2.8), which
// lets clients use an output directory without fetching its whole Tree.
// ComputeOutputsToUpload already uploads every Directory individually as well as the Tree, so we
// only need to find the root's digest. We use the root as it appears in the Tree, which includes the
// pack property if there is one.
// This is best-effort: if we can't work it out we leave it unset, and clients can still use the Tree.
func setRootDirectoryDigests(dirs []*pb.OutputDirectory, entries map[digest.Digest]*uploadinfo.Entry) {
	for _, dir := range dirs {
		entry, present := entries[digest.NewFromProtoUnvalidated(dir.TreeDigest)]
		if !present || !entry.IsBlob() {
			log.Warning("Can't find Tree for output directory %s, not setting its root directory digest", dir.Path)
			continue
		}
		tree := &pb.Tree{}
		if err := proto.Unmarshal(entry.Contents, tree); err != nil {
			log.Warning("Failed to decode Tree for output directory %s: %s", dir.Path, err)
			continue
		}
		rootDigest, err := digest.NewFromMessage(tree.Root)
		if err != nil {
			log.Warning("Failed to calculate root directory digest for output directory %s: %s", dir.Path, err)
			continue
		} else if _, present := entries[rootDigest]; !present {
			log.Warning("Root directory for output directory %s is not being uploaded, not setting its digest", dir.Path)
			continue
		}
		dir.RootDirectoryDigest = rootDigest.ToProto()
	}
}

// allOutputSymlinks returns all the output symlinks from an action result, preferring output_symlinks
// if it's populated and falling back to the deprecated per-type fields otherwise.
func allOutputSymlinks(ar *pb.ActionResult) []*pb.OutputSymlink {
	if len(ar.OutputSymlinks) > 0 {
		return ar.OutputSymlinks
	}
	return slices.Concat(ar.OutputFileSymlinks, ar.OutputDirectorySymlinks)
}

// update sends an update on the response channel
func (w *worker) update(stage pb.ExecutionStage_Value, response *pb.ExecuteResponse) error {
	w.Report(true, stage == pb.ExecutionStage_EXECUTING, true, "%s", stage)
	body := common.MarshalOperation(stage, w.actionDigest, response, w.metadata)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	return common.PublishWithOrderingKey(ctx, w.responses, body, w.actionDigest.Hash, w.name)
}

// periodicallyPushMetrics will push this worker's metrics to the gateway every 5 minutes.
func (w *worker) periodicallyPushMetrics() {
	if w.promGatewayURL != "" {
		// Set up the metrics
		pusher := push.New(w.promGatewayURL, w.name)
		for _, metric := range metrics {
			pusher = pusher.Collector(metric).Format(expfmt.FmtText)
		}
		// Push to the gateway every 5 minutes
		for range w.metricTicker.C {
			if err := pusher.Push(); err != nil {
				log.Warningf("Error pushing to Prometheus pushgateway: %s", err)
			}
		}
	}
}

// readBlobToProto reads an entire blob and deserialises it into a message.
func (w *worker) readBlobToProto(digest *pb.Digest, msg proto.Message) error {
	b, err := w.client.ReadBlob(digest)
	if err != nil {
		return err
	}
	return proto.Unmarshal(b, msg)
}

func status(err error, code codes.Code, msg string, args ...interface{}) *rpcstatus.Status {
	if c := grpcstatus.Code(err); c != codes.Unknown && c != codes.OK {
		code = c
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		code = codes.DeadlineExceeded
	}
	return &rpcstatus.Status{
		Code:    int32(code),
		Message: fmt.Sprintf(msg, args...),
	}
}

// toTimestamp converts the given time to a proto timestamp, ignoring errors
func toTimestamp(t time.Time) *timestamp.Timestamp {
	ts, _ := ptypes.TimestampProto(t)
	return ts
}

// containsEnvVar returns true if the given env var is set on this command.
func containsEnvVar(command *pb.Command, name, value string) bool {
	return getEnvVar(command, name) == value
}

// getEnvVar returns the value of an env var on the given command.
func getEnvVar(command *pb.Command, name string) string {
	for _, e := range command.EnvironmentVariables {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}

// mustAbs converts the given path to an absolute one, or dies in the attempt.
// If it's the empty string it is left as that.
func mustAbs(file string) string {
	if file == "" {
		return file
	}
	p, err := filepath.Abs(file)
	if err != nil {
		log.Fatalf("Failed to make %s absolute: %s", file, err)
	}
	return p
}

// resultsCachePriority returns the priority of an action cache update based on the SkipCacheLookup flag.
// If that flag is true it's deemed 'urgent' which will allow overwriting whatever is there.
func resultsCachePriority(skipCacheLookup bool) int32 {
	if skipCacheLookup {
		return -1
	}
	return 0
}

// commandPath returns the path that should be used for this command.
// Per the spec, if it contains a path separator we don't alter it; if not we look it up,
// honouring the given env var if present.
func commandPath(command *pb.Command) string {
	arg := command.Arguments[0]
	if strings.ContainsRune(arg, filepath.Separator) {
		return arg
	}
	for _, env := range command.EnvironmentVariables {
		if env.Name == "PATH" {
			for _, p := range filepath.SplitList(env.Value) {
				result := filepath.Join(p, arg)
				if _, err := os.Stat(result); err == nil {
					return result
				}
			}
		}
	}
	path, err := exec.LookPath(arg)
	if err != nil {
		log.Error("Failed to look up path: %s", err)
		return arg // Let exec deal with this later
	}
	return path
}
