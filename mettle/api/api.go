// Package api implements the remote execution API server.
package api

import (
	"context"
	"fmt"
	"maps"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	clilogging "github.com/peterebden/go-cli-init/v4/logging"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	bpb "github.com/thought-machine/please-servers/proto/mettle"
	"gocloud.dev/pubsub"
	"google.golang.org/genproto/googleapis/longrunning"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/mettle/common"
	"github.com/thought-machine/please-servers/rexclient"
)

var log = clilogging.MustGetLogger()
var logr = logrus.New()

const timeout = 10 * time.Second

// retentionTime is the length of time we retain completed actions for; after this we
// can no longer answer WaitExecution requests for them.
const retentionTime = 5 * time.Minute

// expiryTime is the period after which we expire an action that hasn't progressed.
const expiryTime = 1 * time.Hour

// resumptionTime is the maximum age of jobs that we permit to be resumed.
var resumptionTime = 10 * time.Minute

var totalRequests = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "requests_total",
})

var totalFailedActions = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "failed_actions_total",
})

var totalSuccessfulActions = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "successful_actions_total",
})

var totalFailedPubSubMessages = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "failed_pub_sub_total",
	Help:      "Number of times the Pub/Sub pool has failed",
})

var noExecutionInProgress = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "no_execution_in_progress",
})

var requestPublishFailure = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "request_publish_failures",
})

var responsePublishFailure = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "response_publish_failures",
})

var timeToComplete = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "time_to_complete_action_secs",
	Buckets:   []float64{1, 5, 10, 30, 60, 120, 300, 600, 900, 1200},
})

var preResponsePublishDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "publish_durations",
	Buckets:   prometheus.DefBuckets,
})

var deleteJobsDurations = prometheus.NewHistogram(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "delete_jobs_durations",
	Buckets:   prometheus.DefBuckets,
})

var metrics = []prometheus.Collector{
	totalRequests,
	totalFailedActions,
	totalSuccessfulActions,
	timeToComplete,
	totalFailedPubSubMessages,
	noExecutionInProgress,
	requestPublishFailure,
	responsePublishFailure,
	preResponsePublishDurations,
	deleteJobsDurations,
}

var register sync.Once

func init() {
	for _, metric := range metrics {
		prometheus.MustRegister(metric)
	}
}

// PubSubOpts holds information to configure queue options in the api server
type PubSubOpts struct {
	RequestQueue          string `short:"q" long:"request_queue" env:"API_REQUEST_QUEUE" required:"true" description:"URL defining the pub/sub queue to connect to for sending requests, e.g. gcppubsub://my-request-queue"`
	ResponseQueue         string `short:"r" long:"response_queue" env:"API_RESPONSE_QUEUE" required:"true" description:"URL defining the pub/sub queue to connect to for sending responses, e.g. gcppubsub://my-response-queue"`
	ResponseQueueSuffix   string `long:"response_queue_suffix" env:"API_RESPONSE_QUEUE_SUFFIX" description:"Suffix to apply to the response queue name"`
	PreResponseQueue      string `long:"pre_response_queue" env:"API_PRE_RESPONSE_QUEUE" required:"true" description:"URL describing the pub/sub queue to connect to for preloading responses to other servers"`
	NumPollers            int    `long:"num_pollers" env:"API_NUM_POLLERS" default:"10"`
	SubscriptionBatchSize uint   `long:"subscription_batch_size" env:"API_SUBSCRIPTION" default:"100"`
}

// ServeForever serves on the given port until terminated.
func ServeForever(opts grpcutil.Opts, name string, queueOpts PubSubOpts, apiURL string, connTLS bool, allowedPlatform map[string][]string, storageURL string, storageTLS bool) {
	s, lis, err := serve(opts, name, queueOpts, apiURL, connTLS, allowedPlatform, storageURL, storageTLS)
	if err != nil {
		log.Fatalf("Failed to start API server: %s", err)
	}
	grpcutil.ServeForever(lis, s)
}

func serve(opts grpcutil.Opts, name string, queueOpts PubSubOpts, apiURL string, connTLS bool, allowedPlatform map[string][]string, storageURL string, storageTLS bool) (*grpc.Server, net.Listener, error) {
	if name == "" {
		name = "mettle API server"
	}

	log.Notice("Contacting CAS server on %s...", storageURL)
	client, err := rexclient.New(name, storageURL, storageTLS, "")
	if err != nil {
		return nil, nil, err
	}
	if queueOpts.NumPollers < 1 {
		return nil, nil, fmt.Errorf("num_pollers must be greater than 1, got: %d", queueOpts.NumPollers)
	}
	// The subscription url is made up of the response queue url and the response queue suffix
	subscriptionURL := queueOpts.ResponseQueue + queueOpts.ResponseQueueSuffix
	srv := &server{
		name:             name,
		requests:         common.MustOpenTopic(queueOpts.RequestQueue),
		responses:        common.MustOpenSubscription(subscriptionURL, queueOpts.SubscriptionBatchSize),
		preResponses:     common.MustOpenTopic(queueOpts.PreResponseQueue),
		jobs:             map[string]*job{},
		platform:         allowedPlatform,
		client:           client,
		numPollers:       queueOpts.NumPollers,
		deleteJobsTicker: time.NewTicker(30 * time.Second),
		actuallyValidate: len(allowedPlatform) > 0,
	}

	// _Technically_ this won't happen more than once in normal running, as we'd only run 1 server, but it does happen in tests.
	register.Do(func() {
		prometheus.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "mettle",
			Name:      "requests_current",
		},
			func() float64 {
				srv.mutex.Lock()
				defer srv.mutex.Unlock()
				return float64(len(srv.jobs))
			},
		))
	})

	log.Notice("Allowed platform values:")
	for k, v := range allowedPlatform {
		log.Notice("    %s: %s", k, strings.Join(v, ", "))
	}

	if jobs, err := getExecutions(opts, apiURL, connTLS); err != nil {
		log.Warningf("Failed to get inflight executions: %s", err)
	} else if len(jobs) > 0 {
		srv.jobs = jobs
		log.Notice("Updated server with %d inflight executions", len(srv.jobs))
	}
	go srv.Receive()
	go srv.periodicallyDeleteJobs()

	lis, s := grpcutil.NewServer(opts)
	pb.RegisterCapabilitiesServer(s, srv)
	pb.RegisterExecutionServer(s, srv)
	bpb.RegisterBootstrapServer(s, srv)
	return s, lis, nil
}

type server struct {
	bpb.UnimplementedBootstrapServer
	name             string
	client           *client.Client
	requests         *pubsub.Topic
	responses        *pubsub.Subscription
	preResponses     *pubsub.Topic
	jobs             map[string]*job
	platform         map[string][]string
	mutex            sync.Mutex
	numPollers       int
	deleteJobsTicker *time.Ticker
	actuallyValidate bool
}

// ServeExecutions serves a list of currently executing jobs over GRPC.
func (s *server) ServeExecutions(ctx context.Context, req *bpb.ServeExecutionsRequest) (*bpb.ServeExecutionsResponse, error) {
	log.Debug("Received request for inflight executions")
	s.mutex.Lock()
	defer s.mutex.Unlock()
	executions := []*bpb.Job{}
	for k, v := range s.jobs {
		current, _ := proto.Marshal(v.Current)
		job := &bpb.Job{
			ID:         k,
			Current:    current,
			LastUpdate: v.LastUpdate.Unix(),
			StartTime:  v.StartTime.Unix(),
			SentFirst:  v.SentFirst,
			Done:       v.Done,
		}
		executions = append(executions, job)
	}
	res := &bpb.ServeExecutionsResponse{
		Jobs: executions,
	}
	log.Debug("Serving %d inflight executions", len(executions))
	return res, nil
}

// getExecutions requests a list of currently executing jobs over grpc
func getExecutions(opts grpcutil.Opts, apiURL string, connTLS bool) (map[string]*job, error) {
	if apiURL == "" {
		log.Notice("No API URL provided, will not request inflight executions")
		return map[string]*job{}, nil
	}
	conn, err := grpcutil.Dial(apiURL, connTLS, opts.CertFile, opts.TokenFile)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := bpb.NewBootstrapClient(conn)
	log.Debug("Requesting inflight executions...")
	req := &bpb.ServeExecutionsRequest{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := client.ServeExecutions(ctx, req)
	if err != nil {
		return nil, err
	}
	jobs := make(map[string]*job, len(res.Jobs))
	for _, j := range res.Jobs {
		current := &longrunning.Operation{}
		if err := proto.Unmarshal(j.Current, current); err != nil {
			log.Warningf("unable to unmarshal s%", j.ID)
			continue
		}
		jobs[j.ID] = &job{
			Current:    current,
			LastUpdate: time.Unix(j.LastUpdate, 0),
			StartTime:  time.Unix(j.StartTime, 0),
			SentFirst:  j.SentFirst,
			Done:       j.Done,
		}
	}
	return jobs, nil
}

func (s *server) GetCapabilities(ctx context.Context, req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	return &pb.ServerCapabilities{
		ExecutionCapabilities: &pb.ExecutionCapabilities{
			DigestFunction: pb.DigestFunction_SHA256,
			ExecEnabled:    true,
		},
		LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
		HighApiVersion: &semver.SemVer{Major: 2, Minor: 12},
	}, nil
}

func (s *server) Execute(req *pb.ExecuteRequest, stream pb.Execution_ExecuteServer) error {
	if req.ActionDigest == nil {
		return status.Errorf(codes.InvalidArgument, "Action digest not specified")
	}
	platform, err := s.validatePlatform(req)
	if err != nil {
		return err
	}
	if md := s.contextMetadata(stream.Context()); md != nil {
		logr.WithFields(logrus.Fields{
			"hash":          req.ActionDigest.Hash,
			"toolName":      md.ToolDetails.ToolName,
			"toolVersion":   md.ToolDetails.ToolVersion,
			"actionID":      md.ActionId,
			"correlationID": md.CorrelatedInvocationsId,
		}).Debug("Received an ExecuteRequest")
	} else {
		logr.WithFields(logrus.Fields{
			"hash": req.ActionDigest.Hash,
		}).Debug("Received an ExecuteRequest")
	}

	// If we're allowed to check the cache, see if this one has already been done.
	// A well-behaved client will likely have done this itself but we should make sure again.
	if !req.SkipCacheLookup {
		if err := stream.Send(common.BuildOperation(pb.ExecutionStage_CACHE_CHECK, req.ActionDigest, nil, nil)); err != nil {
			log.Warningf("Failed to forward to stream: %s", err)
		}
		if ar, err := s.client.CheckActionCache(context.Background(), req.ActionDigest); err != nil {
			log.Warning("Failed to check action cache: %s", err)
		} else if ar != nil {
			return stream.Send(common.BuildOperation(pb.ExecutionStage_COMPLETED, req.ActionDigest, &pb.ExecuteResponse{
				Result:       ar,
				CachedResult: true,
				Status:       &rpcstatus.Status{Code: int32(codes.OK)},
			}, nil))
		}
	}

	ch, created := s.eventStream(req.ActionDigest, true)
	if !created {
		// We didn't create a new execution, so don't need to send a request for a new build.
		return s.streamEvents(req.ActionDigest, ch, stream)
	}
	// Dispatch a pre-emptive response message to let our colleagues know we've queued it.
	// We will also receive & forward this message.
	b := common.MarshalOperation(pb.ExecutionStage_QUEUED, req.ActionDigest, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	preResponseStartTime := time.Now()
	if err := common.PublishWithOrderingKey(ctx, s.preResponses, b, req.ActionDigest.Hash, s.name); err != nil {
		responsePublishFailure.Inc()
		log.Error("Failed to communicate pre-response message: %s", err)
	}
	preResponsePublishDurations.Observe(time.Since(preResponseStartTime).Seconds())
	b, _ = proto.Marshal(req)
	ctx, cancel = context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := s.requests.Send(ctx, &pubsub.Message{
		Body:     b,
		Metadata: platform,
	}); err != nil {
		requestPublishFailure.Inc()
		log.Error("Failed to submit work to stream: %s", err)
		return err
	}
	return s.streamEvents(req.ActionDigest, ch, stream)
}

// contextMetadata returns the RequestMetadata associated with a context
func (s *server) contextMetadata(ctx context.Context) *pb.RequestMetadata {
	const key = "build.bazel.remote.execution.v2.requestmetadata-bin" // as defined by the proto
	msg := &pb.RequestMetadata{}
	if md, ok := metadata.FromIncomingContext(ctx); !ok {
		return nil
	} else if v := md.Get(key); len(v) == 0 {
		return nil
	} else if err := proto.Unmarshal([]byte(v[0]), msg); err != nil {
		log.Errorf("Invalid incoming metadata: %s", err)
		return nil
	} else if msg.ToolDetails == nil {
		msg.ToolDetails = &pb.ToolDetails{} // Ensure this is non-nil, it's easier for the receiver.
	}
	return msg
}

func (s *server) WaitExecution(req *pb.WaitExecutionRequest, stream pb.Execution_WaitExecutionServer) error {
	log.Info("Received a request to wait for %s", req.Name)
	digest := &pb.Digest{Hash: req.Name}
	ch, _ := s.eventStream(digest, false)
	if ch == nil {
		log.Error("Request for execution %s which is not in progress", req.Name)
		noExecutionInProgress.Inc()
		return status.Errorf(codes.NotFound, "No execution in progress for %s", req.Name)
	}
	return s.streamEvents(digest, ch, stream)
}

// streamEvents streams a series of events back to the client.
func (s *server) streamEvents(digest *pb.Digest, ch <-chan *longrunning.Operation, stream pb.Execution_ExecuteServer) error {
	defer s.stopStream(digest, ch)
	for op := range ch {
		op.Name = digest.Hash
		if err := stream.Send(op); err != nil {
			logr.WithFields(logrus.Fields{
				"hash": digest.Hash,
			}).WithError(err).Warn("Failed to forward event")
			return err
		}
		if op.Done {
			break
		}
	}
	logr.WithFields(logrus.Fields{
		"hash": digest.Hash,
	}).Debug("Completed stream")
	return nil
}

// eventStream registers an event stream for a job.
// The second return value indicates if we created a new execution or are resuming an existing one.
func (s *server) eventStream(digest *pb.Digest, create bool) (<-chan *longrunning.Operation, bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	j, present := s.jobs[digest.Hash]
	created := false
	if !present && !create {
		return nil, created
	} else if !present || (j.Done && create) {
		any, _ := ptypes.MarshalAny(&pb.ExecuteOperationMetadata{
			Stage:        pb.ExecutionStage_QUEUED,
			ActionDigest: digest,
		})
		j = &job{
			Current:    &longrunning.Operation{Metadata: any},
			StartTime:  time.Now(),
			LastUpdate: time.Now(),
		}
		s.jobs[digest.Hash] = j
		logr.WithFields(logrus.Fields{
			"hash": digest.Hash,
		}).Debug("Created job")
		totalRequests.Inc()
		created = true
	} else if create && time.Since(j.LastUpdate) >= resumptionTime {
		// In this path we think the job is too old to be relevant; clear out any existing current info
		// and tell the caller we created a new job..
		j.Current = nil
		j.StartTime = time.Now()
		j.LastUpdate = time.Now()
		j.Done = false
		created = true
	} else {
		logr.WithFields(logrus.Fields{
			"hash": digest.Hash,
		}).Debug("Resuming existing job")
	}
	ch := make(chan *longrunning.Operation, 100)
	if !created && j.Current != nil {
		// This request is resuming an existing stream, give them an update on the latest thing to happen.
		// This helps avoid 504s from taking too long to send response headers since it can be an arbitrary
		// amount of time until we receive the next real update.
		ch <- j.Current
	}
	// Keep this stream if we are going to send further updates on it; don't if
	// we are resuming an existing job that is already completed (in which case there will
	// be no further update and no point for the receiver to keep waiting).
	if created || j.Current == nil || !j.Done {
		j.Streams = append(j.Streams, ch)
	}
	return ch, created
}

// stopStream de-registers the given event stream for a job.
func (s *server) stopStream(digest *pb.Digest, ch <-chan *longrunning.Operation) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	job, present := s.jobs[digest.Hash]
	if !present {
		logr.WithFields(logrus.Fields{
			"hash": digest.Hash,
		}).Warn("stopStream for non-existent job")
		return
	}
	job.Streams = slices.DeleteFunc(job.Streams, func(stream chan *longrunning.Operation) bool {
		return stream == ch
	})
}

// Receive runs forever, receiving responses from the queue.
func (s *server) Receive() {
	wg := sync.WaitGroup{}
	for i := 0; i < s.numPollers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				msg, err := s.responses.Receive(context.Background())
				if err != nil {
					log.Errorf("Failed to receive message: %s", err)
					// TODO(xander): Add an unhealthiness channel here so partial degradation can be reported within Mettle
					totalFailedPubSubMessages.Inc()
					return
				}
				s.process(msg)
				msg.Ack()
			}
		}()
	}
	wg.Wait()
	log.Fatalf("All pollers failed, exiting")
}

// process processes a single message off the responses queue. Can be called concurrently by multiple go routines
func (s *server) process(msg *pubsub.Message) {
	op := &longrunning.Operation{}
	metadata := &pb.ExecuteOperationMetadata{}
	if err := proto.Unmarshal(msg.Body, op); err != nil {
		log.Error("Failed to deserialise message: %s", err)
		return
	} else if err := ptypes.UnmarshalAny(op.Metadata, metadata); err != nil {
		log.Error("Failed to deserialise metadata: %s", err)
		return
	} else if metadata.ActionDigest == nil {
		log.Error("ActionDigest in received message is nil: %s", op)
		return
	}
	if op.Done {
		switch result := op.Result.(type) {
		case *longrunning.Operation_Response, *longrunning.Operation_Error:
		default:
			log.Error("Received a done response with neither response nor error field set: %#v", result)
		}
	}
	worker := common.WorkerName(msg)
	key := metadata.ActionDigest.Hash
	if metadata.Stage == pb.ExecutionStage_COMPLETED {
		if response := unmarshalResponse(op); response != nil && response.Status != nil && response.Status.Code != int32(codes.OK) {
			log.Warning("Got an update for %s from %s, failed update: %s. Done: %v", key, worker, response.Status.Message, op.Done)
			totalFailedActions.Inc()
		} else {
			log.Debug("Got an update for %s from %s, completed successfully. Done: %v", key, worker, op.Done)
			totalSuccessfulActions.Inc()
		}
	} else {
		log.Debug("Got an update for %s from %s, now %s. Done: %v", key, worker, metadata.Stage, op.Done)
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	j, present := s.jobs[key]
	if !present {
		// This is legit, we are getting an update about a job someone else started.
		log.Debug("Update for %s is for a previously unknown job", key)
		j = &job{
			Current:    op,
			LastUpdate: time.Now(),
		}
		s.jobs[key] = j
	}
	if metadata.Stage != pb.ExecutionStage_QUEUED || !j.SentFirst {
		// Only send QUEUED messages if they're the first one. This prevents us from
		// re-broadcasting a QUEUED message after something's already started executing.
		j.SentFirst = true
		if j.Done && !op.Done {
			log.Warning("Got a progress message after completion for %s, discarding", key)
			return
		} else if op.Done {
			j.Done = true
		}
		j.Current = op
		j.LastUpdate = time.Now()
		for _, stream := range j.Streams {
			log.Debug("Dispatching update for %s", key)
			stream <- &longrunning.Operation{
				Name:     op.Name,
				Metadata: op.Metadata,
				Done:     op.Done,
				Result:   op.Result,
			}
		}
		if op.Done {
			if !j.StartTime.IsZero() {
				timeToComplete.Observe(j.LastUpdate.Sub(j.StartTime).Seconds())
			}
			log.Debug("Job %s completed by %s", key, worker)
		}
	}
}

func (s *server) periodicallyDeleteJobs() {
	for range s.deleteJobsTicker.C {
		s.mutex.Lock()
		log.Debug("Starting clean")
		startTime := time.Now()
		maps.DeleteFunc(s.jobs, shouldDeleteJob)
		s.mutex.Unlock()
		log.Debug("Finished clean")
		deleteJobsDurations.Observe(time.Since(startTime).Seconds())
	}
}

func shouldDeleteJob(digest string, j *job) bool {
	timeSinceLastUpdate := time.Since(j.LastUpdate)
	if len(j.Streams) == 0 {
		if j.Done && timeSinceLastUpdate > retentionTime {
			// Job is complete with no listeners, safe to delete
			return true
		} else if !j.Done && timeSinceLastUpdate > expiryTime {
			// Job is incomplete but with no listeners, safe to delete
			return true
		}
	}
	if j.Done && timeSinceLastUpdate > resumptionTime {
		// Job is done and old enough that is it considered stale, safe to delete
		log.Warningf("Will delete job %s: Done: %t, listeners: %d, lastUpdate: %v", digest, j.Done, len(j.Streams), j.LastUpdate)
		return true
	}
	if timeSinceLastUpdate > 2*expiryTime {
		// Job hasn't had an update in forever, safe to delete
		log.Warningf("Will delete job %s: Done: %t, listeners: %d, lastUpdate: %v ", digest, j.Done, len(j.Streams), j.LastUpdate)
		return true
	}
	return false
}

// validatePlatform fetches the platform requirements for this request and checks them.
func (s *server) validatePlatform(req *pb.ExecuteRequest) (map[string]string, error) {
	if !s.actuallyValidate {
		return map[string]string{}, nil
	}
	action := &pb.Action{}
	if _, err := s.client.ReadProto(context.Background(), digest.NewFromProtoUnvalidated(req.ActionDigest), action); err != nil {
		return nil, err
	}
	if action.Platform == nil {
		return map[string]string{}, nil
	}
	m := make(map[string]string, len(action.Platform.Properties))
	for _, prop := range action.Platform.Properties {
		m[prop.Name] = prop.Value
		if len(s.platform) == 0 {
			continue // If nothing is specified as allowed, everything is permitted.
		}
		if allowed, present := s.platform[prop.Name]; !present {
			return nil, status.Errorf(codes.InvalidArgument, "Unsupported platform property %s", prop.Name)
		} else if !slices.Contains(allowed, prop.Value) {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid platform property value %s, must be one of: %s", prop.Name, strings.Join(allowed, ", "))
		} else {
			log.Debug("Valid platform property %s: %s (from %s)", prop.Name, prop.Value, allowed)
		}
	}
	return m, nil
}

// A job represents a single execution request.
type job struct {
	Streams    []chan *longrunning.Operation
	Current    *longrunning.Operation
	StartTime  time.Time
	LastUpdate time.Time
	SentFirst  bool
	Done       bool
}

// unmarshalResponse retrieves the REAPI ExecuteResponse from a longrunning.Operation if it exists.
func unmarshalResponse(op *longrunning.Operation) *pb.ExecuteResponse {
	if result, ok := op.Result.(*longrunning.Operation_Response); ok {
		response := &pb.ExecuteResponse{}
		if err := ptypes.UnmarshalAny(result.Response, response); err != nil {
			log.Warning("Failed to unmarshal response: %s", err)
			return nil
		}
		return response
	}
	return nil
}
