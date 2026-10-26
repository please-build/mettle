package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thought-machine/go-flags"
	bpb "github.com/thought-machine/please-servers/proto/mettle"
	"gocloud.dev/pubsub"
	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/mettle/common"
	"github.com/thought-machine/please-servers/rexclient"
)

const (
	uncachedHash = "1234"
	failedHash   = "3456"
	flakyHash    = "5678"
)

var (
	flakyFail = true // determines whether flakyHash passes or fails
	queueID   = 1    // id of queues created in the next call to setupServers
)

func TestUncached(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()
	runExecution(t, client, ex, uncachedHash, 0)
}

func TestCreateNewInsteadOfResume(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()
	runExecution(t, client, ex, uncachedHash, 0)

	// Temporarily set this to zero so we don't have to wait ten minutes in this test.
	old := resumptionTime
	resumptionTime = 0
	defer func() { resumptionTime = old }()

	// We should now get a complete new execution since this job counts as 'expired'
	runExecution(t, client, ex, uncachedHash, 0)
}

func TestFlaky(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()

	flakyFail = true
	runExecution(t, client, ex, flakyHash, 1)
	flakyFail = false
	runExecution(t, client, ex, flakyHash, 0)
}

func TestTwiceFlaky(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()

	flakyFail = true
	runExecution(t, client, ex, flakyHash, 1)
	runExecution(t, client, ex, flakyHash, 1)
	flakyFail = false
	runExecution(t, client, ex, flakyHash, 0)
}

func TestExecuteAndWait(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()

	const hash = uncachedHash
	stream1, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: &pb.Digest{Hash: hash},
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream1, hash, pb.ExecutionStage_CACHE_CHECK)
	op := receiveUpdate(t, stream1, hash, pb.ExecutionStage_QUEUED)

	stream2, err := client.WaitExecution(context.Background(), &pb.WaitExecutionRequest{
		Name: op.Name,
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream2, hash, pb.ExecutionStage_QUEUED)
	assert.Equal(t, hash, ex.Receive().Hash)

	receiveUpdate(t, stream1, hash, pb.ExecutionStage_EXECUTING)
	receiveUpdate(t, stream2, hash, pb.ExecutionStage_EXECUTING)

	ex.Finish(&pb.Digest{Hash: hash})
	op1 := receiveUpdate(t, stream1, hash, pb.ExecutionStage_COMPLETED)
	op2 := receiveUpdate(t, stream2, hash, pb.ExecutionStage_COMPLETED)
	checkExitCode(t, op1, 0)
	checkExitCode(t, op2, 0)
}

func TestExecuteAndWaitTwice(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()

	const hash = uncachedHash
	stream1, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: &pb.Digest{Hash: hash},
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream1, hash, pb.ExecutionStage_CACHE_CHECK)
	op := receiveUpdate(t, stream1, hash, pb.ExecutionStage_QUEUED)

	stream2, err := client.WaitExecution(context.Background(), &pb.WaitExecutionRequest{
		Name: op.Name,
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream2, hash, pb.ExecutionStage_QUEUED)

	stream3, err := client.WaitExecution(context.Background(), &pb.WaitExecutionRequest{
		Name: op.Name,
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream3, hash, pb.ExecutionStage_QUEUED)

	assert.Equal(t, hash, ex.Receive().Hash)

	receiveUpdate(t, stream1, hash, pb.ExecutionStage_EXECUTING)
	receiveUpdate(t, stream2, hash, pb.ExecutionStage_EXECUTING)
	receiveUpdate(t, stream3, hash, pb.ExecutionStage_EXECUTING)

	ex.Finish(&pb.Digest{Hash: hash})
	op1 := receiveUpdate(t, stream1, hash, pb.ExecutionStage_COMPLETED)
	op2 := receiveUpdate(t, stream2, hash, pb.ExecutionStage_COMPLETED)
	op3 := receiveUpdate(t, stream3, hash, pb.ExecutionStage_COMPLETED)
	checkExitCode(t, op1, 0)
	checkExitCode(t, op2, 0)
	checkExitCode(t, op3, 0)
}

func TestExecuteAndWaitLater(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()

	const hash = uncachedHash
	stream1, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: &pb.Digest{Hash: hash},
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream1, hash, pb.ExecutionStage_CACHE_CHECK)
	op := receiveUpdate(t, stream1, hash, pb.ExecutionStage_QUEUED)

	assert.Equal(t, hash, ex.Receive().Hash)
	receiveUpdate(t, stream1, hash, pb.ExecutionStage_EXECUTING)

	stream2, err := client.WaitExecution(context.Background(), &pb.WaitExecutionRequest{
		Name: op.Name,
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream2, hash, pb.ExecutionStage_EXECUTING)

	ex.Finish(&pb.Digest{Hash: hash})
	op1 := receiveUpdate(t, stream1, hash, pb.ExecutionStage_COMPLETED)
	op2 := receiveUpdate(t, stream2, hash, pb.ExecutionStage_COMPLETED)
	checkExitCode(t, op1, 0)
	checkExitCode(t, op2, 0)
}

func TestExecuteAndWaitAfterCompletion(t *testing.T) {
	client, ex, s := setupServers(t)
	defer s.Stop()

	const hash = uncachedHash
	stream1, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: &pb.Digest{Hash: hash},
	})
	assert.NoError(t, err)
	receiveUpdate(t, stream1, hash, pb.ExecutionStage_CACHE_CHECK)
	op := receiveUpdate(t, stream1, hash, pb.ExecutionStage_QUEUED)
	assert.Equal(t, hash, ex.Receive().Hash)
	receiveUpdate(t, stream1, hash, pb.ExecutionStage_EXECUTING)
	ex.Finish(&pb.Digest{Hash: hash})
	op1 := receiveUpdate(t, stream1, hash, pb.ExecutionStage_COMPLETED)
	checkExitCode(t, op1, 0)

	stream2, err := client.WaitExecution(context.Background(), &pb.WaitExecutionRequest{
		Name: op.Name,
	})
	assert.NoError(t, err)
	op2 := receiveUpdate(t, stream2, hash, pb.ExecutionStage_COMPLETED)
	checkExitCode(t, op2, 0)
}

func TestShouldDeleteJob(t *testing.T) {
	now := time.Now()
	var tests = []struct {
		name         string
		job          *job
		shouldDelete bool
	}{
		{
			name: "incomplete job doesn't expire",
			job: &job{
				Done:       false,
				LastUpdate: now.Add(-1 * time.Minute),
			},
			shouldDelete: false,
		},
		{
			name: "completed job within retention time does not expire",
			job: &job{
				Done:       true,
				LastUpdate: now.Add(-1 * time.Minute),
			},
			shouldDelete: false,
		},
		{
			name: "completed job after retention time and no listeners does expire",
			job: &job{
				Done:       true,
				LastUpdate: now.Add(-6 * time.Minute),
				Streams:    []chan *longrunning.Operation{},
			},
			shouldDelete: true,
		},
		{
			name: "complete job with active listeners after 1x resumption time does expire",
			job: &job{
				Done:       true,
				LastUpdate: now.Add(-11 * time.Minute),
				Streams:    []chan *longrunning.Operation{make(chan *longrunning.Operation), make(chan *longrunning.Operation)},
			},
			shouldDelete: true,
		},
		{
			name: "incomplete job with no listeners within expiry time does not expire",
			job: &job{
				Done:       false,
				LastUpdate: now.Add(-59 * time.Minute),
			},
			shouldDelete: false,
		},
		{
			name: "incomplete job with no listeners after 1x expiry time does expire",
			job: &job{
				Done:       false,
				LastUpdate: now.Add(-61 * time.Minute),
			},
			shouldDelete: true,
		},
		{
			name: "incomplete job with listeners after 2x expiry time does expire",
			job: &job{
				Done:       false,
				LastUpdate: now.Add(-121 * time.Minute),
				Streams:    []chan *longrunning.Operation{make(chan *longrunning.Operation), make(chan *longrunning.Operation)},
			},
			shouldDelete: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.shouldDelete, shouldDeleteJob("1234", test.job))
		})
	}
}

func runExecution(t *testing.T, client pb.ExecutionClient, ex *executor, hash string, expectedExitCode int) {
	stream, err := client.Execute(context.Background(), &pb.ExecuteRequest{
		ActionDigest: &pb.Digest{Hash: hash},
	})
	assert.NoError(t, err)

	receiveUpdate(t, stream, hash, pb.ExecutionStage_CACHE_CHECK)
	receiveUpdate(t, stream, hash, pb.ExecutionStage_QUEUED)
	assert.Equal(t, hash, ex.Receive().Hash)
	receiveUpdate(t, stream, hash, pb.ExecutionStage_EXECUTING)
	ex.Finish(&pb.Digest{Hash: hash})
	op := receiveUpdate(t, stream, hash, pb.ExecutionStage_COMPLETED)
	checkExitCode(t, op, expectedExitCode)
}

func receiveUpdate(t *testing.T, stream pb.Execution_ExecuteClient, expectedHash string, expectedStage pb.ExecutionStage_Value) *longrunning.Operation {
	log.Debug("Waiting for %s update...", expectedStage)
	op, metadata := recv(stream)
	assert.Equal(t, expectedStage, metadata.Stage)
	assert.Equal(t, expectedHash, metadata.ActionDigest.Hash)
	log.Debug("Received %s update (expected %s)", metadata.Stage, expectedStage)
	if expectedStage == pb.ExecutionStage_COMPLETED {
		// The stream should also have ended at this point.
		_, err := stream.Recv()
		assert.Error(t, err)
		assert.Equal(t, err, io.EOF)
	}
	return op
}

func checkExitCode(t *testing.T, op *longrunning.Operation, expectedExitCode int) {
	response := &pb.ExecuteResponse{}
	err := ptypes.UnmarshalAny(op.GetResponse(), response)
	assert.NoError(t, err)
	assert.NotNil(t, response.Result)
	assert.EqualValues(t, expectedExitCode, response.Result.ExitCode)
}

func setupServers(t *testing.T) (pb.ExecutionClient, *executor, *grpc.Server) {
	casaddr := setupCASServer()
	requests := fmt.Sprintf("mem://requests%d", queueID)
	responses := fmt.Sprintf("mem://responses%d", queueID)
	queues := PubSubOpts{
		RequestQueue:          requests,
		ResponseQueue:         responses,
		PreResponseQueue:      responses,
		NumPollers:            1,
		SubscriptionBatchSize: 1,
	}
	queueID++
	common.MustOpenTopic(requests)  // Ensure these are created before anything tries
	common.MustOpenTopic(responses) // to open a subscription to either.
	s, lis, err := serve(grpcutil.Opts{
		Host: "127.0.0.1",
		Port: 0,
	}, "", queues, "", true, map[string][]string{}, casaddr, false)
	require.NoError(t, err)
	go s.Serve(lis)
	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)
	return pb.NewExecutionClient(conn), newExecutor(requests, responses), s
}

func setupCASServer() string {
	srv := cas{}
	lis, s := grpcutil.NewServer(grpcutil.Opts{
		Host: "127.0.0.1",
		Port: 0,
	})
	bs.RegisterByteStreamServer(s, srv)
	pb.RegisterCapabilitiesServer(s, srv)
	go s.Serve(lis)
	return lis.Addr().String()
}

func TestGetExecutions(t *testing.T) {
	port := 9990
	opts := grpcutil.Opts{
		Host:      "127.0.0.1",
		Port:      port,
		CertFile:  "",
		TokenFile: "",
	}
	srv := &server{
		name: "mettle API server",
		jobs: loadJob(),
	}
	lis, s := grpcutil.NewServer(opts)
	bpb.RegisterBootstrapServer(s, srv)
	go s.Serve(lis)

	jobs, err := getExecutions(opts, "127.0.0.1:9990", false)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(jobs))
	assert.Equal(t, "Unfinished Operation", jobs["1234"].Current.Name)
}

func loadJob() map[string]*job {
	jobs := map[string]*job{
		"1234": {
			Current: &longrunning.Operation{
				Name: "Unfinished Operation",
				Done: true,
			},
			SentFirst: true,
			Done:      false,
		},
		"5678": {
			Current: &longrunning.Operation{
				Name: "Finished Operation",
				Done: true,
			},
			SentFirst: true,
			Done:      true,
		},
	}
	return jobs
}

// A routingFixture is an API server configured with a standard and a heavy request queue,
// along with executors consuming from each of them.
type routingFixture struct {
	client pb.ExecutionClient
	std    *executor
	heavy  *executor
}

func setupRoutingServer(t *testing.T, allowedPlatform map[string][]string) *routingFixture {
	casaddr := setupCASServer()
	stdQueue := fmt.Sprintf("mem://requests-std-%d", queueID)
	heavyQueue := fmt.Sprintf("mem://requests-heavy-%d", queueID)
	responses := fmt.Sprintf("mem://responses-routing-%d", queueID)
	queueID++
	queues := PubSubOpts{
		RequestQueue:          stdQueue,
		ExtraRequestQueues:    map[string]string{"heavy": heavyQueue},
		ResponseQueue:         responses,
		PreResponseQueue:      responses,
		NumPollers:            1,
		SubscriptionBatchSize: 1,
	}
	common.MustOpenTopic(stdQueue)
	common.MustOpenTopic(heavyQueue)
	common.MustOpenTopic(responses)

	s, lis, err := serve(grpcutil.Opts{
		Host: "127.0.0.1",
		Port: 0,
	}, "", queues, "", true, allowedPlatform, casaddr, false)
	require.NoError(t, err)
	go s.Serve(lis)
	t.Cleanup(s.Stop)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithInsecure())
	require.NoError(t, err)
	return &routingFixture{
		client: pb.NewExecutionClient(conn),
		std:    newExecutor(stdQueue, responses),
		heavy:  newExecutor(heavyQueue, responses),
	}
}

// execute sends an ExecuteRequest for the given action digest and waits for it to be queued.
func (f *routingFixture) execute(t *testing.T, d *pb.Digest) {
	stream, err := f.client.Execute(context.Background(), &pb.ExecuteRequest{ActionDigest: d})
	require.NoError(t, err)
	receiveUpdate(t, stream, d.Hash, pb.ExecutionStage_CACHE_CHECK)
	receiveUpdate(t, stream, d.Hash, pb.ExecutionStage_QUEUED)
}

// receiveWithin returns the digest of the next request on the executor's queue, failing the test
// if nothing arrives (as opposed to blocking forever when a request is misrouted).
func receiveWithin(t *testing.T, ex *executor, queue string) *pb.Digest {
	ch := make(chan *pb.Digest, 1)
	go func() { ch <- ex.Receive() }()
	select {
	case d := <-ch:
		return d
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for a request on the "+queue+" queue")
		return nil
	}
}

// registerAction stores an action with the given platform properties in the fake CAS and returns its digest.
func registerAction(t *testing.T, properties ...*pb.Platform_Property) *pb.Digest {
	action := &pb.Action{}
	if len(properties) > 0 {
		action.Platform = &pb.Platform{Properties: properties}
	}
	b, err := proto.Marshal(action)
	require.NoError(t, err)
	h := sha256.Sum256(b)
	d := &pb.Digest{Hash: hex.EncodeToString(h[:]), SizeBytes: int64(len(b))}
	setCASBlob(d.Hash, b)
	return d
}

func TestExecuteHeavyRouting(t *testing.T) {
	f := setupRoutingServer(t, map[string][]string{})
	heavy := registerAction(t, &pb.Platform_Property{Name: "worker-type", Value: "heavy"})
	std := registerAction(t, &pb.Platform_Property{Name: "worker-type", Value: "standard"})

	f.execute(t, heavy)
	assert.Equal(t, heavy.Hash, receiveWithin(t, f.heavy, "heavy").Hash)

	f.execute(t, std)
	assert.Equal(t, std.Hash, receiveWithin(t, f.std, "standard").Hash)
}

// TestExecuteRoutingWithPlatformValidation checks that worker-type is permitted for the configured queues
// without having to be listed in the allowed platform, while other properties are still validated.
func TestExecuteRoutingWithPlatformValidation(t *testing.T) {
	f := setupRoutingServer(t, map[string][]string{"OSFamily": {"linux"}})
	heavy := registerAction(t, &pb.Platform_Property{Name: "OSFamily", Value: "linux"}, &pb.Platform_Property{Name: "worker-type", Value: "heavy"})
	f.execute(t, heavy)
	assert.Equal(t, heavy.Hash, receiveWithin(t, f.heavy, "heavy").Hash)

	for name, d := range map[string]*pb.Digest{
		"unconfigured worker-type": registerAction(t, &pb.Platform_Property{Name: "worker-type", Value: "gpu"}),
		"invalid other property":   registerAction(t, &pb.Platform_Property{Name: "OSFamily", Value: "windows"}),
	} {
		t.Run(name, func(t *testing.T) {
			stream, err := f.client.Execute(context.Background(), &pb.ExecuteRequest{ActionDigest: d})
			require.NoError(t, err)
			_, err = stream.Recv()
			assert.Equal(t, codes.InvalidArgument, status.Code(err))
		})
	}
}

func TestExecuteFailsIfActionCannotBeRead(t *testing.T) {
	f := setupRoutingServer(t, map[string][]string{})
	d := &pb.Digest{Hash: strings.Repeat("ab", 32), SizeBytes: 10}

	stream, err := f.client.Execute(context.Background(), &pb.ExecuteRequest{ActionDigest: d})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Error(t, err)
}

func TestExecuteRoutingFallsBackToStandard(t *testing.T) {
	f := setupRoutingServer(t, map[string][]string{})

	for name, d := range map[string]*pb.Digest{
		"no platform":              registerAction(t),
		"no worker-type property":  registerAction(t, &pb.Platform_Property{Name: "OSFamily", Value: "linux"}),
		"unconfigured worker-type": registerAction(t, &pb.Platform_Property{Name: "worker-type", Value: "gpu"}),
	} {
		t.Run(name, func(t *testing.T) {
			f.execute(t, d)
			assert.Equal(t, d.Hash, receiveWithin(t, f.std, "standard").Hash)
		})
	}
}

func TestMergeRequestQueues(t *testing.T) {
	tests := []struct {
		name               string
		requestQueue       string
		extraRequestQueues map[string]string
		expected           map[string]string
		err                string
	}{
		{
			name:         "default queue only",
			requestQueue: "mem://std",
			expected:     map[string]string{"standard": "mem://std"},
		},
		{
			name:               "default queue and extra queues",
			requestQueue:       "mem://std",
			extraRequestQueues: map[string]string{"heavy": "mem://heavy", "gpu": "mem://gpu"},
			expected:           map[string]string{"standard": "mem://std", "heavy": "mem://heavy", "gpu": "mem://gpu"},
		},
		{
			name:               "standard queue provided via the map",
			extraRequestQueues: map[string]string{"standard": "mem://std", "heavy": "mem://heavy"},
			expected:           map[string]string{"standard": "mem://std", "heavy": "mem://heavy"},
		},
		{
			name: "nothing configured",
			err:  "must be provided",
		},
		{
			name:               "no standard queue",
			extraRequestQueues: map[string]string{"heavy": "mem://heavy"},
			err:                "must be provided",
		},
		{
			name:               "empty name",
			requestQueue:       "mem://std",
			extraRequestQueues: map[string]string{"": "mem://heavy"},
			err:                "invalid request queue",
		},
		{
			name:               "empty url",
			requestQueue:       "mem://std",
			extraRequestQueues: map[string]string{"heavy": ""},
			err:                "invalid request queue",
		},
		{
			name:               "standard queue configured twice",
			requestQueue:       "mem://std",
			extraRequestQueues: map[string]string{"standard": "mem://other"},
			err:                "configured twice",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			urls, err := mergeRequestQueues(tc.requestQueue, tc.extraRequestQueues)
			if tc.err != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, urls)
		})
	}
}

// TestExtraRequestQueuesFlagParsing checks that the queues can be given as name:url pairs, including urls containing colons.
func TestExtraRequestQueuesFlagParsing(t *testing.T) {
	var opts PubSubOpts
	_, err := flags.NewParser(&opts, flags.None).ParseArgs([]string{
		"--response_queue=mem://responses",
		"--pre_response_queue=mem://responses",
		"--extra_request_queues=heavy:gcppubsub://projects/p/topics/heavy",
		"--extra_request_queues=gpu:mem://gpu",
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		"heavy": "gcppubsub://projects/p/topics/heavy",
		"gpu":   "mem://gpu",
	}, opts.ExtraRequestQueues)
}

func recv(stream pb.Execution_ExecuteClient) (*longrunning.Operation, *pb.ExecuteOperationMetadata) {
	op, err := stream.Recv()
	if err != nil {
		log.Fatalf("Failed to receive message: %s", err)
	}
	metadata := &pb.ExecuteOperationMetadata{}
	if err := ptypes.UnmarshalAny(op.Metadata, metadata); err != nil {
		log.Fatalf("Failed to deserialise metadata: %s", err)
	}
	return op, metadata
}

type executor struct {
	requests  *pubsub.Subscription
	responses *pubsub.Topic
}

func newExecutor(requests, responses string) *executor {
	return &executor{
		requests:  common.MustOpenSubscription(requests, 1),
		responses: common.MustOpenTopic(responses),
	}
}

// Receive receives the next request from the queue and begins "execution" of it.
func (ex *executor) Receive() *pb.Digest {
	msg, _ := ex.requests.Receive(context.Background())
	req := &pb.ExecuteRequest{}
	if err := proto.Unmarshal(msg.Body, req); err != nil {
		log.Fatalf("Failed to deserialise message: %s", err)
	}
	metadata, _ := ptypes.MarshalAny(&pb.ExecuteOperationMetadata{
		Stage:        pb.ExecutionStage_EXECUTING,
		ActionDigest: req.ActionDigest,
	})
	b, _ := proto.Marshal(&longrunning.Operation{
		Name:     req.ActionDigest.Hash,
		Metadata: metadata,
	})
	ex.responses.Send(context.Background(), &pubsub.Message{Body: b})
	msg.Ack()
	return req.ActionDigest
}

// Finish "completes" execution and sends a response.
func (ex *executor) Finish(digest *pb.Digest) {
	metadata, _ := ptypes.MarshalAny(&pb.ExecuteOperationMetadata{
		Stage:        pb.ExecutionStage_COMPLETED,
		ActionDigest: digest,
	})
	if digest.Hash == failedHash || (digest.Hash == flakyHash && flakyFail) {
		response, _ := ptypes.MarshalAny(&pb.ExecuteResponse{
			Result: &pb.ActionResult{
				ExitCode: 1,
			},
		})
		b, _ := proto.Marshal(&longrunning.Operation{
			Name:     digest.Hash,
			Metadata: metadata,
			Done:     true,
			Result:   &longrunning.Operation_Response{Response: response},
		})
		ex.responses.Send(context.Background(), &pubsub.Message{Body: b})
	} else {
		response, _ := ptypes.MarshalAny(&pb.ExecuteResponse{
			Result: &pb.ActionResult{},
		})
		b, _ := proto.Marshal(&longrunning.Operation{
			Name:     digest.Hash,
			Metadata: metadata,
			Done:     true,
			Result:   &longrunning.Operation_Response{Response: response},
		})
		ex.responses.Send(context.Background(), &pubsub.Message{Body: b})
	}
}

var (
	casBlobs   = map[string][]byte{}
	casBlobsMu sync.RWMutex
)

func setCASBlob(hash string, data []byte) {
	casBlobsMu.Lock()
	defer casBlobsMu.Unlock()
	casBlobs[hash] = data
}

func getCASBlob(hash string) ([]byte, bool) {
	casBlobsMu.RLock()
	defer casBlobsMu.RUnlock()
	data, ok := casBlobs[hash]
	return data, ok
}

// A cas is a fake implementation of the CAS server.
type cas struct{}

func (c cas) Read(req *bs.ReadRequest, srv bs.ByteStream_ReadServer) error {
	parts := strings.Split(req.ResourceName, "/")
	if len(parts) >= 3 {
		hash := parts[len(parts)-2]
		if data, ok := getCASBlob(hash); ok {
			return srv.Send(&bs.ReadResponse{
				Data: data,
			})
		}
	}
	return status.Errorf(codes.NotFound, "blob %s not found", req.ResourceName)
}

func (c cas) Write(srv bs.ByteStream_WriteServer) error {
	return status.Errorf(codes.Unimplemented, "bytestream.Write not implemented")
}

func (c cas) QueryWriteStatus(ctx context.Context, req *bs.QueryWriteStatusRequest) (*bs.QueryWriteStatusResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "bytestream.QueryWriteStatus not implemented")
}

func (c cas) GetCapabilities(ctx context.Context, req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	return &pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunctions: []pb.DigestFunction_Value{
				pb.DigestFunction_SHA256,
			},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{
				UpdateEnabled: true,
			},
			SupportedCompressors: []pb.Compressor_Value{
				pb.Compressor_IDENTITY,
				pb.Compressor_ZSTD,
			},
			SupportedBatchUpdateCompressors: []pb.Compressor_Value{
				pb.Compressor_IDENTITY,
				pb.Compressor_ZSTD,
			},
		},
	}, nil
}

func TestMain(m *testing.M) {
	grpcutil.Silence()
	os.Exit(m.Run())
}

// fakeStream is a minimal implementation of an Execute stream that records what is sent on it.
type fakeStream struct {
	pb.Execution_ExecuteServer
	ops []*longrunning.Operation
}

func (f *fakeStream) Send(op *longrunning.Operation) error {
	f.ops = append(f.ops, op)
	return nil
}

func (f *fakeStream) Context() context.Context {
	return context.Background()
}

func newTestServer() *server {
	return &server{
		jobs:      map[string]*job{},
		completed: map[string]time.Time{},
	}
}

// testPublishTimeKey is a metadata key used to tell the server when a message was published, in place of Pub/Sub.
const testPublishTimeKey = "test_publish_time"

// usePublishTimeFromMetadata makes the server read publish times from message metadata for the duration of a test.
func usePublishTimeFromMetadata(t *testing.T) {
	old := publishTime
	publishTime = func(msg *pubsub.Message) time.Time {
		published, _ := time.Parse(time.RFC3339Nano, msg.Metadata[testPublishTimeKey])
		return published
	}
	t.Cleanup(func() { publishTime = old })
}

// progressMessage returns a message from the response queue updating the given action to the given stage.
func progressMessage(hash string, stage pb.ExecutionStage_Value, published time.Time) *pubsub.Message {
	metadata, _ := ptypes.MarshalAny(&pb.ExecuteOperationMetadata{
		Stage:        stage,
		ActionDigest: &pb.Digest{Hash: hash},
	})
	op := &longrunning.Operation{Name: hash, Metadata: metadata}
	if stage == pb.ExecutionStage_COMPLETED {
		response, _ := ptypes.MarshalAny(&pb.ExecuteResponse{Result: &pb.ActionResult{}})
		op.Done = true
		op.Result = &longrunning.Operation_Response{Response: response}
	}
	b, _ := proto.Marshal(op)
	return &pubsub.Message{
		Body:     b,
		Metadata: map[string]string{testPublishTimeKey: published.Format(time.RFC3339Nano)},
	}
}

func TestStaleMessagesDoNotRecreateCompletedJobs(t *testing.T) {
	usePublishTimeFromMetadata(t)
	s := newTestServer()
	const hash = uncachedHash
	completed := time.Now()

	s.process(progressMessage(hash, pb.ExecutionStage_COMPLETED, completed))
	require.Contains(t, s.jobs, hash)
	assert.True(t, s.jobs[hash].Done)

	// Once the retention time has passed the job gets deleted.
	s.jobs[hash].LastUpdate = time.Now().Add(-2 * retentionTime)
	s.deleteJobs()
	require.NotContains(t, s.jobs, hash)

	// Messages that were published before it completed but have been redelivered late should not recreate it.
	s.process(progressMessage(hash, pb.ExecutionStage_QUEUED, completed.Add(-2*time.Second)))
	s.process(progressMessage(hash, pb.ExecutionStage_EXECUTING, completed.Add(-time.Second)))
	s.process(progressMessage(hash, pb.ExecutionStage_COMPLETED, completed))
	assert.NotContains(t, s.jobs, hash)

	// A new execution started after it completed should be tracked as normal.
	s.process(progressMessage(hash, pb.ExecutionStage_QUEUED, completed.Add(time.Second)))
	require.Contains(t, s.jobs, hash)
	assert.False(t, s.jobs[hash].Done)

	// A late duplicate of the previous completion shouldn't complete the new execution...
	s.process(progressMessage(hash, pb.ExecutionStage_COMPLETED, completed))
	assert.False(t, s.jobs[hash].Done)

	// ...but its own completion should.
	s.process(progressMessage(hash, pb.ExecutionStage_COMPLETED, completed.Add(2*time.Second)))
	assert.True(t, s.jobs[hash].Done)
}

func TestCompletionsAreForgottenAfterRetentionTime(t *testing.T) {
	s := newTestServer()
	s.completed["old"] = time.Now().Add(-completionRetentionTime - time.Minute)
	s.completed["new"] = time.Now()
	s.deleteJobs()
	assert.NotContains(t, s.completed, "old")
	assert.Contains(t, s.completed, "new")
}

func TestAbandonedJobEndsStreams(t *testing.T) {
	s := newTestServer()
	d := &pb.Digest{Hash: uncachedHash}
	ch, created := s.eventStream(d, true)
	require.True(t, created)

	// The job never progresses, so eventually gets deleted.
	s.jobs[d.Hash].LastUpdate = time.Now().Add(-3 * expiryTime)
	s.deleteJobs()
	assert.NotContains(t, s.jobs, d.Hash)

	// The client waiting on it should get an error so it retries, rather than waiting forever.
	err := s.streamEvents(d, ch, &fakeStream{})
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestFailedSubmissionDoesNotLeaveJobBehind(t *testing.T) {
	client, err := rexclient.New("test", setupCASServer(), false, "")
	require.NoError(t, err)
	d := registerAction(t)
	requests := fmt.Sprintf("mem://failing-requests-%d", queueID)
	responses := fmt.Sprintf("mem://failing-responses-%d", queueID)
	queueID++
	failing := common.MustOpenTopic(requests)
	require.NoError(t, failing.Shutdown(context.Background()))
	s := newTestServer()
	s.client = client
	s.requests = map[string]*pubsub.Topic{defaultRequestQueue: failing}
	s.preResponses = common.MustOpenTopic(responses)

	req := &pb.ExecuteRequest{ActionDigest: d, SkipCacheLookup: true}
	err = s.Execute(req, &fakeStream{})
	assert.Equal(t, codes.Unavailable, status.Code(err))
	assert.NotContains(t, s.jobs, d.Hash)

	// When the client retries, the request should be submitted again rather than resuming a job that was never queued.
	working := common.MustOpenTopic(requests + "-working")
	sub := common.MustOpenSubscription(requests+"-working", 1)
	s.mutex.Lock()
	s.requests = map[string]*pubsub.Topic{defaultRequestQueue: working}
	s.mutex.Unlock()
	go s.Execute(req, &fakeStream{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg, err := sub.Receive(ctx)
	require.NoError(t, err)
	submitted := &pb.ExecuteRequest{}
	require.NoError(t, proto.Unmarshal(msg.Body, submitted))
	assert.Equal(t, d.Hash, submitted.ActionDigest.Hash)
}

func TestPubSubOptsHelpShowsEnvVars(t *testing.T) {
	var opts PubSubOpts
	var buf bytes.Buffer
	flags.NewParser(&opts, flags.None).WriteHelp(&buf)
	assert.Contains(t, buf.String(), "[$API_NUM_POLLERS]")
	assert.Contains(t, buf.String(), "[$API_SUBSCRIPTION]")
}
