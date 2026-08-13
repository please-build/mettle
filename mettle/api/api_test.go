package api

import (
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
	bpb "github.com/thought-machine/please-servers/proto/mettle"
	"gocloud.dev/pubsub"
	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/mettle/common"
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

func setupRoutingServer(t *testing.T) *routingFixture {
	casaddr := setupCASServer()
	stdQueue := fmt.Sprintf("mem://requests-std-%d", queueID)
	heavyQueue := fmt.Sprintf("mem://requests-heavy-%d", queueID)
	responses := fmt.Sprintf("mem://responses-routing-%d", queueID)
	queueID++
	queues := PubSubOpts{
		RequestQueue:          stdQueue,
		RequestQueues:         []string{"heavy=" + heavyQueue},
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
	}, "", queues, "", true, map[string][]string{}, casaddr, false)
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
	f := setupRoutingServer(t)
	heavy := registerAction(t, &pb.Platform_Property{Name: "worker-type", Value: "heavy"})
	std := registerAction(t, &pb.Platform_Property{Name: "worker-type", Value: "standard"})

	f.execute(t, heavy)
	assert.Equal(t, heavy.Hash, receiveWithin(t, f.heavy, "heavy").Hash)

	f.execute(t, std)
	assert.Equal(t, std.Hash, receiveWithin(t, f.std, "standard").Hash)
}

func TestExecuteRoutingFallsBackToStandard(t *testing.T) {
	f := setupRoutingServer(t)

	for name, d := range map[string]*pb.Digest{
		"no platform":                 registerAction(t),
		"no worker-type property":     registerAction(t, &pb.Platform_Property{Name: "OSFamily", Value: "linux"}),
		"unconfigured worker-type":    registerAction(t, &pb.Platform_Property{Name: "worker-type", Value: "gpu"}),
		"action missing from the CAS": {Hash: strings.Repeat("ab", 32), SizeBytes: 10},
	} {
		t.Run(name, func(t *testing.T) {
			f.execute(t, d)
			assert.Equal(t, d.Hash, receiveWithin(t, f.std, "standard").Hash)
		})
	}
}

func TestParseRequestQueues(t *testing.T) {
	tests := []struct {
		name          string
		requestQueue  string
		requestQueues []string
		expected      map[string]string
		err           string
	}{
		{
			name:         "default queue only",
			requestQueue: "mem://std",
			expected:     map[string]string{"standard": "mem://std"},
		},
		{
			name:          "default queue and extra queues",
			requestQueue:  "mem://std",
			requestQueues: []string{"heavy=mem://heavy", "gpu=mem://gpu"},
			expected:      map[string]string{"standard": "mem://std", "heavy": "mem://heavy", "gpu": "mem://gpu"},
		},
		{
			name:          "standard queue provided via the list",
			requestQueues: []string{"standard=mem://std", "heavy=mem://heavy"},
			expected:      map[string]string{"standard": "mem://std", "heavy": "mem://heavy"},
		},
		{
			name:          "url containing an equals sign",
			requestQueue:  "mem://std",
			requestQueues: []string{"heavy=gcppubsub://projects/p/topics/t?opt=1"},
			expected:      map[string]string{"standard": "mem://std", "heavy": "gcppubsub://projects/p/topics/t?opt=1"},
		},
		{
			name: "nothing configured",
			err:  "must be provided",
		},
		{
			name:          "no standard queue",
			requestQueues: []string{"heavy=mem://heavy"},
			err:           "must be provided",
		},
		{
			name:          "missing separator",
			requestQueue:  "mem://std",
			requestQueues: []string{"heavy"},
			err:           "invalid request queue format",
		},
		{
			name:          "empty name",
			requestQueue:  "mem://std",
			requestQueues: []string{"=mem://heavy"},
			err:           "invalid request queue format",
		},
		{
			name:          "empty url",
			requestQueue:  "mem://std",
			requestQueues: []string{"heavy="},
			err:           "invalid request queue format",
		},
		{
			name:          "duplicate name",
			requestQueue:  "mem://std",
			requestQueues: []string{"heavy=mem://a", "heavy=mem://b"},
			err:           "more than once",
		},
		{
			name:          "standard queue overridden by the list",
			requestQueue:  "mem://std",
			requestQueues: []string{"standard=mem://other"},
			err:           "more than once",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			urls, err := parseRequestQueues(tc.requestQueue, tc.requestQueues)
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
