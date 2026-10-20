// Package rpc implements the load-balancing logic for forwarding all the RPCs.
package rpc

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	apb "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/golang/protobuf/proto"
	"github.com/peterebden/go-cli-init/v4/logging"
	"github.com/peterebden/go-sri"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/codes"
	hpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/thought-machine/please-servers/flair/trie"
	"github.com/thought-machine/please-servers/grpcutil"
	ppb "github.com/thought-machine/please-servers/proto/purity"
	"github.com/thought-machine/please-servers/rexclient"
)

var log = logging.MustGetLogger()
var logr = logrus.New()

// emptyHash is the sha256 hash of the empty file.
var emptyHash = digest.Empty.Hash

// ServeForever serves on the given port until terminated.
func ServeForever(opts grpcutil.Opts, casReplicator, assetReplicator, executorReplicator *trie.Replicator, timeout time.Duration) {
	srv := &server{
		replicator:      casReplicator,
		assetReplicator: assetReplicator,
		exeReplicator:   executorReplicator,
		bytestreamRe:    regexp.MustCompile("(?:uploads/[0-9a-f-]+/)?(blobs|compressed-blobs/zstd)/([0-9a-f]+)/([0-9]+)"),
		timeout:         timeout,
	}
	healthSrv := &healthServer{
		replicator:      casReplicator,
		assetReplicator: assetReplicator,
		exeReplicator:   executorReplicator,
	}
	opts.NoHealth = true // We will do this ourselves.
	lis, s := grpcutil.NewServer(opts)
	pb.RegisterCapabilitiesServer(s, srv)
	pb.RegisterActionCacheServer(s, srv)
	pb.RegisterContentAddressableStorageServer(s, srv)
	bs.RegisterByteStreamServer(s, srv)
	if assetReplicator != nil {
		apb.RegisterFetchServer(s, srv)
	}
	if executorReplicator != nil {
		pb.RegisterExecutionServer(s, srv)
	}
	ppb.RegisterGCServer(s, srv)
	hpb.RegisterHealthServer(s, healthSrv)
	grpcutil.ServeForever(lis, s)
}

type server struct {
	ppb.UnimplementedGCServer
	replicator, assetReplicator, exeReplicator *trie.Replicator
	bytestreamRe                               *regexp.Regexp
	timeout                                    time.Duration
}

// healthServer is a separate struct (rather than embedding hpb.UnimplementedHealthServer into server)
// because ppb.UnimplementedGCServer and hpb.UnimplementedHealthServer both define a List method with
// different signatures, which would cause a compile-time conflict.
type healthServer struct {
	hpb.UnimplementedHealthServer
	replicator, assetReplicator, exeReplicator *trie.Replicator
}

func (s *healthServer) Check(context.Context, *hpb.HealthCheckRequest) (*hpb.HealthCheckResponse, error) {
	for _, r := range []*trie.Replicator{s.replicator, s.assetReplicator, s.exeReplicator} {
		if r != nil {
			if err := r.Healthcheck(); err != nil {
				log.Error("Failed healthcheck: %s", err)
				return &hpb.HealthCheckResponse{Status: hpb.HealthCheckResponse_NOT_SERVING}, nil
			}
		}
	}
	log.Debug("Passed healthcheck, all ranges serving")
	return &hpb.HealthCheckResponse{Status: hpb.HealthCheckResponse_SERVING}, nil
}

func (s *server) GetCapabilities(ctx context.Context, req *pb.GetCapabilitiesRequest) (*pb.ServerCapabilities, error) {
	// This always does the same thing as Elan.
	// We might consider upping some of the size limits though since it will multiplex batch requests so will be more
	// efficient with bigger ones than Elan would be on its own.
	caps := &pb.ServerCapabilities{
		CacheCapabilities: &pb.CacheCapabilities{
			DigestFunctions: []pb.DigestFunction_Value{
				pb.DigestFunction_SHA1,
				pb.DigestFunction_SHA256,
			},
			ActionCacheUpdateCapabilities: &pb.ActionCacheUpdateCapabilities{
				UpdateEnabled: false,
			},
			CachePriorityCapabilities: &pb.PriorityCapabilities{
				Priorities: []*pb.PriorityCapabilities_PriorityRange{
					{
						MinPriority: 0,
						MaxPriority: 0,
					},
					{
						MinPriority: -1,
						MaxPriority: -1,
					},
				},
			},
			MaxBatchTotalSizeBytes: 4048000, // 4000 Kelly-Bootle standard units
			SupportedCompressors: []pb.Compressor_Value{
				pb.Compressor_IDENTITY,
				pb.Compressor_ZSTD,
			},
			SupportedBatchUpdateCompressors: []pb.Compressor_Value{
				pb.Compressor_IDENTITY,
				pb.Compressor_ZSTD,
			},
		},
		LowApiVersion:  &semver.SemVer{Major: 2, Minor: 0},
		HighApiVersion: &semver.SemVer{Major: 2, Minor: 12},
	}
	if s.exeReplicator != nil {
		caps.ExecutionCapabilities = &pb.ExecutionCapabilities{
			DigestFunction: pb.DigestFunction_SHA256,
			ExecEnabled:    true,
		}
	}
	return caps, nil
}

func (s *server) GetActionResult(ctx context.Context, req *pb.GetActionResultRequest) (ar *pb.ActionResult, err error) {
	err = s.replicator.SequentialDigest(req.ActionDigest, func(srv *trie.Server) error {
		ctx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()
		a, e := srv.AC.GetActionResult(ctx, req)
		if e == nil {
			ar = a
		}
		return e
	})
	return
}

func (s *server) UpdateActionResult(ctx context.Context, req *pb.UpdateActionResultRequest) (ar *pb.ActionResult, err error) {
	err = s.replicator.ParallelDigest(req.ActionDigest, func(srv *trie.Server) error {
		ctx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()
		a, e := srv.AC.UpdateActionResult(ctx, req)
		if e == nil {
			ar = a
		}
		return e
	})
	return
}

func (s *server) FindMissingBlobs(ctx context.Context, req *pb.FindMissingBlobsRequest) (*pb.FindMissingBlobsResponse, error) {
	// Note that the replication strategy here assumes that the set of blobs to go to each replica is the same as for
	// the primary (basically that the replication offset is an integer multiple of the size of each hash block, and those
	// blocks are of a consistent size). This currently fits our setup.
	blobs := map[*trie.Server][]*pb.Digest{}
	seen := map[string]struct{}{}
	for _, d := range req.BlobDigests {
		// Empty directories have empty hashes. We don't need to check for them.
		if d.Hash != "" && d.Hash != emptyHash {
			if _, present := seen[d.Hash]; !present {
				s := s.replicator.Trie.Get(d.Hash)
				blobs[s] = append(blobs[s], d)
				seen[d.Hash] = struct{}{}
			}
		}
	}
	resp := &pb.FindMissingBlobsResponse{}
	var g errgroup.Group
	var mutex sync.Mutex
	for srv, b := range blobs {
		g.Go(func() error {
			var mutex2 sync.Mutex
			found := make(map[string]struct{}, len(b))
			// This call is best-effort; if it fails we fail to populate found, and return more blobs
			// as missing. That should not be a problem for the callers who will then simply upload them.
			s.replicator.Parallel(srv.Start, func(srv *trie.Server) error {
				ctx, cancel := context.WithTimeout(ctx, s.timeout)
				defer cancel()
				r, err := srv.CAS.FindMissingBlobs(ctx, &pb.FindMissingBlobsRequest{
					InstanceName: req.InstanceName,
					BlobDigests:  b,
				})
				if err != nil {
					return err
				}
				missing := map[string]struct{}{}
				for _, dg := range r.MissingBlobDigests {
					missing[dg.Hash] = struct{}{}
				}
				mutex2.Lock()
				defer mutex2.Unlock()
				for _, dg := range b {
					if _, present := missing[dg.Hash]; !present {
						found[dg.Hash] = struct{}{}
					}
				}
				return nil
			})
			mutex.Lock()
			defer mutex.Unlock()
			for _, dg := range b {
				if _, present := found[dg.Hash]; !present {
					resp.MissingBlobDigests = append(resp.MissingBlobDigests, dg)
				}
			}
			return nil
		})
	}
	return resp, g.Wait()
}

func (s *server) BatchUpdateBlobs(ctx context.Context, req *pb.BatchUpdateBlobsRequest) (*pb.BatchUpdateBlobsResponse, error) {
	blobs := map[*trie.Server][]*pb.BatchUpdateBlobsRequest_Request{}
	for _, d := range req.Requests {
		s := s.replicator.Trie.Get(d.Digest.Hash)
		blobs[s] = append(blobs[s], d)
	}
	resp := &pb.BatchUpdateBlobsResponse{}
	var g errgroup.Group
	var mutex sync.Mutex
	m := make(map[string][]*pb.BatchUpdateBlobsResponse_Response, len(blobs)*s.replicator.Replicas)
	for srv, rs := range blobs {
		g.Go(func() error {
			return s.replicator.Parallel(srv.Start, func(srv *trie.Server) error {
				ctx, cancel := context.WithTimeout(ctx, s.timeout)
				defer cancel()
				r, err := srv.CAS.BatchUpdateBlobs(ctx, &pb.BatchUpdateBlobsRequest{
					InstanceName: req.InstanceName,
					Requests:     rs,
				})
				if err != nil {
					return err
				}
				mutex.Lock()
				defer mutex.Unlock()
				for _, resp := range r.Responses {
					m[resp.Digest.Hash] = append(m[resp.Digest.Hash], resp)
				}
				return nil
			})
		})
	}
	err := g.Wait()
	chooseResponse := func(v []*pb.BatchUpdateBlobsResponse_Response) *pb.BatchUpdateBlobsResponse_Response {
		for _, r := range v {
			if r.Status.Code == int32(codes.OK) {
				return r
			}
		}
		// Not ideal but just return the first one so it has a sensible code.
		log.Error("BatchUpdateBlobs failed to all replicas: %s", v)
		return v[0]
	}
	for _, v := range m {
		resp.Responses = append(resp.Responses, chooseResponse(v))
	}
	return resp, err
}

func (s *server) BatchReadBlobs(ctx context.Context, req *pb.BatchReadBlobsRequest) (*pb.BatchReadBlobsResponse, error) {
	blobs := map[*trie.Server][]*pb.Digest{}
	for _, d := range req.Digests {
		s := s.replicator.Trie.Get(d.Hash)
		blobs[s] = append(blobs[s], d)
	}
	resp := &pb.BatchReadBlobsResponse{}
	var g errgroup.Group
	var mutex sync.Mutex
	successes := map[string]struct{}{}
	failures := []*pb.BatchReadBlobsResponse_Response{}

	defer func() {
		if len(failures) > 0 {
			for _, fail := range failures {
				if _, present := successes[fail.Digest.Hash]; !present {
					resp.Responses = append(resp.Responses, fail)
				}
			}
		}
	}()

	for srv, d := range blobs {
		g.Go(func() error {
			return s.replicator.SequentialAck(srv.Start, func(s2 *trie.Server) (bool, error) {
				ctx, cancel := context.WithTimeout(ctx, s.timeout)
				defer cancel()
				r, err := s2.CAS.BatchReadBlobs(ctx, &pb.BatchReadBlobsRequest{
					InstanceName: req.InstanceName,
					Digests:      d,
				})
				if err != nil {
					return false, err
				}
				shouldContinue := false
				mutex.Lock()
				defer mutex.Unlock()
				for _, rr := range r.Responses {
					switch rr.Status.Code {
					case int32(codes.OK):
						if _, present := successes[rr.Digest.Hash]; !present {
							resp.Responses = append(resp.Responses, rr)
							successes[rr.Digest.Hash] = struct{}{}
						}
					case int32(codes.NotFound):
						shouldContinue = true
						fallthrough
					default:
						failures = append(failures, rr)
					}
				}
				return shouldContinue, nil
			})
		})
	}
	return resp, g.Wait()
}

func (s *server) GetTree(req *pb.GetTreeRequest, srv pb.ContentAddressableStorage_GetTreeServer) error {
	if req.PageSize > 0 {
		return status.Errorf(codes.Unimplemented, "page_size not implemented for GetTree")
	} else if req.PageToken != "" {
		return status.Errorf(codes.Unimplemented, "page tokens not implemented for GetTree")
	} else if req.RootDigest == nil {
		return status.Errorf(codes.InvalidArgument, "missing root_digest field")
	} else if req.RootDigest.SizeBytes == 0 && req.RootDigest.Hash == emptyHash {
		// Optimised case for empty inputs
		return srv.Send(&pb.GetTreeResponse{})
	}
	// The individual directories need to be split up too...
	var g errgroup.Group
	var mutex sync.Mutex
	ctx := srv.Context()
	stopAtPack := rexclient.ShouldStopAtPack(ctx)
	r := &pb.GetTreeResponse{}

	var fetchDir func(digest *pb.Digest) error
	fetchDir = func(digest *pb.Digest) error {
		var resp *pb.BatchReadBlobsResponse
		if err := s.replicator.SequentialDigest(digest, func(s2 *trie.Server) error {
			ctx, cancel := context.WithTimeout(ctx, s.timeout)
			defer cancel()
			r, err := s2.CAS.BatchReadBlobs(ctx, &pb.BatchReadBlobsRequest{
				InstanceName: req.InstanceName,
				Digests:      []*pb.Digest{digest},
			})
			if err != nil {
				return err
			} else if len(r.Responses) != 1 {
				return fmt.Errorf("missing blob in response") // shouldn't happen...
			} else if s := r.Responses[0].Status; s.Code != int32(codes.OK) {
				return status.Errorf(codes.Code(s.Code), "%s", s.Message)
			}
			resp = r
			return nil
		}); err != nil {
			return err
		}
		dir := &pb.Directory{}
		if err := proto.Unmarshal(resp.Responses[0].Data, dir); err != nil {
			return err
		}
		if !stopAtPack || rexclient.PackDigest(dir).Hash == "" {
			for _, dir := range dir.Directories {
				digest := dir.Digest
				g.Go(func() error {
					return fetchDir(digest)
				})
			}
		}
		mutex.Lock()
		defer mutex.Unlock()
		r.Directories = append(r.Directories, dir)
		return nil
	}

	g.Go(func() error {
		return fetchDir(req.RootDigest)
	})
	if err := g.Wait(); err != nil {
		return err
	}
	return srv.Send(r)
}

func (s *server) SpliceBlob(ctx context.Context, req *pb.SpliceBlobRequest) (*pb.SpliceBlobResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *server) SplitBlob(ctx context.Context, req *pb.SplitBlobRequest) (*pb.SplitBlobResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *server) Read(req *bs.ReadRequest, srv bs.ByteStream_ReadServer) error {
	hash, err := s.bytestreamBlobName(req.ResourceName)
	if err != nil {
		return err
	}
	// This needs a little bookkeeping since we can fail partway and restart on another server.
	// That's actually OK as long as we jiggle the ReadOffset appropriately.
	return s.replicator.Sequential(hash, func(s2 *trie.Server) error {
		ctx, update := withRollingTimeout(srv.Context(), s.timeout)
		client, err := s2.BS.Read(ctx, req)
		if err != nil {
			return err
		}
		for {
			update()
			resp, err := client.Recv()
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			} else if err := srv.Send(resp); err != nil {
				return err
			}
			req.ReadOffset += int64(len(resp.Data))
		}
	})
}

func (s *server) Write(srv bs.ByteStream_WriteServer) error {
	// This is a bit different to (and rather more complex than) Read since we have to perform all writes in parallel,
	// but we only receive from the client once so we have to fan out the messages.
	req, err := srv.Recv()
	if err != nil {
		return err
	}
	hash, err := s.bytestreamBlobName(req.ResourceName)
	if err != nil {
		return err
	}
	chs := make([]chan *bs.WriteRequest, s.replicator.Replicas)
	chch := make(chan chan *bs.WriteRequest, s.replicator.Replicas)
	for i := range chs {
		ch := make(chan *bs.WriteRequest, 10) // Bit of arbitrary buffering so they can get a little out of sync if needed.
		ch <- req
		chs[i] = ch
		chch <- ch
	}
	var g errgroup.Group
	g.Go(func() error {
		for {
			req, err := srv.Recv()
			if err != nil {
				for _, ch := range chs {
					close(ch)
				}
				if err == io.EOF {
					return nil
				}
				return err
			}
			for _, ch := range chs {
				ch <- req
			}
		}
	})
	var resp *bs.WriteResponse
	if err := s.replicator.Parallel(hash, func(s2 *trie.Server) error {
		ctx, update := withRollingTimeout(srv.Context(), s.timeout)
		ch := <-chch
		defer func() {
			for range ch {
			}
		}() // Ensure we exhaust this channel if anything goes wrong
		client, err := s2.BS.Write(ctx)
		if err != nil {
			return err
		}
		for req := range ch {
			update()
			if err := client.Send(req); err != nil {
				return err
			}
		}
		r, err := client.CloseAndRecv()
		if err != nil {
			return err
		}
		resp = r
		return nil
	}); err != nil {
		return err
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return srv.SendAndClose(resp)
}

func (s *server) QueryWriteStatus(ctx context.Context, req *bs.QueryWriteStatusRequest) (resp *bs.QueryWriteStatusResponse, err error) {
	hash, err := s.bytestreamBlobName(req.ResourceName)
	if err != nil {
		return nil, err
	}
	err = s.replicator.Sequential(hash, func(srv *trie.Server) error {
		ctx, cancel := context.WithTimeout(ctx, s.timeout)
		defer cancel()
		r, e := srv.BS.QueryWriteStatus(ctx, req)
		if e == nil {
			resp = r
		}
		return e
	})
	return resp, err
}

// bytestreamBlobName returns the hash corresponding to a bytestream resource name.
func (s *server) bytestreamBlobName(bytestream string) (string, error) {
	matches := s.bytestreamRe.FindStringSubmatch(bytestream)
	if matches == nil {
		return "", status.Errorf(codes.InvalidArgument, "invalid ResourceName: %s", bytestream)
	}
	return matches[2], nil
}

func (s *server) FetchDirectory(ctx context.Context, req *apb.FetchDirectoryRequest) (resp *apb.FetchDirectoryResponse, err error) {
	err = s.assetReplicator.Sequential(s.assetHash(req.Qualifiers), func(s *trie.Server) error {
		resp, err = s.Fetch.FetchDirectory(ctx, req)
		return err
	})
	return resp, err
}

func (s *server) FetchBlob(ctx context.Context, req *apb.FetchBlobRequest) (resp *apb.FetchBlobResponse, err error) {
	err = s.assetReplicator.Sequential(s.assetHash(req.Qualifiers), func(srv *trie.Server) error {
		resp, err = srv.Fetch.FetchBlob(ctx, req)
		return err
	})
	return resp, err
}

// assetHash returns a hash used to key requests to the asset service.
// It attempts to work it out from any subresource integrity qualifiers in order to put the fetch closer to the
// CAS server (assuming their geometries match...) but on failure just picks something random.
func (s *server) assetHash(quals []*apb.Qualifier) string {
	for _, q := range quals {
		if q.Name == "checksum.sri" {
			if c, err := sri.NewChecker(q.Value); err == nil {
				if hashes := c.Expected("sha256"); len(hashes) > 0 {
					// Need to convert from base64 to hex...
					if b, err := base64.StdEncoding.DecodeString(hashes[0]); err == nil {
						return hex.EncodeToString(b)
					}
				}
			}
		}
	}
	// Didn't find anything above, for whatever reason. Make up something random.
	key := [32]byte{}
	rand.Read(key[:])
	return hex.EncodeToString(key[:])
}

func (s *server) Execute(req *pb.ExecuteRequest, stream pb.Execution_ExecuteServer) error {
	return s.exeReplicator.SequentialDigest(req.ActionDigest, func(srv *trie.Server) error {
		client, err := srv.Exe.Execute(s.forwardMetadata(stream.Context()), req)
		if err != nil {
			return err
		}
		return s.streamExecution(client, stream)
	})
}

func (s *server) WaitExecution(req *pb.WaitExecutionRequest, stream pb.Execution_WaitExecutionServer) error {
	return s.exeReplicator.Sequential(req.Name, func(srv *trie.Server) error {
		client, err := srv.Exe.WaitExecution(s.forwardMetadata(stream.Context()), req)
		if err != nil {
			return err
		}
		return s.streamExecution(client, stream)
	})
}

func (s *server) streamExecution(client pb.Execution_ExecuteClient, server pb.Execution_ExecuteServer) error {
	for {
		if resp, err := client.Recv(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		} else if err := server.Send(resp); err != nil {
			return err
		}
	}
}

// forwardMetadata forwards the standard rex proto metadata.
func (s *server) forwardMetadata(ctx context.Context) context.Context {
	const key = "build.bazel.remote.execution.v2.requestmetadata-bin" // as defined by the proto
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get(key); len(v) != 0 {
			return metadata.AppendToOutgoingContext(ctx, key, v[0])
		}
	}
	return ctx
}

func (s *server) List(ctx context.Context, req *ppb.ListRequest) (*ppb.ListResponse, error) {
	if len(req.Prefix) != 2 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid prefix provided: %s", req.Prefix)
	}
	var mutex sync.Mutex
	resp := &ppb.ListResponse{}
	ars := map[string]*ppb.ActionResult{}
	blobs := map[string]*ppb.Blob{}
	err := s.replicator.Parallel(req.Prefix, func(srv *trie.Server) error {
		ctx, cancel := context.WithTimeout(ctx, 3*s.timeout) // Multiply up timeout since this operation can be expensive.
		defer cancel()
		r, err := srv.GC.List(ctx, req)
		if err != nil {
			return err
		}
		mutex.Lock()
		defer mutex.Unlock()
		for _, ar := range r.ActionResults {
			if strings.HasPrefix(ar.Hash, "tmp") {
				continue
			}
			if ar.Replicas == 0 {
				ar.Replicas = 1
			}
			if existing, present := ars[ar.Hash]; !present {
				resp.ActionResults = append(resp.ActionResults, ar)
				ars[ar.Hash] = ar
			} else {
				// Temporary logging for debug purposes: if one replica is older than 72 hrs and one younger, make a note
				minAge := 72 * time.Hour
				ageThreshold := time.Now().Add(-minAge).Unix()
				if (existing.LastAccessed < ageThreshold && ar.LastAccessed >= ageThreshold) || (ar.LastAccessed < ageThreshold && existing.LastAccessed >= ageThreshold) {
					existingLastAccessed := time.Unix(existing.LastAccessed, 0)
					arLastAccessed := time.Unix(ar.LastAccessed, 0)
					logr.WithFields(logrus.Fields{
						"hash":                 ar.Hash,
						"existingLastAccessed": existingLastAccessed,
						"lastAccessed":         arLastAccessed,
					}).Debug("AR: replicas accessed at existingLastAccessed and lastAccessed")
				}
				// End temporary logging
				if existing.LastAccessed < ar.LastAccessed {
					existing.LastAccessed = ar.LastAccessed
				}
				existing.Replicas += ar.Replicas
			}
		}
		for _, blob := range r.Blobs {
			if strings.HasPrefix(blob.Hash, "tmp") {
				continue
			}
			if blob.Replicas == 0 {
				blob.Replicas = 1
			}
			if existing, present := blobs[blob.Hash]; !present {
				resp.Blobs = append(resp.Blobs, blob)
				blobs[blob.Hash] = blob
			} else {
				existing.Replicas += blob.Replicas
			}
		}
		return nil
	})
	return resp, err
}

func (s *server) Delete(ctx context.Context, req *ppb.DeleteRequest) (*ppb.DeleteResponse, error) {
	if len(req.Prefix) != 2 {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid prefix provided: %s", req.Prefix)
	}
	return &ppb.DeleteResponse{}, s.replicator.All(req.Prefix, func(srv *trie.Server) error {
		ctx, cancel := context.WithTimeout(ctx, 10*s.timeout) // Multiply up timeout since this operation can be expensive.
		defer cancel()
		_, err := srv.GC.Delete(ctx, req)
		return err
	})
}

// withRollingTimeout implements a timeout between each step of a streamed RPC.
// Essentially we want to fail if it takes > 5s (for example) to get a chunk, but we don't
// want to put an upper limit on the total time the RPC can run for (it might be long
// if it's a big file and things are moving kinda slowly).
// It returns a new context which will be cancelled when the rolling timeout expired and
// a callback to update its timeout.
func withRollingTimeout(ctx context.Context, timeout time.Duration) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	ch := make(chan struct{})
	go func() {
		for {
			select {
			case <-ch:
			case <-time.After(timeout):
				cancel()
				return
			}
		}
	}()
	return ctx, func() {
		ch <- struct{}{}
	}
}
