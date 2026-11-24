package rpc

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// expectedBRBResponse is what BatchReadBlobs should return for one requested digest.
type expectedBRBResponse struct {
	digest     *pb.Digest
	code       codes.Code
	compressor pb.Compressor_Value
	data       []byte
}

// makeExpectedBRBResponses returns the expected responses for the first count blobs of the given kind, as created
// by place. The data is the blob's contents, zstd-compressed if compressor is ZSTD, and empty if code is not OK.
func makeExpectedBRBResponses(kind blobKind, count int, code codes.Code, compressor pb.Compressor_Value) []expectedBRBResponse {
	var responses []expectedBRBResponse
	for i, d := range makeExpectedDigests(kind, count) {
		r := expectedBRBResponse{digest: d, code: code, compressor: compressor}
		if code == codes.OK {
			r.data = blobData(kind, i)
			if compressor == pb.Compressor_ZSTD {
				r.data = defaultEncoder.EncodeAll(r.data, nil)
			}
		}
		responses = append(responses, r)
	}
	return responses
}

// assertBRBResponses checks that the responses are exactly the expected ones, in order.
func assertBRBResponses(t *testing.T, expected []expectedBRBResponse, resp *pb.BatchReadBlobsResponse) {
	require.Len(t, resp.Responses, len(expected))
	for i, r := range resp.Responses {
		assert.Equal(t, expected[i].digest.Hash, r.Digest.Hash, "response %d", i)
		assert.Equal(t, expected[i].code, codes.Code(r.Status.Code), "response %d", i)
		assert.Equal(t, expected[i].compressor, r.Compressor, "response %d", i)
		assert.Equal(t, expected[i].data, r.Data, "response %d", i)
	}
}

// countingHook counts the Redis calls made.
type countingHook struct {
	singles, pipelines atomic.Int64
}

func (h *countingHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	h.singles.Add(1)
	return ctx, nil
}

func (h *countingHook) AfterProcess(context.Context, redis.Cmder) error { return nil }

func (h *countingHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	h.pipelines.Add(1)
	return ctx, nil
}

func (h *countingHook) AfterProcessPipeline(context.Context, []redis.Cmder) error { return nil }

// TestBatchReadBlobs checks the contents, status and compression of each response for blobs of many
// different kinds requested together, response ordering, the handling of empty, duplicate and invalid
// digests, and that small blobs are looked up in Redis with pipelined calls (rather than one call per
// blob) falling back to the bucket if Redis fails. Large blobs are never looked up in Redis.
func TestBatchReadBlobs(t *testing.T) {
	// everyKind has several blobs of each kind, so that a single request mixes them all.
	everyKind := blobCounts{
		smallInRedis:            3,
		smallInBucket:           3,
		smallInBucketCompressed: 3,
		smallMissing:            2,
		largeInRedis:            2,
		largeInBucket:           2,
		largeMissing:            2,
	}
	for _, tc := range []struct {
		name string

		// Setup
		blobs blobCounts
		// redisFails makes every Redis call fail, so small blobs must come from the bucket.
		redisFails bool

		// Request: the digests of the blobs are requested first, then extraDigests.
		extraDigests []*pb.Digest
		acceptZstd   bool

		expected          []expectedBRBResponse
		expectedPipelines int64
	}{
		{
			name: "no digests gives no responses",
		},
		{
			name:         "empty blob is served without data",
			extraDigests: []*pb.Digest{digest.Empty.ToProto()},
			expected: []expectedBRBResponse{
				{digest: digest.Empty.ToProto(), code: codes.OK, compressor: pb.Compressor_IDENTITY},
			},
		},
		{
			name: "duplicate digests each get a response",
			blobs: blobCounts{
				smallInBucket: 1,
			},
			extraDigests:      slices.Repeat(makeExpectedDigests(smallInBucket, 1), 2),
			expected:          slices.Repeat(makeExpectedBRBResponses(smallInBucket, 1, codes.OK, pb.Compressor_IDENTITY), 3),
			expectedPipelines: 1,
		},
		{
			name:  "blobs of every kind are served uncompressed if zstd is not acceptable",
			blobs: everyKind,
			expected: slices.Concat(
				makeExpectedBRBResponses(smallInRedis, 3, codes.OK, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(smallInBucket, 3, codes.OK, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(smallInBucketCompressed, 3, codes.OK, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(smallMissing, 2, codes.NotFound, pb.Compressor_IDENTITY),
				// Redis is not consulted for large blobs.
				makeExpectedBRBResponses(largeInRedis, 2, codes.NotFound, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(largeInBucket, 2, codes.OK, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(largeMissing, 2, codes.NotFound, pb.Compressor_IDENTITY),
			),
			expectedPipelines: 1,
		},
		{
			name:       "only compressed bucket blobs are served compressed if zstd is acceptable",
			blobs:      everyKind,
			acceptZstd: true,
			expected: slices.Concat(
				// Redis hits are always uncompressed.
				makeExpectedBRBResponses(smallInRedis, 3, codes.OK, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(smallInBucket, 3, codes.OK, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(smallInBucketCompressed, 3, codes.OK, pb.Compressor_ZSTD),
				makeExpectedBRBResponses(smallMissing, 2, codes.NotFound, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(largeInRedis, 2, codes.NotFound, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(largeInBucket, 2, codes.OK, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(largeMissing, 2, codes.NotFound, pb.Compressor_IDENTITY),
			),
			expectedPipelines: 1,
		},
		{
			name: "exactly one full pipeline",
			blobs: blobCounts{
				smallInBucket: 256,
			},
			expected:          makeExpectedBRBResponses(smallInBucket, 256, codes.OK, pb.Compressor_IDENTITY),
			expectedPipelines: 1,
		},
		{
			name: "one more than a full pipeline uses two",
			blobs: blobCounts{
				smallInBucket: 257,
			},
			expected:          makeExpectedBRBResponses(smallInBucket, 257, codes.OK, pb.Compressor_IDENTITY),
			expectedPipelines: 2,
		},
		{
			name: "many blobs spanning several pipelines",
			blobs: blobCounts{
				smallInRedis:  300,
				smallInBucket: 300,
				smallMissing:  10,
			},
			expected: slices.Concat(
				makeExpectedBRBResponses(smallInRedis, 300, codes.OK, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(smallInBucket, 300, codes.OK, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(smallMissing, 10, codes.NotFound, pb.Compressor_IDENTITY),
			),
			expectedPipelines: 3,
		},
		{
			name: "only large blobs are not looked up in Redis",
			blobs: blobCounts{
				largeInRedis:  5,
				largeInBucket: 5,
				largeMissing:  5,
			},
			expected: slices.Concat(
				makeExpectedBRBResponses(largeInRedis, 5, codes.NotFound, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(largeInBucket, 5, codes.OK, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(largeMissing, 5, codes.NotFound, pb.Compressor_IDENTITY),
			),
		},
		{
			name: "too short a hash is reported as an error for that blob only",
			blobs: blobCounts{
				smallInRedis:  2,
				smallInBucket: 2,
			},
			extraDigests: []*pb.Digest{{Hash: "a", SizeBytes: 1}},
			expected: slices.Concat(
				makeExpectedBRBResponses(smallInRedis, 2, codes.OK, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(smallInBucket, 2, codes.OK, pb.Compressor_IDENTITY),
				[]expectedBRBResponse{{digest: &pb.Digest{Hash: "a", SizeBytes: 1}, code: codes.Unknown, compressor: pb.Compressor_IDENTITY}},
			),
			expectedPipelines: 1,
		},
		{
			name: "failed pipeline falls back to the bucket",
			blobs: blobCounts{
				smallInRedis:  3,
				smallInBucket: 300,
			},
			redisFails: true,
			expected: slices.Concat(
				// Blobs only in Redis can't be read when Redis fails.
				makeExpectedBRBResponses(smallInRedis, 3, codes.NotFound, pb.Compressor_IDENTITY),
				makeExpectedBRBResponses(smallInBucket, 300, codes.OK, pb.Compressor_IDENTITY),
			),
			expectedPipelines: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook := &countingHook{}
			s, mr := newRedisServer(t, tc.redisFails, hook)
			req := &pb.BatchReadBlobsRequest{Digests: append(tc.blobs.place(t, s, mr), tc.extraDigests...)}
			if tc.acceptZstd {
				req.AcceptableCompressors = []pb.Compressor_Value{pb.Compressor_ZSTD}
			}

			resp, err := s.BatchReadBlobs(t.Context(), req)
			require.NoError(t, err)
			assertBRBResponses(t, tc.expected, resp)
			assert.Zero(t, hook.singles.Load(), "no per-blob Redis calls expected")
			assert.Equal(t, tc.expectedPipelines, hook.pipelines.Load())
		})
	}
}
