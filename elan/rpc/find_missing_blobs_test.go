package rpc

import (
	"slices"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errInvalidHash = status.Errorf(codes.InvalidArgument, "Invalid hash '%s'", "abc")

// Large counts of blobs are used here to exercise the redis pipelining behaviour.
func TestFindMissingBlobs(t *testing.T) {
	for _, tc := range []struct {
		name string

		blobs        blobCounts
		redisFails   bool
		extraDigests []*pb.Digest

		expectedMissing []*pb.Digest
		expectedErr     error
	}{
		{
			name: "no digests",
		},
		{
			name: "small blob only in Redis is present",
			blobs: blobCounts{
				smallInRedis: 3,
			},
		},
		{
			name: "small blob in bucket is present",
			blobs: blobCounts{
				smallInBucket: 3,
			},
		},
		{
			name: "small blob in compressed bucket is present",
			blobs: blobCounts{
				smallInBucketCompressed: 3,
			},
		},
		{
			name: "small blob nowhere is missing",
			blobs: blobCounts{
				smallMissing: 3,
			},
			expectedMissing: makeExpectedDigests(smallMissing, 3),
		},
		{
			name: "large blob in bucket is present",
			blobs: blobCounts{
				largeInBucket: 3,
			},
		},
		{
			// This isn't a particularly important test given large blobs should never be written
			// to redis, but it's included here for completeness.
			name: "large blob only in Redis is missing because Redis is not consulted",
			blobs: blobCounts{
				largeInRedis: 3,
			},
			expectedMissing: makeExpectedDigests(largeInRedis, 3),
		},
		{
			name: "large blob nowhere is missing",
			blobs: blobCounts{
				largeMissing: 3,
			},
			expectedMissing: makeExpectedDigests(largeMissing, 3),
		},
		{
			name: "mixed blobs spanning several pipelines",
			blobs: blobCounts{
				smallInRedis:            300,
				smallInBucket:           300,
				smallInBucketCompressed: 50,
				smallMissing:            300,
				largeInRedis:            2,
				largeInBucket:           2,
				largeMissing:            2,
			},
			expectedMissing: slices.Concat(
				makeExpectedDigests(smallMissing, 300),
				makeExpectedDigests(largeInRedis, 2),
				makeExpectedDigests(largeMissing, 2),
			),
		},
		{
			name: "failing Redis falls back to the bucket",
			blobs: blobCounts{
				smallInRedis:            3,
				smallInBucket:           300,
				smallInBucketCompressed: 3,
				smallMissing:            3,
				largeInBucket:           2,
			},
			redisFails: true,
			expectedMissing: slices.Concat(
				makeExpectedDigests(smallInRedis, 3),
				makeExpectedDigests(smallMissing, 3),
			),
		},
		{
			name:         "empty blob is never missing",
			extraDigests: []*pb.Digest{digest.Empty.ToProto()},
		},
		{
			name: "invalid hash is rejected",
			blobs: blobCounts{
				smallMissing: 1,
			},
			extraDigests: []*pb.Digest{{Hash: "abc", SizeBytes: 3}},
			expectedErr:  errInvalidHash,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, mr := newRedisServer(t, tc.redisFails)

			resp, err := s.FindMissingBlobs(t.Context(), &pb.FindMissingBlobsRequest{
				BlobDigests: append(tc.blobs.place(t, s, mr), tc.extraDigests...),
			})
			if tc.expectedErr != nil {
				assert.ErrorIs(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			// The order of the missing digests is not part of the behaviour.
			assert.ElementsMatch(t, tc.expectedMissing, resp.MissingBlobDigests)
		})
	}
}

func TestNotInRedis(t *testing.T) {
	for _, tc := range []struct {
		name string

		blobs      blobCounts
		redisFails bool
		noRedis    bool

		expectedReturned []*pb.Digest
		expectedKnown    []*pb.Digest
	}{
		{
			name: "no digests",
		},
		{
			name: "all small blobs in Redis",
			blobs: blobCounts{
				smallInRedis: 3,
			},
			expectedKnown: makeExpectedDigests(smallInRedis, 3),
		},
		{
			name: "small blobs not in Redis are returned",
			blobs: blobCounts{
				smallInBucket: 3,
				smallMissing:  3,
			},
			expectedReturned: slices.Concat(
				makeExpectedDigests(smallInBucket, 3),
				makeExpectedDigests(smallMissing, 3),
			),
		},
		{
			name: "large blobs are returned even if in Redis",
			blobs: blobCounts{
				largeInRedis:  3,
				largeInBucket: 3,
				largeMissing:  3,
			},
			expectedReturned: slices.Concat(
				makeExpectedDigests(largeInRedis, 3),
				makeExpectedDigests(largeInBucket, 3),
				makeExpectedDigests(largeMissing, 3),
			),
		},
		{
			name: "mixed blobs spanning several pipelines",
			blobs: blobCounts{
				smallInRedis:  300,
				smallInBucket: 300,
				smallMissing:  300,
				largeInRedis:  2,
				largeMissing:  2,
			},
			expectedReturned: slices.Concat(
				makeExpectedDigests(smallInBucket, 300),
				makeExpectedDigests(smallMissing, 300),
				makeExpectedDigests(largeInRedis, 2),
				makeExpectedDigests(largeMissing, 2),
			),
			expectedKnown: makeExpectedDigests(smallInRedis, 300),
		},
		{
			name: "failing Redis returns everything",
			blobs: blobCounts{
				smallInRedis:  3,
				smallInBucket: 300,
				largeInRedis:  2,
			},
			redisFails: true,
			expectedReturned: slices.Concat(
				makeExpectedDigests(smallInRedis, 3),
				makeExpectedDigests(smallInBucket, 300),
				makeExpectedDigests(largeInRedis, 2),
			),
		},
		{
			name: "no Redis returns everything",
			blobs: blobCounts{
				smallInRedis:  3,
				smallInBucket: 3,
			},
			noRedis: true,
			expectedReturned: slices.Concat(
				makeExpectedDigests(smallInRedis, 3),
				makeExpectedDigests(smallInBucket, 3),
			),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, mr := newRedisServer(t, tc.redisFails)
			if tc.noRedis {
				s.readRedis = nil
			}

			actual := s.notInRedis(t.Context(), tc.blobs.place(t, s, mr))
			s.knownBlobCache.Wait() // The cache applies writes asynchronously.

			// The order of the returned digests is not part of the behaviour.
			assert.ElementsMatch(t, tc.expectedReturned, actual)
			for _, d := range tc.expectedKnown {
				assert.True(t, s.knownBlob(s.compressedKey("cas", d, false)), "expected known blob %s", d.Hash)
			}
			for _, d := range tc.expectedReturned {
				assert.False(t, s.knownBlob(s.compressedKey("cas", d, false)), "unexpected known blob %s", d.Hash)
			}
		})
	}
}
