package rpc

import (
	"fmt"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/require"
)

// testLargeBlobSize is the size at or above which blobs are large; see blobKind.
const testLargeBlobSize = 100

// blobKind identifies a group of blobs by where they are held and whether they are small (below
// largeBlobSize, so can be indexed in Redis) or large (at or above it, so are not).
type blobKind string

const (
	smallInRedis            blobKind = "smallInRedis"
	smallInBucket           blobKind = "smallInBucket"
	smallInBucketCompressed blobKind = "smallInBucketCompressed"
	smallMissing            blobKind = "smallMissing"
	largeInRedis            blobKind = "largeInRedis"
	largeInBucket           blobKind = "largeInBucket"
	largeMissing            blobKind = "largeMissing"
)

// blobCounts is the number of blobs to create of each kind. Blobs are requested in the order of the fields.
type blobCounts struct {
	smallInRedis            int
	smallInBucket           int
	smallInBucketCompressed int
	smallMissing            int
	largeInRedis            int
	largeInBucket           int
	largeMissing            int
}

func blobData(kind blobKind, i int) []byte {
	data := fmt.Appendf(nil, "%s blob %d", kind, i)
	if strings.HasPrefix(string(kind), "large") {
		data = append(data, make([]byte, testLargeBlobSize)...)
	}
	return data
}

// makeExpectedDigests returns the digests of the first count blobs of the given kind, as created by place.
func makeExpectedDigests(kind blobKind, count int) []*pb.Digest {
	var digests []*pb.Digest
	for i := range count {
		digests = append(digests, digest.NewFromBlob(blobData(kind, i)).ToProto())
	}
	return digests
}

// place creates the blobs in Redis and the server's bucket as appropriate, and returns their digests in request order.
func (c blobCounts) place(t *testing.T, s *server, mr *miniredis.Miniredis) []*pb.Digest {
	var digests []*pb.Digest
	for _, g := range []struct {
		kind       blobKind
		count      int
		inRedis    bool
		inBucket   bool
		compressed bool
	}{
		{kind: smallInRedis, count: c.smallInRedis, inRedis: true},
		{kind: smallInBucket, count: c.smallInBucket, inBucket: true},
		{kind: smallInBucketCompressed, count: c.smallInBucketCompressed, inBucket: true, compressed: true},
		{kind: smallMissing, count: c.smallMissing},
		{kind: largeInRedis, count: c.largeInRedis, inRedis: true},
		{kind: largeInBucket, count: c.largeInBucket, inBucket: true},
		{kind: largeMissing, count: c.largeMissing},
	} {
		for i := range g.count {
			data := blobData(g.kind, i)
			d := digest.NewFromBlob(data).ToProto()
			if g.inRedis {
				require.NoError(t, mr.Set(d.Hash, string(data)))
			}
			if g.inBucket && g.compressed {
				require.NoError(t, s.bucket.WriteAll(t.Context(), s.key("zstd_cas", d), s.compressor.EncodeAll(data, nil)))
			} else if g.inBucket {
				require.NoError(t, s.bucket.WriteAll(t.Context(), s.key("cas", d), data))
			}
			digests = append(digests, d)
		}
	}
	return digests
}

// newRedisServer returns a server using a fresh miniredis, which is closed first if redisFails is set.
// Any hooks are added to the Redis client.
func newRedisServer(t *testing.T, redisFails bool, hooks ...redis.Hook) (*server, *miniredis.Miniredis) {
	mr := miniredis.RunT(t)
	rc := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	for _, h := range hooks {
		rc.AddHook(h)
	}
	s := createServer(t.Context(), "mem://", 5, 1000, 10_000_000, rc, testLargeBlobSize)
	if redisFails {
		mr.Close()
	}
	return s, mr
}
