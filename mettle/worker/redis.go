package worker

import (
	"context"
	"os"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"

	elan "github.com/thought-machine/please-servers/elan/rpc"
	rediscommon "github.com/thought-machine/please-servers/redis"
)

var redisHits = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "redis_hits_total",
})
var redisMisses = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "redis_misses_total",
})
var redisBytesRead = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "mettle",
	Name:      "redis_bytes_read_total",
})
var redisLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "mettle",
	Name:      "redis_latency_ms",
	Buckets:   []float64{20, 50, 100, 200, 500, 1000, 2000, 5000, 10000},
}, []string{"command"})

// newRedisClient augments an existing elan.Client with a Redis connection.
// All usage of Redis is best-effort only.
// If readRedis is set, all reads will happen on this client. If not, everything
// will go to the primary client.
func newRedisClient(client elan.Client, primaryRedis, readRedis *redis.Client, maxSize int64) elan.Client {
	// This is a safeguard in case the caller does not pass readRedis.
	if readRedis == nil {
		readRedis = primaryRedis
	}
	if maxSize <= 0 {
		maxSize = rediscommon.DefaultMaxSize
	}
	return &elanRedisWrapper{
		elan:      client,
		redis:     &monitoredRedisClient{primaryRedis},
		readRedis: &monitoredRedisClient{readRedis},
		maxSize:   maxSize,
	}
}

type elanRedisWrapper struct {
	elan      elan.Client
	redis     redisClient
	readRedis redisClient
	maxSize   int64
}

func (r *elanRedisWrapper) Healthcheck(ctx context.Context) error {
	return r.elan.Healthcheck(ctx)
}

func (r *elanRedisWrapper) ReadBlob(ctx context.Context, dg *pb.Digest) ([]byte, error) {
	if dg.SizeBytes < r.maxSize {
		cmd := r.readRedis.Get(ctx, dg.Hash)
		if err := cmd.Err(); err == nil {
			blob, _ := cmd.Bytes()
			return blob, nil
		} else if err != redis.Nil {
			log.Warningf("Failed to read blob from Redis: %s", err)
		}
	}
	// If we get here, the blob didn't exist. Download it then write back to Redis.
	blob, err := r.elan.ReadBlob(ctx, dg)
	if err != nil {
		return nil, err
	}
	if dg.SizeBytes < r.maxSize {
		// Write the blob to redis in the background, with a detached context to ensure the write
		// doesn't get terminated when ctx is cancelled upon request completion.
		go func(hash string) {
			if cmd := r.redis.Set(context.WithoutCancel(ctx), hash, blob, 0); cmd.Val() != "OK" {
				log.Warning("Failed to set blob in Redis: %s", cmd.Err())
			}
		}(dg.Hash)
	}
	return blob, nil
}

func (r *elanRedisWrapper) WriteBlob(ctx context.Context, blob []byte) (*pb.Digest, error) {
	return r.elan.WriteBlob(ctx, blob)
}

func (r *elanRedisWrapper) UpdateActionResult(ctx context.Context, ar *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	// We don't currently do anything with this, because we never try to read action results from
	// Redis.
	return r.elan.UpdateActionResult(ctx, ar)
}

func (r *elanRedisWrapper) UploadIfMissing(ctx context.Context, entries []*uploadinfo.Entry, compressors []pb.Compressor_Value) error {
	// All we use Redis for is to filter down the missing set.
	// This is approximate only and assumes that Redis always has a strict subset of the total keys.
	missing := make([]*uploadinfo.Entry, 0, len(entries))
	uploads := make([]interface{}, 0, 2*len(entries))
	keys := make([]string, 0, len(entries))
	entriesByHash := make(map[string]*uploadinfo.Entry, len(entries))
	// Unfortunately there is no MEXISTS, so we reuse MGET but chuck away the values.
	// We could also do lots of EXISTS in parallel but this seems simpler...
	for _, entry := range entries {
		if entry.Digest.Size < r.maxSize {
			keys = append(keys, entry.Digest.Hash)
		} else {
			missing = append(missing, entry)
		}
		entriesByHash[entry.Digest.Hash] = entry
	}
	blobs := r.readBlobs(ctx, keys, false)
	if blobs == nil {
		return r.elan.UploadIfMissing(ctx, entries, compressors)
	}
	for i, blob := range blobs {
		if blob == nil {
			e := entriesByHash[keys[i]]
			missing = append(missing, e)
			if dg := e.Digest; dg.Size < r.maxSize && dg.Hash != emptyHash {
				if e.Contents != nil {
					uploads = append(uploads, e.Digest.Hash, e.Contents)
				} else {
					b, err := os.ReadFile(e.Path)
					if err != nil {
						log.Warning("Failed to read file %s: %s", e.Path, err)
						continue
					}
					uploads = append(uploads, e.Digest.Hash, b)
				}
			}
		}
	}
	numPresent := len(entries) - len(missing)
	log.Debug("UploadIfMissing: %d / %d in Redis", numPresent, len(entries))
	if len(missing) == 0 {
		return nil // Redis had everything
	}
	if err := r.elan.UploadIfMissing(ctx, missing, compressors); err != nil {
		return err
	}
	if len(uploads) == 0 {
		return nil
	}
	// Only upload Redis after we have successfully uploaded blobs to Elan, otherwise we could
	// create an inconsistent situation.
	go r.writeBlobs(ctx, uploads)
	return nil // We never propagate Redis errors regardless.
}

func (r *elanRedisWrapper) BatchDownload(ctx context.Context, dgs []digest.Digest) (map[digest.Digest][]byte, error) {
	log.Debug("Checking Redis for batch of %d files...", len(dgs))
	keys := make([]string, 0, len(dgs))
	dgsByHash := make(map[string]digest.Digest, len(dgs))
	missingDigests := make([]digest.Digest, 0, len(dgs))
	for _, dg := range dgs {
		if dg.Size < r.maxSize {
			keys = append(keys, dg.Hash)
		} else {
			missingDigests = append(missingDigests, dg)
		}
		dgsByHash[dg.Hash] = dg
	}
	blobs := r.readBlobs(ctx, keys, true)
	if blobs == nil {
		return r.elan.BatchDownload(ctx, dgs)
	}
	ret := make(map[digest.Digest][]byte, len(dgs))
	for i, blob := range blobs {
		if blob == nil {
			missingDigests = append(missingDigests, dgsByHash[keys[i]])
		} else {
			ret[dgsByHash[keys[i]]] = blob
		}
	}
	if len(missingDigests) == 0 {
		return ret, nil
	}
	log.Debug("Found %d / %d files in Redis", len(dgs)-len(missingDigests), len(dgs))
	m, err := r.elan.BatchDownload(ctx, missingDigests)
	if err != nil {
		return nil, err
	}
	uploads := make([]interface{}, 0, 2*len(m))
	for k, v := range m {
		ret[k] = v
		if k.Size < r.maxSize {
			uploads = append(uploads, k.Hash, v)
		}
	}
	go r.writeBlobs(ctx, uploads)
	return ret, nil
}

// readBlobs reads a set of blobs from Redis. It returns nil on any failure.
// On success some blobs may not have been available in Redis, in which case they'll be nil.
func (r *elanRedisWrapper) readBlobs(ctx context.Context, keys []string, metrics bool) [][]byte {
	if len(keys) == 0 {
		return nil
	}
	resp, err := r.readRedis.MGet(ctx, keys...).Result()
	if err != nil {
		log.Warning("Failed to check blobs in Redis: %s", err)
		return nil
	} else if len(resp) != len(keys) {
		log.Warning("Length mismatch in Redis response; got %d, expected %d", len(resp), len(keys))
		// If the lengths don't match we don't know which is which so it's useless to us.
		// This shouldn't happen but we don't want to assume it never does and blindly index...
		return nil
	}
	ret := make([][]byte, len(keys))
	for i, blob := range resp {
		if blob == nil {
			if metrics {
				redisMisses.Inc()
			}
			continue
		}
		// Somewhat counter-intuitively they are always strings, regardless of what we set them as originally.
		s, ok := blob.(string)
		if !ok {
			log.Warning("Failed to cast Redis response to string, was a %T", blob)
			return nil
		}
		if metrics {
			redisHits.Inc()
			redisBytesRead.Add(float64(len(s)))
		}
		ret[i] = []byte(s)
	}
	return ret
}

// writeBlobs writes a series of blobs to Redis, detached from ctx's cancellation so it can outlive the request. They should be passed in interleaved key/value order.
func (r *elanRedisWrapper) writeBlobs(ctx context.Context, uploads []interface{}) {
	if len(uploads) == 0 {
		return
	}
	log.Debug("Writing %d blobs to Redis...", len(uploads)/2)
	if cmd := r.redis.MSet(context.WithoutCancel(ctx), uploads...); cmd.Val() != "OK" {
		log.Warning("Failed to upload %d blobs to Redis: %s", len(uploads), cmd.Err())
	}
	log.Debug("Wrote %d blobs to Redis...", len(uploads)/2)
}

func (r *elanRedisWrapper) ReadToFile(ctx context.Context, dg digest.Digest, filename string, compressed bool) error {
	if dg.Size < r.maxSize {
		blob, err := r.readRedis.Get(ctx, dg.Hash).Bytes()
		if err != nil {
			if err != redis.Nil { // Not found.
				log.Warning("Error reading blob from Redis: %s", err)
			} else {
				redisMisses.Inc()
			}
		} else {
			redisHits.Inc()
			return os.WriteFile(filename, blob, 0644)
		}
	}
	return r.elan.ReadToFile(ctx, dg, filename, compressed)
}

func (r *elanRedisWrapper) GetDirectoryTree(ctx context.Context, dg *pb.Digest, usePacks bool) ([]*pb.Directory, error) {
	// TODO(peterebden): Figure out how we are going to stitch Redis into this. There are a lot of       //                   little internal requests that we ideally want to be able to cache into Redis.
	return r.elan.GetDirectoryTree(ctx, dg, usePacks)
}

type redisClient interface {
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	Get(ctx context.Context, key string) *redis.StringCmd
	MSet(ctx context.Context, pairs ...interface{}) *redis.StatusCmd
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd
}

// monitoredRedisClient wraps redis client with an histogram to measure
// latency on different commands.
type monitoredRedisClient struct {
	redisClient
}

func (c *monitoredRedisClient) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	start := time.Now()
	defer func() {
		redisLatency.WithLabelValues("MGET").
			Observe(float64(time.Since(start).Milliseconds()))
	}()
	return c.redisClient.MGet(ctx, keys...)
}

func (c *monitoredRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	start := time.Now()
	defer func() {
		redisLatency.WithLabelValues("GET").
			Observe(float64(time.Since(start).Milliseconds()))
	}()
	return c.redisClient.Get(ctx, key)
}

func (c *monitoredRedisClient) MSet(ctx context.Context, pairs ...interface{}) *redis.StatusCmd {
	start := time.Now()
	defer func() {
		redisLatency.WithLabelValues("MSET").
			Observe(float64(time.Since(start).Milliseconds()))
	}()
	return c.redisClient.MSet(ctx, pairs...)
}

func (c *monitoredRedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	start := time.Now()
	defer func() {
		redisLatency.WithLabelValues("SET").
			Observe(float64(time.Since(start).Milliseconds()))
	}()
	return c.redisClient.Set(ctx, key, value, expiration)
}
