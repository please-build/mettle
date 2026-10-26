package rpc

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gocloud.dev/gcerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rateLimitedWriteRechecks are the delays between successive checks for a blob's existence after
// a write to it has been rate limited.
var rateLimitedWriteRechecks = []time.Duration{
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2 * time.Second,
}

var rateLimitedWrites = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "elan",
	Name:      "rate_limited_writes_total",
	Help:      "Number of CAS writes rejected by the storage backend as rate limited, partitioned by whether the blob was subsequently found to exist.",
}, []string{"recovered"})

func init() {
	prometheus.MustRegister(rateLimitedWrites)
}

// handleRateLimitedWrite handles an error from writing a CAS blob to the bucket.
//
// GCS limits writes to a single object to around one per second, so if several clients upload the
// same blob at once all but one of them can be rejected with a 429. Since CAS keys are content
// addressed, the write that succeeded must have had the same contents, so once we've confirmed that
// the blob really does exist we can treat our write as successful too. The successful write may
// still be in progress when we get rejected so we give it a little while to appear.
//
// The given context must not have been cancelled as a result of the error (otherwise we can't check).
// Returns nil if the blob is found to exist, otherwise the original error.
func (s *server) handleRateLimitedWrite(ctx context.Context, key string, err error) error {
	if !isRateLimited(err) {
		return err
	}
	for _, delay := range rateLimitedWriteRechecks {
		select {
		case <-ctx.Done():
			rateLimitedWrites.WithLabelValues("false").Inc()
			return err
		case <-time.After(delay):
		}
		// Deliberately bypass the known blob cache & Redis; we only trust the bucket itself here.
		if s.blobExistsUncached(ctx, key) {
			log.Debug("Write of %s was rate limited, but it exists now", key)
			rateLimitedWrites.WithLabelValues("true").Inc()
			s.markKnownBlob(key)
			return nil
		}
	}
	rateLimitedWrites.WithLabelValues("false").Inc()
	return err
}

// isRateLimited returns true if the given error is for a request being rate limited.
func isRateLimited(err error) bool {
	return err != nil && (gcerrors.Code(err) == gcerrors.ResourceExhausted || status.Code(err) == codes.ResourceExhausted)
}
