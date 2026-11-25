package rpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/stretchr/testify/assert"
	"gocloud.dev/blob"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Unfortunately, we can't construct the errors that gocloud would produce, so we test against
// gRPC errors.
var (
	errRateLimited        = status.Error(codes.ResourceExhausted, "exceeded the rate limit for object mutation operations")
	errFailedPrecondition = status.Error(codes.FailedPrecondition, "failed precondition")
)

// fakeBucket is a bucket whose writes fail with a given error and whose blobs appear to exist
// after a given number of existence checks.
type fakeBucket struct {
	bucket      // Unimplemented methods will panic
	mutex       sync.Mutex
	writeErr    error
	existsAfter int // Exists returns true on this call (1-indexed) and subsequently; 0 = never
	existsCalls int
}

func (f *fakeBucket) WriteAll(ctx context.Context, key string, data []byte, opts *blob.WriterOptions) error {
	return f.writeErr
}

func (f *fakeBucket) Exists(ctx context.Context, key string) (bool, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.existsCalls++
	return f.existsAfter > 0 && f.existsCalls >= f.existsAfter, nil
}

func setFastRechecks(t *testing.T) {
	old := rateLimitedWriteRechecks
	rateLimitedWriteRechecks = []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	t.Cleanup(func() { rateLimitedWriteRechecks = old })
}

func TestHandleCASWriteErrorNoError(t *testing.T) {
	b := &fakeBucket{}
	s := &server{bucket: b}
	assert.NoError(t, s.handleCASWriteError(t.Context(), "cas/ab/abcd", nil))
	assert.Equal(t, 0, b.existsCalls)
}

func TestHandleCASWriteErrorOtherError(t *testing.T) {
	b := &fakeBucket{existsAfter: 1}
	s := &server{bucket: b}
	err := errors.New("kaboom")
	assert.Equal(t, err, s.handleCASWriteError(t.Context(), "cas/ab/abcd", err))
	assert.Equal(t, 0, b.existsCalls, "Should not check for existence on errors other than rate limiting")
}

func TestHandleCASWriteErrorBlobAppears(t *testing.T) {
	setFastRechecks(t)
	b := &fakeBucket{existsAfter: 2}
	s := &server{bucket: b}
	assert.NoError(t, s.handleCASWriteError(t.Context(), "cas/ab/abcd", errRateLimited))
	assert.Equal(t, 2, b.existsCalls)
}

func TestHandleCASWriteErrorBlobNeverAppears(t *testing.T) {
	setFastRechecks(t)
	b := &fakeBucket{}
	s := &server{bucket: b}
	assert.Equal(t, errRateLimited, s.handleCASWriteError(t.Context(), "cas/ab/abcd", errRateLimited))
	assert.Equal(t, len(rateLimitedWriteRechecks), b.existsCalls)
}

func TestHandleCASWriteErrorContextCancelled(t *testing.T) {
	setFastRechecks(t)
	b := &fakeBucket{existsAfter: 1}
	s := &server{bucket: b}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	assert.Equal(t, errRateLimited, s.handleCASWriteError(ctx, "cas/ab/abcd", errRateLimited))
	assert.Equal(t, 0, b.existsCalls)
}

func TestWriteAllRateLimited(t *testing.T) {
	setFastRechecks(t)
	data := []byte("hello world")
	h := sha256.Sum256(data)
	dg := &pb.Digest{Hash: hex.EncodeToString(h[:]), SizeBytes: int64(len(data))}

	s := &server{bucket: &fakeBucket{writeErr: errRateLimited, existsAfter: 1}, largeBlobSize: DefaultMaxSize}
	assert.NoError(t, s.writeAll(t.Context(), dg, data, false))

	s = &server{bucket: &fakeBucket{writeErr: errRateLimited}, largeBlobSize: DefaultMaxSize}
	assert.Equal(t, codes.ResourceExhausted, status.Code(s.writeAll(t.Context(), dg, data, false)))
}

func TestElanClientWriteBlobRateLimited(t *testing.T) {
	setFastRechecks(t)
	e := &elanClient{s: &server{bucket: &fakeBucket{writeErr: errRateLimited, existsAfter: 1}}, timeout: time.Minute}
	_, err := e.WriteBlob(t.Context(), []byte("hello world"))
	assert.NoError(t, err)

	e = &elanClient{s: &server{bucket: &fakeBucket{writeErr: errRateLimited}}, timeout: time.Minute}
	_, err = e.WriteBlob(t.Context(), []byte("hello world"))
	assert.Error(t, err)
}

func TestHandleCASWriteFailedPrecondition(t *testing.T) {
	s := createServer(t.Context(), "mem://", 4, 0, 1<<20, nil, 0)
	assert.NoError(t, s.handleCASWriteError(t.Context(), "cas/ab/abcd", errFailedPrecondition))
	s.knownBlobCache.Wait()
	_, known := s.knownBlobCache.Get("cas/ab/abcd")
	assert.True(t, known)
}
