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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errRateLimited = status.Error(codes.ResourceExhausted, "exceeded the rate limit for object mutation operations")

// fakeBucket is a bucket whose writes fail with a given error and whose blobs appear to exist
// after a given number of existence checks.
type fakeBucket struct {
	bucket      // Unimplemented methods will panic
	mutex       sync.Mutex
	writeErr    error
	existsAfter int // Exists returns true on this call (1-indexed) and subsequently; 0 = never
	existsCalls int
}

func (f *fakeBucket) WriteAll(ctx context.Context, key string, data []byte) error {
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

func TestHandleRateLimitedWriteNoError(t *testing.T) {
	b := &fakeBucket{}
	s := &server{bucket: b}
	assert.NoError(t, s.handleRateLimitedWrite(context.Background(), "cas/ab/abcd", nil))
	assert.Equal(t, 0, b.existsCalls)
}

func TestHandleRateLimitedWriteOtherError(t *testing.T) {
	b := &fakeBucket{existsAfter: 1}
	s := &server{bucket: b}
	err := errors.New("kaboom")
	assert.Equal(t, err, s.handleRateLimitedWrite(context.Background(), "cas/ab/abcd", err))
	assert.Equal(t, 0, b.existsCalls, "Should not check for existence on errors other than rate limiting")
}

func TestHandleRateLimitedWriteBlobAppears(t *testing.T) {
	setFastRechecks(t)
	b := &fakeBucket{existsAfter: 2}
	s := &server{bucket: b}
	assert.NoError(t, s.handleRateLimitedWrite(context.Background(), "cas/ab/abcd", errRateLimited))
	assert.Equal(t, 2, b.existsCalls)
}

func TestHandleRateLimitedWriteBlobNeverAppears(t *testing.T) {
	setFastRechecks(t)
	b := &fakeBucket{}
	s := &server{bucket: b}
	assert.Equal(t, errRateLimited, s.handleRateLimitedWrite(context.Background(), "cas/ab/abcd", errRateLimited))
	assert.Equal(t, len(rateLimitedWriteRechecks), b.existsCalls)
}

func TestHandleRateLimitedWriteContextCancelled(t *testing.T) {
	setFastRechecks(t)
	b := &fakeBucket{existsAfter: 1}
	s := &server{bucket: b}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.Equal(t, errRateLimited, s.handleRateLimitedWrite(ctx, "cas/ab/abcd", errRateLimited))
	assert.Equal(t, 0, b.existsCalls)
}

func TestWriteAllRateLimited(t *testing.T) {
	setFastRechecks(t)
	data := []byte("hello world")
	h := sha256.Sum256(data)
	dg := &pb.Digest{Hash: hex.EncodeToString(h[:]), SizeBytes: int64(len(data))}

	s := &server{bucket: &fakeBucket{writeErr: errRateLimited, existsAfter: 1}, largeBlobSize: DefaultMaxSize}
	assert.NoError(t, s.writeAll(context.Background(), dg, data, false))

	s = &server{bucket: &fakeBucket{writeErr: errRateLimited}, largeBlobSize: DefaultMaxSize}
	assert.Equal(t, codes.ResourceExhausted, status.Code(s.writeAll(context.Background(), dg, data, false)))
}

func TestElanClientWriteBlobRateLimited(t *testing.T) {
	setFastRechecks(t)
	// The initial existence check in WriteBlob is the first call, so make it appear on the second.
	e := &elanClient{s: &server{bucket: &fakeBucket{writeErr: errRateLimited, existsAfter: 2}}, timeout: time.Minute}
	_, err := e.WriteBlob([]byte("hello world"))
	assert.NoError(t, err)

	e = &elanClient{s: &server{bucket: &fakeBucket{writeErr: errRateLimited}}, timeout: time.Minute}
	_, err = e.WriteBlob([]byte("hello world"))
	assert.Error(t, err)
}
