package rpc

import (
	"context"
	"io"
	"io/ioutil"

	"cloud.google.com/go/storage"
	"github.com/klauspost/compress/zstd"
	"gocloud.dev/blob"
	"google.golang.org/api/googleapi"
)

// A bucket is a minimal interface that we need to talk to our backend storage.
// It is very similar to a subset of blob.Bucket but slightly different (the reader and
// writer returned are more generic to facilitate us putting a generic interface around it).
type bucket interface {
	NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blob.ReaderOptions) (io.ReadCloser, error)
	NewWriter(ctx context.Context, key string, opts *blob.WriterOptions) (io.WriteCloser, error)
	WriteAll(ctx context.Context, key string, data []byte) error
	ReadAll(ctx context.Context, key string) ([]byte, error)
	Exists(ctx context.Context, key string) (bool, error)
	List(opts *blob.ListOptions) *blob.ListIterator
	Delete(ctx context.Context, key string, hard bool) error
}

func mustOpenStorage(url string) bucket {
	bucket, err := blob.OpenBucket(context.Background(), url)
	if err != nil {
		log.Fatalf("Failed to open storage bucket %s: %v", url, err)
	}

	var gcsClient *storage.Client
	if bucket.As(&gcsClient) {
		gcsClient.SetRetry()
	}

	return &adapter{bucket: bucket}
}

type adapter struct {
	bucket *blob.Bucket
}

func (a *adapter) NewRangeReader(ctx context.Context, key string, offset, length int64, opts *blob.ReaderOptions) (io.ReadCloser, error) {
	return a.bucket.NewRangeReader(ctx, key, offset, length, opts)
}

func (a *adapter) NewWriter(ctx context.Context, key string, opts *blob.WriterOptions) (io.WriteCloser, error) {
	return a.bucket.NewWriter(ctx, key, opts)
}

func (a *adapter) WriteAll(ctx context.Context, key string, data []byte) error {
	// Without a BufferSize the GCS writer allocates a full upload chunk (16 MiB) per call, however small the data is.
	// The buffer must be larger than the data: if the data exactly fills it the client cannot see EOF and falls
	// back to a slower multi-request resumable upload.
	size := min(len(data)+1, googleapi.DefaultUploadChunkSize)
	return a.bucket.WriteAll(ctx, key, data, &blob.WriterOptions{BufferSize: size})
}

func (a *adapter) ReadAll(ctx context.Context, key string) ([]byte, error) {
	return a.bucket.ReadAll(ctx, key)
}

func (a *adapter) Exists(ctx context.Context, key string) (bool, error) {
	return a.bucket.Exists(ctx, key)
}

func (a *adapter) List(opts *blob.ListOptions) *blob.ListIterator {
	return a.bucket.List(opts)
}

func (a *adapter) Delete(ctx context.Context, key string, hard bool) error {
	return a.bucket.Delete(ctx, key) // this is always a 'hard' delete
}

// compressedReader returns a reader wrapped in a decompressor or compressor as needed.
func (s *server) compressedReader(r io.ReadCloser, needCompression, isCompressed bool, offset int64) (io.ReadCloser, bool, error) {
	if needCompression == isCompressed {
		// Just stream the bytes back directly. This is not in line with the API (which says that offsets
		// always refer to the uncompressed stream) but (more relevantly to us right now) does match what
		// the SDK does (which currently just counts bytes it's read off the remote, regardless of mode).
		return r, false, nil
	} else if isCompressed && offset != 0 {
		// Offsets refer into the uncompressed blob, we have to handle that ourselves.
		r = s.decompressReader(r)
		_, err := io.CopyN(ioutil.Discard, r, offset)
		return r, false, err
	} else if isCompressed {
		return s.decompressReader(r), false, nil
	}
	return r, true, nil
}

// decompressReader wraps a reader in zstd decompression.
func (s *server) decompressReader(r io.ReadCloser) io.ReadCloser {
	zr, _ := zstd.NewReader(r)
	return &zstdReadCloser{c: r, r: zr}
}

type zstdReadCloser struct {
	c io.Closer
	r *zstd.Decoder
}

func (c *zstdReadCloser) Read(p []byte) (int, error) {
	return c.r.Read(p)
}

func (c *zstdReadCloser) Close() error {
	c.r.Close()
	return c.c.Close()
}

// compressWriter optionally wraps a writer in zstd compression.
func (s *server) compressWriter(w io.Writer, needCompression bool) io.WriteCloser {
	if !needCompression {
		return &nopCloser{w: w}
	}
	zw, _ := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
	return zw
}

type nopCloser struct {
	w io.Writer
}

func (n *nopCloser) Write(p []byte) (int, error) {
	return n.w.Write(p)
}

func (n *nopCloser) Close() error {
	return nil
}
