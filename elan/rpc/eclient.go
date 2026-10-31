package rpc

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"gocloud.dev/blob"
	"golang.org/x/sync/errgroup"
)

const compressionThreshold = 1024

// This is the implementation backed by Elan in-process.
type elanClient struct {
	s       *server
	timeout time.Duration
}

func (e *elanClient) Healthcheck() error {
	return nil // We don't have a remote server to check for health
}

func (e *elanClient) ReadBlob(dg *pb.Digest) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	blob, err := e.s.readAllBlob(ctx, "cas", dg)
	if err != nil {
		return blob, fmt.Errorf("Error reading blob: %w", err)
	}
	return blob, nil
}

func (e *elanClient) WriteBlob(b []byte) (*pb.Digest, error) {
	dg := digest.NewFromBlob(b).ToProto()
	if dg.Hash == digest.Empty.Hash {
		return dg, nil
	}
	compressed := false
	if len(b) >= compressionThreshold {
		compressed = true
		b = e.s.compressor.EncodeAll(b, make([]byte, 0, len(b)))
	}
	key := e.s.compressedKey("cas", dg, compressed)
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	if e.s.blobExists(ctx, CASPrefix, dg, compressed, false) {
		return dg, nil
	}
	if err := e.s.handleRateLimitedWrite(ctx, key, e.s.bucket.WriteAll(ctx, key, b)); err != nil {
		return dg, fmt.Errorf("Error writing blob: %w", err)
	}
	return dg, nil
}

func (e *elanClient) UpdateActionResult(req *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	return e.s.UpdateActionResult(ctx, req)
}

func (e *elanClient) UploadIfMissing(entries []*uploadinfo.Entry, compressors []pb.Compressor_Value) error {
	m := make(map[string]*uploadinfo.Entry, len(entries))
	m2 := make(map[string]pb.Compressor_Value, len(entries))
	digests := make([]*pb.Digest, len(entries))
	for i, e := range entries {
		m[e.Digest.Hash] = e
		m2[e.Digest.Hash] = compressors[i]
		digests[i] = e.Digest.ToProto()
	}
	resp, err := e.s.FindMissingBlobs(context.Background(), &pb.FindMissingBlobsRequest{
		BlobDigests: digests,
	})
	if err != nil {
		return err
	}
	var g errgroup.Group
	for _, dg := range resp.MissingBlobDigests {
		entry := m[dg.Hash]
		compressor := m2[dg.Hash]
		g.Go(func() error {
			return e.uploadOne(entry, compressor)
		})
	}
	return g.Wait()
}

func (e *elanClient) uploadOne(entry *uploadinfo.Entry, compressor pb.Compressor_Value) (err error) {
	if entry.Digest.Hash == digest.Empty.Hash {
		return nil
	}
	compressed := compressor != pb.Compressor_IDENTITY
	key := e.s.compressedKey("cas", entry.Digest.ToProto(), compressed)
	e.s.limiter <- struct{}{}
	defer func() { <-e.s.limiter }()
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	if len(entry.Contents) > 0 {
		if compressed {
			entry.Contents = e.s.compressor.EncodeAll(entry.Contents, make([]byte, 0, entry.Digest.Size))
		}
		return e.s.handleRateLimitedWrite(ctx, key, e.s.bucket.WriteAll(ctx, key, entry.Contents))
	}
	// OK it's in a file, stream it across.
	f, err := os.Open(entry.Path)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); err == nil {
			err = closeErr
		}
	}()
	wr, err := e.s.bucket.NewWriter(ctx, key, &blob.WriterOptions{BufferSize: e.s.bufferSize(entry.Digest.ToProto())})
	if err != nil {
		return err
	}

	defer func() {
		if closeErr := wr.Close(); err == nil {
			err = e.s.handleRateLimitedWrite(ctx, key, closeErr)
		}
	}()

	w := e.s.compressWriter(wr, compressed)
	if _, err := io.Copy(w, f); err != nil {
		// cancel the context before wr.Close()
		cancel()
		return err
	}
	if err := w.Close(); err != nil {
		cancel()
		return err
	}
	e.s.markKnownBlob(key)
	return nil
}

func (e *elanClient) BatchDownload(digests []digest.Digest) (map[digest.Digest][]byte, error) {
	m := make(map[digest.Digest][]byte, len(digests))
	for _, dg := range digests {
		d, err := e.downloadOne(dg.ToProto())
		if err != nil {
			return nil, err
		}
		m[dg] = d
	}
	return m, nil
}

func (e *elanClient) downloadOne(dg *pb.Digest) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	b, _, err := e.s.readAllBlobBatched(ctx, "cas", dg, true, false)
	return b, err
}

func (e *elanClient) ReadToFile(dg digest.Digest, filename string, compressed bool) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()
	if dg == digest.Empty {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()
	r, _, err := e.s.readCompressed(ctx, "cas", dg.ToProto(), false, 0, -1)
	if err != nil {
		return err
	}
	defer r.Close()
	_, err = io.Copy(f, r)
	return err
}

func (e *elanClient) GetDirectoryTree(dg *pb.Digest, stopAtPack bool) ([]*pb.Directory, error) {
	return e.s.getTree(dg, stopAtPack)
}
