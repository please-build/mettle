package rpc

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDigest(data []byte) *pb.Digest {
	h := sha256.Sum256(data)
	return &pb.Digest{Hash: hex.EncodeToString(h[:]), SizeBytes: int64(len(data))}
}

// TestWriteToExistingBlob checks that every CAS write path leaves an existing blob in GCS untouched
// (i.e. uses a conditional write with WriterOptions.IfNotExist), and treats the resulting
// precondition failure as success.
// Note that this can't be combined with TestWriteToNewBlob because the intefaces being tested here
// compute the filename from the data as required by CAS, and so there's no way to assert that files
// weren't overwritten without manually writing a file whose digest doesn't actually match the path
// it was written under.
func TestWriteToExistingBlob(t *testing.T) {
	data := []byte("hello world")
	dg := testDigest(data)
	key := "cas/" + dg.Hash[:2] + "/" + dg.Hash

	for _, tc := range []struct {
		name  string
		write func(t *testing.T, s *server) error
	}{
		{
			name: "writeAll",
			write: func(t *testing.T, s *server) error {
				return s.writeAll(t.Context(), dg, data, false)
			},
		},
		{
			name: "writeBlob",
			write: func(t *testing.T, s *server) error {
				return s.writeBlob(t.Context(), "cas", dg, bytes.NewReader(data), false)
			},
		},
		{
			name: "elanClient WriteBlob",
			write: func(t *testing.T, s *server) error {
				_, err := (&elanClient{s: s, timeout: time.Minute}).WriteBlob(t.Context(), data)
				return err
			},
		},
		{
			name: "elanClient uploadOne with contents",
			write: func(t *testing.T, s *server) error {
				return (&elanClient{s: s, timeout: time.Minute}).uploadOne(t.Context(), uploadinfo.EntryFromBlob(data), pb.Compressor_IDENTITY)
			},
		},
		{
			name: "elanClient uploadOne from file",
			write: func(t *testing.T, s *server) error {
				path := filepath.Join(t.TempDir(), "blob")
				require.NoError(t, os.WriteFile(path, data, 0o600))
				return (&elanClient{s: s, timeout: time.Minute}).uploadOne(t.Context(), uploadinfo.EntryFromFile(digest.NewFromBlob(data), path), pb.Compressor_IDENTITY)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := createServer(t.Context(), "mem://", 4, 0, 0, nil, 0)
			require.NoError(t, s.bucket.WriteAll(t.Context(), key, []byte("original"), nil))

			assert.NoError(t, tc.write(t, s))

			got, err := s.bucket.ReadAll(t.Context(), key)
			require.NoError(t, err)
			assert.Equal(t, []byte("original"), got)
		})
	}
}

// TestWriteToNewBlob checks that writes still store the blob when it doesn't exist yet.
func TestWriteToNewBlob(t *testing.T) {
	data := []byte("hello world")
	dg := testDigest(data)
	key := "cas/" + dg.Hash[:2] + "/" + dg.Hash

	for _, tc := range []struct {
		name  string
		write func(t *testing.T, s *server) error
	}{
		{
			name: "writeAll",
			write: func(t *testing.T, s *server) error {
				return s.writeAll(t.Context(), dg, data, false)
			},
		},
		{
			name: "writeBlob",
			write: func(t *testing.T, s *server) error {
				return s.writeBlob(t.Context(), "cas", dg, bytes.NewReader(data), false)
			},
		},
		{
			name: "elanClient WriteBlob",
			write: func(t *testing.T, s *server) error {
				_, err := (&elanClient{s: s, timeout: time.Minute}).WriteBlob(t.Context(), data)
				return err
			},
		},
		{
			name: "elanClient uploadOne with contents",
			write: func(t *testing.T, s *server) error {
				return (&elanClient{s: s, timeout: time.Minute}).uploadOne(t.Context(), uploadinfo.EntryFromBlob(data), pb.Compressor_IDENTITY)
			},
		},
		{
			name: "elanClient uploadOne from file",
			write: func(t *testing.T, s *server) error {
				path := filepath.Join(t.TempDir(), "blob")
				require.NoError(t, os.WriteFile(path, data, 0o600))
				return (&elanClient{s: s, timeout: time.Minute}).uploadOne(t.Context(), uploadinfo.EntryFromFile(digest.NewFromBlob(data), path), pb.Compressor_IDENTITY)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := createServer(t.Context(), "mem://", 4, 0, 0, nil, 0)

			require.NoError(t, tc.write(t, s))

			got, err := s.bucket.ReadAll(t.Context(), key)
			require.NoError(t, err)
			assert.Equal(t, data, got)
		})
	}
}
