package rpc

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	elan "github.com/please-build/mettle/elan/rpc"
	"github.com/please-build/mettle/grpcutil"
	"github.com/please-build/mettle/rexclient"
)

const elanPort = 17791

var content = []byte("the quick brown fox jumps over the lazy dog")

func sriFor(b []byte) string {
	h := sha256.Sum256(b)
	return "sha256-" + base64.StdEncoding.EncodeToString(h[:])
}

func sriQualifier(sri string) []*pb.Qualifier {
	return []*pb.Qualifier{{Name: "checksum.sri", Value: sri}}
}

// newTestServer returns a new Zeal server backed by the test Elan, and an HTTP server serving
// some content which counts how many times it has been requested.
func newTestServer(t *testing.T) (*server, string, *atomic.Int64) {
	t.Helper()
	var requests atomic.Int64
	h := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Write(content)
	}))
	t.Cleanup(h.Close)
	client, err := rexclient.New(t.Context(), "mettle", fmt.Sprintf("127.0.0.1:%d", elanPort), false, "")
	require.NoError(t, err)
	srv := &server{
		client:        retryablehttp.NewClient(),
		storageClient: client,
		limiter:       make(chan struct{}, 4),
	}
	srv.client.RetryMax = 0
	return srv, h.URL + "/" + t.Name(), &requests
}

func TestFetchBlobVerifiesChecksum(t *testing.T) {
	s, url, _ := newTestServer(t)
	resp, err := s.FetchBlob(t.Context(), &pb.FetchBlobRequest{
		Timeout:    durationpb.New(time.Minute),
		Uris:       []string{url},
		Qualifiers: sriQualifier(sriFor(content)),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(len(content)), resp.BlobDigest.SizeBytes)

	_, err = s.FetchBlob(t.Context(), &pb.FetchBlobRequest{
		Timeout:    durationpb.New(time.Minute),
		Uris:       []string{url + "-other"},
		Qualifiers: sriQualifier(sriFor([]byte("wibble"))),
	})
	assert.Error(t, err)
}

func TestFetchBlobDifferentChecksumForSameURL(t *testing.T) {
	s, url, requests := newTestServer(t)
	_, err := s.FetchBlob(t.Context(), &pb.FetchBlobRequest{
		Timeout:    durationpb.New(time.Minute),
		Uris:       []string{url},
		Qualifiers: sriQualifier(sriFor(content)),
	})
	require.NoError(t, err)
	// A subsequent request for the same URL with a checksum it doesn't match must fail, not be satisfied
	// by the earlier download.
	_, err = s.FetchBlob(t.Context(), &pb.FetchBlobRequest{
		Timeout:    durationpb.New(time.Minute),
		Uris:       []string{url},
		Qualifiers: sriQualifier(sriFor([]byte("wibble"))),
	})
	assert.Error(t, err)
	assert.EqualValues(t, 2, requests.Load())
}

func TestFetchBlobUnverifiedThenVerified(t *testing.T) {
	s, url, _ := newTestServer(t)
	_, err := s.FetchBlob(t.Context(), &pb.FetchBlobRequest{
		Timeout: durationpb.New(time.Minute),
		Uris:    []string{url},
	})
	require.NoError(t, err)
	// An earlier download with no checksum must not satisfy one that has one.
	_, err = s.FetchBlob(t.Context(), &pb.FetchBlobRequest{
		Timeout:    durationpb.New(time.Minute),
		Uris:       []string{url},
		Qualifiers: sriQualifier(sriFor([]byte("wibble"))),
	})
	assert.Error(t, err)
}

func TestFetchBlobNoTimeout(t *testing.T) {
	s, url, _ := newTestServer(t)
	_, err := s.FetchBlob(t.Context(), &pb.FetchBlobRequest{
		Uris: []string{url},
	})
	assert.NoError(t, err, "a request with no timeout should not time out immediately")
	_, err = s.FetchBlob(t.Context(), &pb.FetchBlobRequest{
		Timeout: durationpb.New(0),
		Uris:    []string{url + "-zero"},
	})
	assert.NoError(t, err, "a zero timeout should be treated as no timeout")
}

func TestFetchBlobTimeout(t *testing.T) {
	s, _, _ := newTestServer(t)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
		w.Write(content)
	}))
	defer slow.Close()
	start := time.Now()
	_, err := s.FetchBlob(t.Context(), &pb.FetchBlobRequest{
		Timeout: durationpb.New(200 * time.Millisecond),
		Uris:    []string{slow.URL + "/" + t.Name()},
	})
	assert.Error(t, err)
	assert.Less(t, time.Since(start), 3*time.Second, "the request's timeout should be applied")
}

func TestFetchBlobSameQualifiersIsDeduplicated(t *testing.T) {
	s, url, requests := newTestServer(t)
	for i := 0; i < 3; i++ {
		// Call this directly to bypass the action cache check in FetchBlob.
		_, err := s.singleflightFetchURL(t.Context(), url, sriQualifier(sriFor(content)))
		require.NoError(t, err)
	}
	assert.EqualValues(t, 1, requests.Load())
}

func TestDownloadKey(t *testing.T) {
	a := []*pb.Qualifier{{Name: "checksum.sri", Value: "sha256-abc"}, {Name: "foo", Value: "bar"}}
	b := []*pb.Qualifier{{Name: "foo", Value: "bar"}, {Name: "checksum.sri", Value: "sha256-abc"}}
	assert.Equal(t, downloadKey("https://example.com", a), downloadKey("https://example.com", b))
	assert.NotEqual(t, downloadKey("https://example.com", a), downloadKey("https://example.com", a[:1]))
	assert.NotEqual(t, downloadKey("https://example.com", nil), downloadKey("https://example.com", a[:1]))
	assert.Equal(t, "https://example.com", downloadKey("https://example.com", nil))
}

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "zeal_test")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	go elan.ServeForever(context.Background(), grpcutil.Opts{Host: "127.0.0.1", Port: elanPort}, "file://"+dir, 4, 0, 0, nil, 0)
	for i := 0; i < 100; i++ {
		if conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", elanPort)); err == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	os.Exit(m.Run())
}
