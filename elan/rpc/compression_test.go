package rpc

import (
	"bytes"
	"context"
	"io"
	"os"
	"path"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thought-machine/please-servers/grpcutil"
	bs "google.golang.org/genproto/googleapis/bytestream"
)

var bsClient bs.ByteStreamClient

const (
	// These are determined empirically from the generated test files.
	hash  = "2e2595aaffe0987a183d3ba9a3c5d81c79d29f3f6e2b2dd47e628d6bf9b12725"
	size  = 5377790
	ssize = "5377790"
	name  = "blobs/" + hash + "/" + ssize
	cname = "compressed-blobs/zstd/" + hash + "/" + ssize
)

var expectedData, compressedData []byte

func TestStreamRead(t *testing.T) {
	client, err := bsClient.Read(context.Background(), &bs.ReadRequest{
		ResourceName: name,
	})
	require.NoError(t, err)
	buf := []byte{}
	for {
		msg, err := client.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		buf = append(buf, msg.Data...)
	}
	assert.Equal(t, expectedData, buf)
}

func TestStreamReadResume(t *testing.T) {
	client, err := bsClient.Read(context.Background(), &bs.ReadRequest{
		ResourceName: name,
	})
	require.NoError(t, err)
	buf := []byte{}
	for i := 0; i < 3; i++ {
		msg, err := client.Recv()
		require.NoError(t, err)
		buf = append(buf, msg.Data...)
	}
	// Pretend the connection gets broken here.
	client, err = bsClient.Read(context.Background(), &bs.ReadRequest{
		ResourceName: name,
		ReadOffset:   int64(len(buf)),
	})
	require.NoError(t, err)
	for {
		msg, err := client.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		buf = append(buf, msg.Data...)
	}
	assert.Equal(t, expectedData, buf)
}

func TestStreamReadResumeWithLimit(t *testing.T) {
	client, err := bsClient.Read(context.Background(), &bs.ReadRequest{
		ResourceName: name,
	})
	require.NoError(t, err)
	buf := []byte{}
	for i := 0; i < 6; i++ {
		msg, err := client.Recv()
		require.NoError(t, err)
		buf = append(buf, msg.Data...)
	}
	// Pretend the connection gets broken here.
	client, err = bsClient.Read(context.Background(), &bs.ReadRequest{
		ResourceName: name,
		ReadOffset:   int64(len(buf)),
		ReadLimit:    int64(size - len(buf)),
	})
	require.NoError(t, err)
	for {
		msg, err := client.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		buf = append(buf, msg.Data...)
	}
	assert.Equal(t, expectedData, buf)
}

func TestCompressedRead(t *testing.T) {
	client, err := bsClient.Read(context.Background(), &bs.ReadRequest{
		ResourceName: cname,
	})
	require.NoError(t, err)
	buf := []byte{}
	for {
		msg, err := client.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		buf = append(buf, msg.Data...)
	}
	assert.Equal(t, compressedData, buf)
}

func TestCompressedReadResume(t *testing.T) {
	r := newReconnectingReader(bsClient, cname)
	var buf, ubuf bytes.Buffer
	zr, err := zstd.NewReader(io.TeeReader(r, &buf))
	require.NoError(t, err)
	// Allocate our own buffer so we know it's small enough to require multiple reads.
	n, err := io.CopyBuffer(&ubuf, zr, make([]byte, 1000))
	require.NoError(t, err)
	assert.EqualValues(t, n, size)
	assert.True(t, r.Total < n) // We don't know exactly what this should be.
	assert.Equal(t, len(expectedData), ubuf.Len())
	assert.Equal(t, len(compressedData), buf.Len())
	assert.Equal(t, expectedData, ubuf.Bytes())
	assert.Equal(t, compressedData, buf.Bytes())
}

func testMain(m *testing.M) int {
	storage := "file://" + os.Getenv("TEST_DIR") + "/elan/rpc"
	lis, s := startServer(grpcutil.Opts{
		Host: "127.0.0.1",
		Port: 7777,
	}, storage, 5, 1000, 1000, nil, 0)
	go grpcutil.ServeForever(lis, s)
	defer s.Stop()

	conn, err := grpcutil.Dial("127.0.0.1:7777", false, "", "")
	if err != nil {
		log.Fatalf("%s", err)
	}
	bsClient = bs.NewByteStreamClient(conn)
	defer conn.Close()

	expectedData, err = os.ReadFile(path.Join("elan/rpc/cas", hash[:2], hash))
	if err != nil {
		log.Fatalf("%s", err)
	}
	compressedData, err = os.ReadFile(path.Join("elan/rpc/zstd_cas", hash[:2], hash))
	if err != nil {
		log.Fatalf("%s", err)
	}

	return m.Run()
}

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

// A reconnectingReader implements an io.Reader over the bytestream.Read RPC
// by reconnecting every time to simulate connection failures.
type reconnectingReader struct {
	client bs.ByteStreamClient
	name   string
	offset int64
	Total  int64
}

func newReconnectingReader(client bs.ByteStreamClient, resourceName string) *reconnectingReader {
	return &reconnectingReader{
		client: client,
		name:   resourceName,
	}
}

func (r *reconnectingReader) Read(buf []byte) (int, error) {
	stream, err := r.client.Read(context.Background(), &bs.ReadRequest{
		ResourceName: r.name,
		ReadOffset:   r.offset,
	})
	if err != nil {
		return 0, err
	}
	resp, err := stream.Recv()
	if err != nil {
		return 0, err
	}
	n := len(buf)
	if n > len(resp.Data) {
		n = len(resp.Data)
	}
	copy(buf, resp.Data[:n])
	r.offset += int64(n)
	r.Total += int64(n)
	return n, err
}
