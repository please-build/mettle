// Package rexclient implements some common functionality around creating Remote Execution clients.
package rexclient

import (
	"context"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/peterebden/go-cli-init/v4/logging"

	"github.com/please-build/mettle/grpcutil"
)

var log = logging.MustGetLogger()

// CompressionThreshold is the minimum size (in bytes) for the client to consider any blob
// for compression. Empirical evidence suggests zstd typically makes it worse at sizes under
// a few hundred bytes and gains are minimal until approaching a kilobyte.
const CompressionThreshold = 1024

// New creates a new remote execution client.
// It automatically handles some things like compression.
func New(instanceName, url string, tls bool, tokenFile string) (*client.Client, error) {
	log.Notice("Dialling remote %s...", url)
	client, err := client.NewClient(context.Background(), instanceName,
		client.DialParams{
			Service:            url,
			NoSecurity:         !tls,
			TransportCredsOnly: tls,
			DialOpts:           grpcutil.DialOptions(tokenFile),
		},
		client.UseBatchOps(true),
		client.RetryTransient(),
		&client.TreeSymlinkOpts{Preserved: true},
		client.CompressedBytestreamThreshold(CompressionThreshold),
		client.UploadCompressionPredicate(DetectArchiveUploads),
		client.UsePackName(PackName),
	)
	if err != nil {
		log.Error("Error initialising remote execution client: %s", err)
		return nil, err
	}
	log.Notice("Connected to remote server on %s", url)
	return client, nil
}

// MustNew is like New but dies on errors.
func MustNew(instanceName, url string, tls bool, tokenFile string) *client.Client {
	client, err := New(instanceName, url, tls, tokenFile)
	if err != nil {
		log.Fatalf("Failed to contact remote server: %s", err)
	}
	return client
}

// Uninitialised returns an uninitialised client that's not suitable for making remote requests
// with (but can still be useful for all-local logic like ComputeOutputsToUpload)
func Uninitialised() *client.Client {
	c := &client.Client{}
	o := client.TreeSymlinkOpts{Preserved: true}
	o.Apply(c)
	client.CompressedBytestreamThreshold(CompressionThreshold).Apply(c)
	client.UsePackName(PackName).Apply(c)
	return c
}
