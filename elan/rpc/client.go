package rpc

import (
	"context"
	"strings"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/please-build/mettle/rexclient"
	hpb "google.golang.org/grpc/health/grpc_health_v1"
)

// New creates a new Client based on the given URL.
// If a scheme is given (gs:// etc) it assumes it is a bucket URL and uses that (with some
// default settings etc). The tls and tokenFile arguments are ignored in this case.
// If no scheme is given it assumes a gRPC URL and creates a remote client communicating using the API.
func New(ctx context.Context, url string, tls bool, tokenFile string) (Client, error) {
	// We can't use url.Parse here because it tends to put too much into the scheme (e.g. example.org:8080 -> scheme:example.org)
	if strings.Contains(url, "://") {
		return &elanClient{
			s:       createServer(ctx, url, 8, 10240, 10*1024*1024, nil, 0),
			timeout: 3 * time.Minute,
		}, nil
	}
	client, err := rexclient.New(ctx, "mettle", url, tls, tokenFile)
	if err != nil {
		return nil, err
	}
	return &remoteClient{
		c:      client,
		health: hpb.NewHealthClient(client.CASConnection),
	}, nil
}

// Client is a genericised interface over a limited set of actions on the CAS / AC.
type Client interface {
	Healthcheck(context.Context) error
	ReadBlob(context.Context, *pb.Digest) ([]byte, error)
	WriteBlob(context.Context, []byte) (*pb.Digest, error)
	UpdateActionResult(context.Context, *pb.UpdateActionResultRequest) (*pb.ActionResult, error)
	UploadIfMissing(context.Context, []*uploadinfo.Entry, []pb.Compressor_Value) error
	BatchDownload(context.Context, []digest.Digest) (map[digest.Digest][]byte, error)
	ReadToFile(context.Context, digest.Digest, string, bool) error
	GetDirectoryTree(context.Context, *pb.Digest, bool) ([]*pb.Directory, error)
}
