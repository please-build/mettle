// Package main implements a proxy server for the Remote Execution API.
package main

import (
	"time"

	"github.com/peterebden/go-cli-init/v4/flags"
	"github.com/peterebden/go-cli-init/v4/logging"
	"google.golang.org/grpc"

	"github.com/please-build/mettle/cli"
	"github.com/please-build/mettle/flair/rpc"
	"github.com/please-build/mettle/flair/trie"
	"github.com/please-build/mettle/grpcutil"
)

var log = logging.MustGetLogger()

var opts = struct {
	Usage            string
	Logging          cli.LoggingOpts   `group:"Options controlling logging output"`
	GRPC             grpcutil.Opts     `group:"Options controlling the gRPC server"`
	Geometry         map[string]string `short:"g" long:"geometry" required:"true" description:"CAS server geometry to forward requests to (e.g. 0-f:127.0.0.1:443"`
	AssetGeometry    map[string]string `short:"a" long:"asset_geometry" description:"Asset server geometry to forward requests to. If not given then the remote asset API will be unavailable."`
	ExecutorGeometry map[string]string `short:"e" long:"executor_geometry" description:"Executor server geometry to forward request to. If not given then the executor API will be unavailable."`
	Replicas         int               `short:"r" long:"replicas" default:"1" description:"Number of servers to replicate reads/writes to"`
	ConnTLS          bool              `long:"tls" description:"Use TLS for connecting to other servers"`
	Timeout          flags.Duration    `long:"timeout" default:"20s" description:"Default timeout for all RPCs"`
	CA               string            `long:"ca" description:"File containing PEM-formatted CA certificate to verify TLS connections with"`
	Admin            cli.AdminOpts     `group:"Options controlling HTTP admin server" namespace:"admin"`
	LoadBalance      bool              `short:"b" long:"load_balance" description:"Enable basic load balancing on server requests"`
}{
	Usage: `
Flair is a proxy server used to forward requests to Elan.

The main usage for it is to load-balance requests to individual CAS servers, where each one
is responsible for a range of the hash space. This is intended for a setup with a number of
them that each handle a subset of storage. The hash space setup is configured statically on
this server so Elan remains unaware of it; this implies that online rebalancing etc is not
possible at present.

Flair continues the increasingly stretched naming scheme, referring to having a special
or instinctive ability for something (hopefully load balancing in this case). You definitely
want to have more than the minimum number of instances of it (hopefully more than fifteen...).
`,
}

func main() {
	_, info := cli.ParseFlagsOrDie("Flair", &opts, &opts.Logging)
	go cli.ServeAdmin("flair", opts.Admin, info)
	cr := newReplicator(opts.Geometry, opts.Replicas, opts.LoadBalance)
	ar := newReplicator(opts.AssetGeometry, opts.Replicas, opts.LoadBalance)
	er := newReplicator(opts.ExecutorGeometry, opts.Replicas, opts.LoadBalance)
	rpc.ServeForever(opts.GRPC, cr, ar, er, time.Duration(opts.Timeout))
}

func newReplicator(geometry map[string]string, replicas int, loadBalance bool) *trie.Replicator {
	if len(geometry) == 0 {
		return nil
	}
	t := trie.New(func(address string) (*grpc.ClientConn, error) {
		return grpcutil.Dial(address, opts.ConnTLS, opts.CA, opts.GRPC.TokenFile)
	})
	if err := t.AddAll(geometry); err != nil {
		log.Fatalf("Failed to create trie for provided geometry: %s", err)
	} else if err := t.Check(); err != nil {
		log.Fatalf("Failed to construct trie: %s", err)
	}
	return trie.NewReplicator(t, replicas, loadBalance)
}
