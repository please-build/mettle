// Package main implements a server to record the state of a fleet of Mettle workers.
package main

import (
	"context"
	"time"

	"github.com/peterebden/go-cli-init/v4/flags"

	"github.com/please-build/mettle/cli"
	"github.com/please-build/mettle/grpcutil"
	"github.com/please-build/mettle/lucidity/rpc"
)

var opts = struct {
	Usage         string
	Logging       cli.LoggingOpts `group:"Options controlling logging output"`
	HTTPPort      int             `long:"http_port" default:"7773" description:"Port to serve HTTP on"`
	MaxAge        flags.Duration  `long:"max_age" description:"Forget results from any workers older than this"`
	MinProportion float64         `long:"min_proportion" default:"0.2" description:"Min proportion of workers at a particular version before it's enabled"`
	IAP           struct {
		Audience string   `long:"audience" description:"Expected audience for the IAP tokens"`
		Users    []string `short:"u" long:"user" env:"LUCIDITY_IAP_USERS" env-delim:"," description:"Users allowed to make mutating actions on the server"`
	} `group:"Options controlling Cloud IAP auth"`
	GRPC  grpcutil.Opts `group:"Options controlling the gRPC server"`
	Admin cli.AdminOpts `group:"Options controlling HTTP admin server" namespace:"admin"`
}{
	Usage: `
Lucidity is a server that records the state of a fleet of Mettle workers.

Essentially the workers periodically report their current state to it; it exists simply to
make it easy to keep an eye on the state of a distributed fleet of them.

Lucidity carries on with the somewhat overdone naming scheme, referring to having a
quality of being able to think or express something clearly. If we need a video game reference
to go along with Zeal, then it could refer to the GTA Lucidity in FreeSpace 2.
`,
}

func main() {
	_, info := cli.ParseFlagsOrDie("Lucidity", &opts, &opts.Logging)
	go cli.ServeAdmin("lucidity", opts.Admin, info)
	rpc.ServeForever(context.Background(), opts.GRPC, opts.HTTPPort, time.Duration(opts.MaxAge), opts.MinProportion, opts.IAP.Audience, opts.IAP.Users)
}
