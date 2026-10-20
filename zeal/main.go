// Package main implements an server for the Remote Asset API.
package main

import (
	"os"

	"strings"

	"github.com/thought-machine/please-servers/cli"
	"github.com/thought-machine/please-servers/grpcutil"
	"github.com/thought-machine/please-servers/zeal/rpc"
)

var log = cli.MustGetLogger()

var opts = struct {
	Usage       string
	Logging     cli.LoggingOpts              `group:"Options controlling logging output"`
	GRPC        grpcutil.Opts                `group:"Options controlling the gRPC server"`
	Parallelism int                          `long:"parallelism" default:"4" description:"Max parallel download tasks to run"`
	Headers     map[string]map[string]string `short:"H" long:"header" description:"Headers to set on downloads, as a map of domain -> header name -> header"`
	Auth        map[string]string            `short:"a" long:"auth" description:"Authorization header to use per domain, as a map of URL prefix -> filename to read from"`
	Storage     struct {
		Storage string `short:"s" long:"storage" env:"STORAGE_URL" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
		TLS     bool   `long:"tls" description:"Use TLS for communication with the storage server"`
	} `group:"Options controlling communication with the CAS server"`
	Admin         cli.AdminOpts `group:"Options controlling HTTP admin server" namespace:"admin"`
	ForceCasCheck bool          `long:"force-check" description:"Force check for the CAS even when we have an action result"`
}{
	Usage: `
Zeal is a partial implementation of the Remote Asset API.
It supports only the FetchBlob RPC of the Fetch service; FetchDirectory and the Push service
are not implemented.

The only qualifier that is reliably supported is checksum.sri for hash verification. It does
not understand any communication protocol other than HTTP(S); we may add Git support in future.
SHA256 (preferred) and SHA1 (for compatibility) are the only supported hash functions.
Requests without SRI attached will be rejected with extreme prejudice.

It must communicate with a CAS server to store its eventual blobs.

Requests are downloaded entirely into memory before being uploaded, so the user should ensure there is
enough memory available for any likely request.

It is partly named to match the ongoing theme of "qualities a person can have", and partly
for the Paladin skill in Diablo II since its job is to bang things down as fast as possible.
`,
}

func main() {
	_, info := cli.ParseFlagsOrDie("Zeal", &opts, &opts.Logging)
	go cli.ServeAdmin("zeal", opts.Admin, info)

	for domain, headers := range opts.Headers {
		for name, header := range headers {
			log.Notice("Header configured for %s: %s: %s", domain, name, header)
		}
	}

	auth := make(map[string]string, len(opts.Auth))
	for prefix, filename := range opts.Auth {
		b, err := os.ReadFile(filename)
		if err != nil {
			log.Fatalf("Failed to read auth token from %s: %s", filename, err)
		}
		// Don't require the protocol on the front, that doesn't play nicely with the flags format.
		if !strings.HasPrefix(prefix, "https://") {
			prefix = "https://" + prefix
		}
		auth[prefix] = strings.TrimSpace(string(b))
		log.Notice("Loaded auth credentials for %s", prefix)
	}

	rpc.ServeForever(opts.GRPC, opts.Storage.Storage, opts.Storage.TLS, opts.Parallelism, opts.Headers, auth, opts.ForceCasCheck)
}
