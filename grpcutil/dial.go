package grpcutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
)

// Dial is a convenience function wrapping up some common gRPC functionality.
// If the URL is prefixed by a protocol (grpc:// or grpcs://) that overrides the TLS flag.
func Dial(address string, tls bool, caFile, tokenFile string) (*grpc.ClientConn, error) {
	address, tls = parseAddress(address, tls)
	return grpc.Dial(address, append(DialOptions(tokenFile), tlsOpt(tls, caFile))...)
}

// DialOptions returns some common dial options.
func DialOptions(tokenFile string) []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(419430400)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    20 * time.Second,
			Timeout: 20 * time.Second,
		}),
	}
	if tokenFile == "" {
		return opts
	}
	return append(opts, grpc.WithPerRPCCredentials(tokenCredProvider{
		"authorization": "Bearer " + mustLoadToken(tokenFile),
	}))
}

func tlsOpt(useTLS bool, caFile string) grpc.DialOption {
	if useTLS {
		if caFile != "" {
			return grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
				RootCAs: mustLoadCACert(caFile),
			}))
		}
		return grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, ""))
	}
	return grpc.WithInsecure()
}

// parseAddress parses a URL with an optional prefix like grpcs:// and returns the URL without it.
func parseAddress(url string, tlsDefault bool) (string, bool) {
	if strings.HasPrefix(url, "grpcs://") {
		return strings.TrimPrefix(url, "grpcs://"), true
	} else if strings.HasPrefix(url, "grpc://") {
		return strings.TrimPrefix(url, "grpc://"), false
	}
	return url, tlsDefault
}

// mustLoadCACert loads a CA cert from a file and dies on any errors.
func mustLoadCACert(filename string) *x509.CertPool {
	ca, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("Failed to read CA cert from %s: %s", filename, err)
	}
	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(ca) {
		log.Fatalf("Failed to append CA cert to pool (invalid PEM file?)")
	}
	return cp
}

func mustLoadToken(tokenFile string) string {
	contents, err := os.ReadFile(tokenFile)
	if err != nil {
		log.Fatalf("Failed to read token file: %s", err)
	}
	return strings.TrimSpace(string(contents))
}

type tokenCredProvider map[string]string

func (cred tokenCredProvider) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return cred, nil
}

func (cred tokenCredProvider) RequireTransportSecurity() bool {
	return false // Allow these to be provided over an insecure channel; this facilitates e.g. service meshes like Istio.
}
