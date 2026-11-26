package grpcutil

import (
	"context"
	"fmt"
	"net"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	// Register the compressors on the server side.
	_ "github.com/mostynb/go-grpc-compression/zstd"
	_ "google.golang.org/grpc/encoding/gzip"

	"github.com/please-build/mettle/auth"
)

var serverMetrics = grpc_prometheus.NewServerMetrics(
	grpc_prometheus.WithServerHandlingTimeHistogram(
		grpc_prometheus.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}),
		func(opts *prometheus.HistogramOpts) {
			opts.NativeHistogramBucketFactor = 1.1
		},
	))

func init() {
	prometheus.MustRegister(serverMetrics)
}

// Opts is the set of common options for gRPC servers.
type Opts struct {
	Host          string `long:"host" description:"Host to listen on"`
	Port          int    `short:"p" long:"port" default:"7777" description:"Port to serve on"`
	KeyFile       string `short:"k" long:"key_file" description:"Key file to load TLS credentials from"`
	CertFile      string `short:"c" long:"cert_file" description:"Cert file to load TLS credentials from"`
	TLSMinVersion string `long:"tls-min-version" choice:"1.2" choice:"1.3" default:"1.3" description:"TLS minimum version allowed on new connections"`
	NoHealth      bool   `no-flag:"true" description:"Used internally to indicate when we don't want to automatically add a healthcheck."`

	AuthConfig string `long:"auth_config_file" env:"AUTH_CONFIG_FILE" description:"Config file for authentication. Overrides --token_file and --auth_all if set."`
	TokenFile  string `long:"token_file" env:"TOKEN_FILE" description:"File containing a pre-shared token that clients must provide as authentication."`
	AuthAll    bool   `long:"auth_all" description:"Require authentication on all RPCs (by default only on RPCs that mutate state)"`
}

// NewServer creates a new gRPC server with a standard set of interceptors.
// It opens the relevant port and returns a listener for it, but does not begin serving.
func NewServer(ctx context.Context, opts Opts) (net.Listener, *grpc.Server) {
	auth := loadAuth(ctx, opts)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", opts.Host, opts.Port))
	if err != nil {
		log.Fatalf("Failed to listen on %s:%d: %v", opts.Host, opts.Port, err)
	}
	log.Notice("Listening on %s:%d", opts.Host, opts.Port)

	s := grpc.NewServer(OptionalTLS(opts.KeyFile, opts.CertFile, opts.TLSMinVersion,
		grpc.ChainUnaryInterceptor(
			LogUnaryRequests,
			serverMetrics.UnaryServerInterceptor(),
			grpc_recovery.UnaryServerInterceptor(),
			unaryAuthInterceptor(auth),
		),
		grpc.ChainStreamInterceptor(
			LogStreamRequests,
			serverMetrics.StreamServerInterceptor(),
			grpc_recovery.StreamServerInterceptor(),
			streamAuthInterceptor(auth),
		),
		grpc.MaxRecvMsgSize(419430400), // 400MB
		grpc.MaxSendMsgSize(419430400),
	)...)

	serverMetrics.InitializeMetrics(s)
	reflection.Register(s)
	if !opts.NoHealth {
		grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	}
	return lis, s
}

func loadAuth(ctx context.Context, opts Opts) *auth.Authenticator {
	if opts.AuthConfig != "" {
		cfg := auth.MustLoadConfig(opts.AuthConfig)
		return auth.MustNewAuthenticator(ctx, cfg)
	}
	// Various legacy setups
	if opts.AuthAll {
		if opts.TokenFile == "" {
			log.Fatalf("Cannot specify --auth_all without --token_file")
		}
		cfg := auth.Config{
			Roles: map[string]struct {
				Permissions []string `yaml:"permissions"`
				Principals  []string `yaml:"principals"`
			}{
				auth.LegacyRole: {
					Permissions: []string{
						auth.CASRead,
						auth.CASWrite,
						auth.ACRead,
						auth.ACWrite,
						auth.ExecuteRead,
						auth.ExecuteWrite,
						auth.Fetch,
						auth.Bootstrap,
						auth.Capabilities,
						auth.GC,
						auth.Reflection,
						auth.Health,
						auth.WorkersRead,
						auth.WorkersWrite,
					},
					Principals: []string{
						auth.LegacyPrincipal,
					},
				},
			},
		}
		cfg.Static.File = opts.TokenFile
		return auth.MustNewAuthenticator(ctx, &cfg)
	} else if opts.TokenFile != "" {
		cfg := auth.Config{
			Roles: map[string]struct {
				Permissions []string `yaml:"permissions"`
				Principals  []string `yaml:"principals"`
			}{
				auth.LegacyRole: {
					Permissions: []string{
						auth.CASRead,
						auth.CASWrite,
						auth.ACRead,
						auth.ACWrite,
						auth.ExecuteRead,
						auth.ExecuteWrite,
						auth.Fetch,
						auth.Bootstrap,
						auth.Capabilities,
						auth.GC,
						auth.Reflection,
						auth.Health,
						auth.WorkersRead,
						auth.WorkersWrite,
					},
					Principals: []string{
						auth.LegacyPrincipal,
					},
				},
				auth.AnonymousRole: {
					Permissions: []string{
						auth.CASRead,
						auth.ACRead,
						auth.ExecuteRead,
						auth.Bootstrap,
						auth.Capabilities,
						auth.Reflection,
						auth.Health,
					},
					Principals: []string{
						auth.AnonymousPrincipal,
					},
				},
			},
		}
		cfg.Static.File = opts.TokenFile
		return auth.MustNewAuthenticator(ctx, &cfg)
	}

	// No auth configured; all RPCs are permitted.
	return nil
}

func unaryAuthInterceptor(authenticator *auth.Authenticator) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := authenticator.AuthRPC(ctx, info.FullMethod); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func streamAuthInterceptor(authenticator *auth.Authenticator) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authenticator.AuthRPC(ss.Context(), info.FullMethod); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}
