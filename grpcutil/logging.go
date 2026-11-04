// Package grpcutil implements some common functionality for gRPC that we use across all servers.
package grpcutil

import (
	"context"
	"errors"
	"time"

	cli "github.com/peterebden/go-cli-init/v4/logging"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/status"
	"gopkg.in/op/go-logging.v1"
)

var log = cli.MustGetLogger()

// grpcLogMabob is an implementation of grpc's logging interface using our backend.
type grpcLogMabob struct{}

func (g *grpcLogMabob) Info(args ...interface{})                 { log.Info("%s", args) }
func (g *grpcLogMabob) Infof(format string, args ...interface{}) { log.Info(format, args...) }
func (g *grpcLogMabob) Infoln(args ...interface{})               { log.Info("%s", args) }
func (g *grpcLogMabob) Warning(args ...interface{}) {
	if !g.handleEmptyRequest(args) {
		log.Warning("%s", args)
	}
}
func (g *grpcLogMabob) Warningf(format string, args ...interface{}) {
	if !g.handleEmptyRequest(args) {
		log.Warning(format, args...)
	}
}
func (g *grpcLogMabob) Warningln(args ...interface{}) {
	if !g.handleEmptyRequest(args) {
		log.Warning("%s", args)
	}
}
func (g *grpcLogMabob) Error(args ...interface{})                 { log.Error("%s", args) }
func (g *grpcLogMabob) Errorf(format string, args ...interface{}) { log.Errorf(format, args...) }
func (g *grpcLogMabob) Errorln(args ...interface{})               { log.Error("%s", args) }
func (g *grpcLogMabob) Fatal(args ...interface{})                 { log.Fatal(args...) }
func (g *grpcLogMabob) Fatalf(format string, args ...interface{}) { log.Fatalf(format, args...) }
func (g *grpcLogMabob) Fatalln(args ...interface{})               { log.Fatal(args...) }
func (g *grpcLogMabob) V(l int) bool                              { return log.IsEnabledFor(logging.Level(l)) }

// handleEmptyRequest lowers the priority of a gRPC warning triggered by a request that closes
// immediately; that makes it convenient to use for k8s healthchecks, for example.
func (*grpcLogMabob) handleEmptyRequest(args []interface{}) bool {
	if len(args) == 2 && args[0] == "grpc: Server.Serve failed to create ServerTransport: " {
		if err, ok := args[1].(error); ok {
			if err.Error() == `connection error: desc = "transport: http2Server.HandleStreams failed to receive the preface from client: EOF"` {
				log.Debug("%s %s", args[0], args[1])
				return true
			}
		}
	}
	return false
}

func init() {
	// Change grpc to log using our implementation
	grpclog.SetLoggerV2(&grpcLogMabob{})
}

// Silence quietens logging from gRPC.
func Silence() {
	logging.SetLevel(logging.WARNING, "grpcutil")
}

// LogUnaryRequests is a gRPC interceptor that logs outcomes of unary requests.
func LogUnaryRequests(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	if err != nil {
		if hasGRPCCode(err, codes.NotFound) {
			log.Debug("Not found on %s: %v", info.FullMethod, err)
		} else {
			log.Error("Error handling %s: %v", info.FullMethod, err)
		}
	} else {
		log.Debug("Handled %s successfully in %s", info.FullMethod, time.Since(start))
	}
	return resp, err
}

// LogStreamRequests is a gRPC interceptor that logs outcomes of stream requests.
func LogStreamRequests(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	err := handler(srv, ss)
	if err != nil {
		if hasGRPCCode(err, codes.NotFound) {
			log.Debug("Not found on %s: %v", info.FullMethod, err)
		} else {
			log.Error("Error handling %s: %v", info.FullMethod, err)
		}
	} else {
		log.Debug("Handled %s successfully in %s", info.FullMethod, time.Since(start))
	}
	return err
}

// grpcStatusError is the interface implemented by gRPC status errors.
type grpcStatusError interface {
	GRPCStatus() *status.Status
}

// hasGRPCCode reports whether err or any error in its tree has the given gRPC
// status code. This handles errors wrapped by multierror or fmt.Errorf("%w")
// where status.Code() alone would return codes.Unknown.
func hasGRPCCode(err error, code codes.Code) bool {
	if status.Code(err) == code {
		return true
	}
	var se grpcStatusError
	if errors.As(err, &se) {
		return se.GRPCStatus().Code() == code
	}
	return false
}
