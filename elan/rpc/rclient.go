package rpc

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	hpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/thought-machine/please-servers/rexclient"
)

var rpcLatencies = prometheus.NewSummaryVec(prometheus.SummaryOpts{
	Namespace: "elan_remote_rpc",
	Name:      "rpc_latency_seconds",
}, []string{"rpc_method"})

func init() {
	prometheus.MustRegister(rpcLatencies)
}

// This is the implementation backed by the SDK client. It's pretty simple since it was what we
// were using before and the interface mostly mimics that.
type remoteClient struct {
	c      *client.Client
	health hpb.HealthClient
}

func (r *remoteClient) Healthcheck(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if resp, err := r.health.Check(ctx, &hpb.HealthCheckRequest{}); err != nil {
		return err
	} else if resp.Status != hpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("Server not in healthy state: %s", resp.Status)
	}
	return nil
}

func (r *remoteClient) ReadBlob(ctx context.Context, dg *pb.Digest) ([]byte, error) {
	defer observeTime(time.Now(), "ReadBlob")
	ctx, cnx := context.WithTimeout(ctx, time.Minute*2)
	defer cnx()
	b, _, err := r.c.ReadBlob(ctx, digest.NewFromProtoUnvalidated(dg))
	return b, err
}

func (r *remoteClient) WriteBlob(ctx context.Context, b []byte) (*pb.Digest, error) {
	defer observeTime(time.Now(), "WriteBlob")
	ctx, cnx := context.WithTimeout(ctx, time.Minute*2)
	defer cnx()
	dg, err := r.c.WriteBlob(ctx, b)
	return dg.ToProto(), err
}

func (r *remoteClient) UpdateActionResult(ctx context.Context, req *pb.UpdateActionResultRequest) (*pb.ActionResult, error) {
	defer observeTime(time.Now(), "UpdateActionResult")
	ctx, cnx := context.WithTimeout(ctx, time.Minute*2)
	defer cnx()
	return r.c.UpdateActionResult(ctx, req)
}

func (r *remoteClient) UploadIfMissing(ctx context.Context, entries []*uploadinfo.Entry, compressors []pb.Compressor_Value) error {
	defer observeTime(time.Now(), "UploadIfMissing")
	ctx, cnx := context.WithTimeout(ctx, time.Minute*10)
	defer cnx()
	_, _, err := r.c.UploadIfMissing(ctx, entries...)
	return err
}

func (r *remoteClient) BatchDownload(ctx context.Context, digests []digest.Digest) (map[digest.Digest][]byte, error) {
	defer observeTime(time.Now(), "BatchDownload")
	ctx, cnx := context.WithTimeout(ctx, time.Minute*5)
	defer cnx()
	return r.c.BatchDownloadBlobs(ctx, digests)
}

func (r *remoteClient) ReadToFile(ctx context.Context, dg digest.Digest, filename string, compressed bool) error {
	defer observeTime(time.Now(), "ReadToFile")
	ctx, cnx := context.WithTimeout(ctx, time.Minute*2)
	defer cnx()
	if !compressed {
		_, err := r.c.ReadBlobToFile(ctx, dg, filename)
		return err
	}
	_, err := r.c.ReadBlobToFile(ctx, dg, filename)
	return err
}

func (r *remoteClient) GetDirectoryTree(ctx context.Context, dg *pb.Digest, stopAtPack bool) ([]*pb.Directory, error) {
	defer observeTime(time.Now(), "GetDirectoryTree")
	ctx, cnx := context.WithTimeout(ctx, time.Minute*2)
	defer cnx()
	if stopAtPack {
		return r.c.GetDirectoryTree(rexclient.StopAtPack(ctx), dg)
	}
	return r.c.GetDirectoryTree(ctx, dg)
}

func observeTime(start time.Time, rpcMethod string) {
	total := time.Since(start)
	rpcLatencies.WithLabelValues(rpcMethod).Observe(total.Seconds())
}
