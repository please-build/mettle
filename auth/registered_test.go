package auth

import (
	"testing"

	apb "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	rpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	rapb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"

	lpb "github.com/please-build/mettle/proto/lucidity"
	bpb "github.com/please-build/mettle/proto/mettle"
	ppb "github.com/please-build/mettle/proto/purity"
)

// Every service registered on any of our servers.
var registeredServices = []grpc.ServiceDesc{
	pb.Capabilities_ServiceDesc,
	pb.ActionCache_ServiceDesc,
	pb.ContentAddressableStorage_ServiceDesc,
	pb.Execution_ServiceDesc,
	apb.Fetch_ServiceDesc,
	// genproto doesn't export ByteStream's ServiceDesc
	{
		ServiceName: "google.bytestream.ByteStream",
		Methods:     []grpc.MethodDesc{{MethodName: "QueryWriteStatus"}},
		Streams:     []grpc.StreamDesc{{StreamName: "Read"}, {StreamName: "Write"}},
	},
	bpb.Bootstrap_ServiceDesc,
	ppb.GC_ServiceDesc,
	lpb.Lucidity_ServiceDesc,
	grpc_health_v1.Health_ServiceDesc,
	rpb.ServerReflection_ServiceDesc,
	rapb.ServerReflection_ServiceDesc,
}

func TestAllRegisteredMethodsHavePermissions(t *testing.T) {
	for _, svc := range registeredServices {
		for _, m := range svc.Methods {
			method := "/" + svc.ServiceName + "/" + m.MethodName
			assert.Contains(t, authenticatedMethods, method)
		}
		for _, s := range svc.Streams {
			method := "/" + svc.ServiceName + "/" + s.StreamName
			assert.Contains(t, authenticatedMethods, method)
		}
	}
}
