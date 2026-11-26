package grpcutil

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

const (
	getCapabilities    = "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities"
	fetchBlob          = "/build.bazel.remote.asset.v1.Fetch/FetchBlob"
	getActionResult    = "/build.bazel.remote.execution.v2.ActionCache/GetActionResult"
	updateActionResult = "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult"
	batchReadBlobs     = "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs"
	batchUpdateBlobs   = "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs"
	findMissingBlobs   = "/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs"
	getTree            = "/build.bazel.remote.execution.v2.ContentAddressableStorage/GetTree"
	execute            = "/build.bazel.remote.execution.v2.Execution/Execute"
	waitExecution      = "/build.bazel.remote.execution.v2.Execution/WaitExecution"
	serveExecutions    = "/build.please.remote.mettle.Bootstrap/ServeExecutions"
	gcList             = "/build.please.remote.purity.GC/List"
	gcDelete           = "/build.please.remote.purity.GC/Delete"
	lucidityUpdate     = "/build.please.remote.lucidity.Lucidity/Update"
	lucidityList       = "/build.please.remote.lucidity.Lucidity/ListWorkers"
	bsQueryWriteStatus = "/google.bytestream.ByteStream/QueryWriteStatus"
	bsRead             = "/google.bytestream.ByteStream/Read"
	bsWrite            = "/google.bytestream.ByteStream/Write"
	reflectionV1Alpha  = "/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo"
	reflectionV1       = "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"
	healthCheck        = "/grpc.health.v1.Health/Check"
)

// Methods that don't need a token when only --token_file is given.
var anonymousMethods = []string{
	getCapabilities,
	getActionResult,
	batchReadBlobs,
	findMissingBlobs,
	getTree,
	waitExecution,
	serveExecutions,
	bsRead,
	reflectionV1Alpha,
	reflectionV1,
	healthCheck,
}

// Methods that need a token when only --token_file is given.
var mutatingMethods = []string{
	fetchBlob,
	updateActionResult,
	batchUpdateBlobs,
	execute,
	gcList,
	gcDelete,
	lucidityUpdate,
	lucidityList,
	bsQueryWriteStatus,
	bsWrite,
}

func allMethods() []string {
	return append(append([]string{}, anonymousMethods...), mutatingMethods...)
}

func writeToken(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(path, []byte("s3cr3t\n"), 0644))
	return path
}

func withToken(token string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
}

func TestLoadAuthNoFlags(t *testing.T) {
	a := loadAuth(t.Context(), Opts{})
	for _, ctx := range []context.Context{
		t.Context(),
		withToken("s3cr3t"),
		metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "garbage")),
	} {
		for _, method := range allMethods() {
			assert.NoError(t, a.AuthRPC(ctx, method), method)
		}
		assert.NoError(t, a.AuthRPC(ctx, "/some.Unknown/Method"))
	}
}

func TestLoadAuthTokenFile(t *testing.T) {
	a := loadAuth(t.Context(), Opts{TokenFile: writeToken(t)})
	for _, method := range anonymousMethods {
		assert.NoError(t, a.AuthRPC(t.Context(), method), method)
	}
	for _, method := range mutatingMethods {
		assert.Error(t, a.AuthRPC(t.Context(), method), method)
		assert.Error(t, a.AuthRPC(withToken("wrong"), method), method)
	}
	for _, method := range allMethods() {
		assert.NoError(t, a.AuthRPC(withToken("s3cr3t"), method), method)
	}
}

func TestLoadAuthAuthAll(t *testing.T) {
	a := loadAuth(t.Context(), Opts{TokenFile: writeToken(t), AuthAll: true})
	for _, method := range allMethods() {
		assert.Error(t, a.AuthRPC(t.Context(), method), method)
		assert.Error(t, a.AuthRPC(withToken("wrong"), method), method)
		assert.NoError(t, a.AuthRPC(withToken("s3cr3t"), method), method)
	}
}

func TestLoadAuthConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := filepath.Join(dir, "auth.yaml")
	require.NoError(t, os.WriteFile(cfg, []byte(`
roles:
  reader:
    permissions:
      - cas:read
    principals:
      - anonymous
  writer:
    permissions:
      - cas:read
      - cas:write
    principals:
      - legacy
static:
  file: `+writeToken(t)), 0644))
	// --auth_config takes precedence over the legacy flags.
	a := loadAuth(t.Context(), Opts{AuthConfig: cfg, TokenFile: "/nonexistent", AuthAll: true})
	assert.NoError(t, a.AuthRPC(t.Context(), bsRead))
	assert.Error(t, a.AuthRPC(t.Context(), bsWrite))
	assert.Error(t, a.AuthRPC(t.Context(), getActionResult))
	assert.NoError(t, a.AuthRPC(withToken("s3cr3t"), bsWrite))
	assert.Error(t, a.AuthRPC(withToken("s3cr3t"), execute))
}
