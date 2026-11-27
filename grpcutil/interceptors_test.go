package grpcutil

import (
	"context"
	"os"
	"path/filepath"
	"slices"
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

// Methods requiring cas:read and cas:write respectively.
var (
	casReadMethods  = []string{batchReadBlobs, findMissingBlobs, getTree, bsRead}
	casWriteMethods = []string{batchUpdateBlobs, bsQueryWriteStatus, bsWrite}
)

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

func TestLoadAuth(t *testing.T) {
	const unknownMethod = "/some.Unknown/Method"
	everything := append(allMethods(), unknownMethod)
	cfg := filepath.Join(t.TempDir(), "auth.yaml")
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

	for name, tc := range map[string]struct {
		opts Opts
		// Methods each kind of caller is allowed to call; everything else must be rejected.
		anonymous, wrongToken, rightToken []string
	}{
		"no flags": {
			opts:       Opts{},
			anonymous:  everything,
			wrongToken: everything,
			rightToken: everything,
		},
		"token file": {
			opts:       Opts{TokenFile: writeToken(t)},
			anonymous:  anonymousMethods,
			rightToken: allMethods(),
		},
		"auth all": {
			opts:       Opts{TokenFile: writeToken(t), AuthAll: true},
			rightToken: allMethods(),
		},
		"config overrides legacy flags": {
			opts:       Opts{AuthConfig: cfg, TokenFile: "/nonexistent", AuthAll: true},
			anonymous:  casReadMethods,
			rightToken: append(append([]string{}, casReadMethods...), casWriteMethods...),
		},
	} {
		t.Run(name, func(t *testing.T) {
			a := loadAuth(t.Context(), tc.opts)
			for _, caller := range []struct {
				ctx     context.Context
				allowed []string
			}{
				{t.Context(), tc.anonymous},
				{withToken("wrong"), tc.wrongToken},
				{withToken("s3cr3t"), tc.rightToken},
			} {
				for _, method := range everything {
					if err := a.AuthRPC(caller.ctx, method); slices.Contains(caller.allowed, method) {
						assert.NoError(t, err, method)
					} else {
						assert.Error(t, err, method)
					}
				}
			}
		})
	}
}
