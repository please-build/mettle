package worker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/command"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thought-machine/please-servers/rexclient"
)

func TestSetRootDirectoryDigests(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "out/sub"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "out/a.txt"), []byte("a"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "out/sub/b.txt"), []byte("b"), 0644))

	entries, ar, err := rexclient.Uninitialised().ComputeOutputsToUpload(dir, ".", []string{"out"}, filemetadata.NewNoopCache(), command.PreserveSymlink)
	require.NoError(t, err)
	require.Len(t, ar.OutputDirectories, 1)
	setRootDirectoryDigests(ar.OutputDirectories, entries)
	outDir := ar.OutputDirectories[0]
	require.NotNil(t, outDir.RootDirectoryDigest)

	// The root digest should be that of the Tree's root, which carries the pack property.
	tree := &pb.Tree{}
	require.NoError(t, proto.Unmarshal(blob(t, entries, outDir.TreeDigest), tree))
	rootDigest, err := digest.NewFromMessage(tree.Root)
	require.NoError(t, err)
	assert.Equal(t, rootDigest.ToProto(), outDir.RootDirectoryDigest)
	packDigest := rexclient.PackDigest(tree.Root)
	assert.NotEqual(t, digest.Digest{}, packDigest)
	assert.Contains(t, entries, packDigest, "pack should be uploaded")

	// And we should be able to walk the whole directory from it via uploaded Directory blobs.
	root := &pb.Directory{}
	require.NoError(t, proto.Unmarshal(blob(t, entries, outDir.RootDirectoryDigest), root))
	require.Len(t, root.Files, 1)
	assert.Equal(t, "a.txt", root.Files[0].Name)
	require.Len(t, root.Directories, 1)
	assert.Equal(t, "sub", root.Directories[0].Name)
	sub := &pb.Directory{}
	require.NoError(t, proto.Unmarshal(blob(t, entries, root.Directories[0].Digest), sub))
	require.Len(t, sub.Files, 1)
	assert.Equal(t, "b.txt", sub.Files[0].Name)
	assert.Empty(t, sub.Directories)
}

func TestSetRootDirectoryDigestsMissingTree(t *testing.T) {
	dirs := []*pb.OutputDirectory{{Path: "out", TreeDigest: digest.NewFromBlob([]byte("not uploaded")).ToProto()}}
	setRootDirectoryDigests(dirs, map[digest.Digest]*uploadinfo.Entry{})
	assert.Nil(t, dirs[0].RootDirectoryDigest)
}

func blob(t *testing.T, entries map[digest.Digest]*uploadinfo.Entry, dg *pb.Digest) []byte {
	t.Helper()
	entry, present := entries[digest.NewFromProtoUnvalidated(dg)]
	require.True(t, present, "blob %s not uploaded", dg.Hash)
	require.True(t, entry.IsBlob())
	return entry.Contents
}

func TestAllOutputSymlinks(t *testing.T) {
	file := &pb.OutputSymlink{Path: "file_link", Target: "file"}
	dir := &pb.OutputSymlink{Path: "dir_link", Target: "dir"}

	// No symlinks must stay nil so the serialised ActionResult doesn't change.
	assert.Nil(t, allOutputSymlinks(&pb.ActionResult{}))

	// Only the deprecated fields are populated
	assert.Equal(t, []*pb.OutputSymlink{file, dir}, allOutputSymlinks(&pb.ActionResult{
		OutputFileSymlinks:      []*pb.OutputSymlink{file},
		OutputDirectorySymlinks: []*pb.OutputSymlink{dir},
	}))

	// output_symlinks is already populated, which takes priority
	link := &pb.OutputSymlink{Path: "link", Target: "target"}
	assert.Equal(t, []*pb.OutputSymlink{link}, allOutputSymlinks(&pb.ActionResult{
		OutputSymlinks:          []*pb.OutputSymlink{link},
		OutputFileSymlinks:      []*pb.OutputSymlink{file},
		OutputDirectorySymlinks: []*pb.OutputSymlink{dir},
	}))
}
