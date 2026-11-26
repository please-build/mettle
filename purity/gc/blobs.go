package gc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ppb "github.com/please-build/mettle/proto/purity"
	"github.com/please-build/mettle/rexclient"
)

// markReferencedBlobs takes an action result and adds all blobs on which this
// action result depends (either directly or indirectly) to the
// `referencedBlobs` map. To do so, it has to traverse the whole tree of blobs
// at which this action result point and collect the various blob digests.
// Missing input blobs are non-fatal, but missing output blobs will return an
// error and will not mark any blob as referenced.
func (c *collector) markReferencedBlobs(ctx context.Context, ar *ppb.ActionResult) error {
	outputBlobs, err := c.outputBlobs(ctx, ar)
	if err != nil {
		return err
	}

	// Mark all the inputs as well. There are some fringe cases that make things awkward
	// and it means things look more sensible in the browser.
	inputBlobs := c.inputBlobs(ctx, ar)

	c.mutex.Lock()
	defer c.mutex.Unlock()
	logr.WithFields(logrus.Fields{
		"hash":       ar.Hash,
		"inputBlobs": inputBlobs,
	}).Debug("marking input blobs")
	var inputSize int64
	for _, b := range inputBlobs {
		c.referencedBlobs[b.Hash] = struct{}{}
		inputSize += b.SizeBytes
	}
	c.inputSizes[ar.Hash] = int(inputSize)
	logr.WithFields(logrus.Fields{
		"hash":        ar.Hash,
		"outputBlobs": outputBlobs,
	}).Debug("marking output blobs")
	var outputSize int64
	for _, b := range outputBlobs {
		c.referencedBlobs[b.Hash] = struct{}{}
		outputSize += b.SizeBytes
	}
	c.outputSizes[ar.Hash] = int(outputSize)
	c.referencedBlobs[ar.Hash] = struct{}{}
	return nil
}

// inputBlobs returns all the inputs for an action. It doesn't return any errors because we don't
// want it to be fatal on failure; otherwise anything missing breaks the whole process.
func (c *collector) inputBlobs(ctx context.Context, ar *ppb.ActionResult) []*pb.Digest {
	dg := &pb.Digest{Hash: ar.Hash, SizeBytes: ar.SizeBytes}
	action := &pb.Action{}
	// Actions are stored separately from ARs. They are located in the CAS
	// like any other blob with the same digest as the AR.
	blob, present := c.allBlobs[dg.Hash]
	if !present {
		logr.WithFields(logrus.Fields{
			"hash": dg.Hash,
		}).Error("missing action")
		atomic.AddInt64(&c.missingInputs, 1)
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()
	if _, err := c.client.ReadProto(ctx, digest.Digest{
		Hash: dg.Hash,
		Size: blob.SizeBytes,
	}, action); err != nil {
		logr.WithFields(logrus.Fields{
			"hash": dg.Hash,
		}).WithError(err).Error("Failed to read action")
		atomic.AddInt64(&c.missingInputs, 1)
		return nil
	}

	digests, err := c.blobsForAction(ctx, action)
	if err != nil {
		logr.WithFields(logrus.Fields{
			"hash": dg.Hash,
		}).WithError(err).Error("Failed to get blobs for action")
		atomic.AddInt64(&c.missingInputs, 1)
		return nil
	}
	return append(digests, dg)
}

// blobsForAction returns the list of blob digests on which the given action
// "depends" (directly and indirectly).
// An action can point to the following blobs:
// - a command
// - an input root (which contains the directory tree for inputs)
func (c *collector) blobsForAction(ctx context.Context, action *pb.Action) ([]*pb.Digest, error) {
	digests := make([]*pb.Digest, 0, 2)
	if action.InputRootDigest == nil {
		return nil, errors.New("nil input root")
	}
	digests = append(digests, action.InputRootDigest)
	if action.CommandDigest != nil {
		digests = append(digests, action.CommandDigest)
	}

	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()
	dirs, err := c.client.GetDirectoryTree(ctx, action.InputRootDigest)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory tree for input root %s: %v", action.InputRootDigest, err)
	}
	for _, dir := range dirs {
		digests = append(digests, c.blobsForDirectory(dir)...)
	}
	return digests, nil
}

// outputBlobs collects the list of output blobs from the given action result.
// It also checks those are not missing and returns an error if some are.
func (c *collector) outputBlobs(ctx context.Context, ar *ppb.ActionResult) ([]*pb.Digest, error) {
	dg := &pb.Digest{Hash: ar.Hash, SizeBytes: ar.SizeBytes}
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()
	result, err := c.client.GetActionResult(ctx, &pb.GetActionResultRequest{
		InstanceName: c.client.InstanceName,
		ActionDigest: dg,
	})
	if err != nil {
		return nil, fmt.Errorf("Couldn't download action result for %s: %s", ar.Hash, err)
	}
	if result.ExitCode != 0 {
		return nil, fmt.Errorf("Found failed action result %s: exit code %d", ar.Hash, result.ExitCode)
	}

	outputBlobs, err := c.blobsForActionResult(ctx, result)
	if err != nil {
		return nil, err
	}

	// Check whether all these outputs exist.
	resp, err := c.client.FindMissingBlobs(ctx, &pb.FindMissingBlobsRequest{
		InstanceName: c.client.InstanceName,
		BlobDigests:  outputBlobs,
	})
	if err != nil {
		logr.WithFields(logrus.Fields{
			"hash": ar.Hash,
		}).WithError(err).Warn("Failed to check blob digests")
	}
	if resp != nil && len(resp.MissingBlobDigests) > 0 {
		digests := make([]string, len(resp.MissingBlobDigests))
		for i, dg := range resp.MissingBlobDigests {
			digests[i] = fmt.Sprintf("%s/%d", dg.Hash, dg.SizeBytes)
		}
		return nil, fmt.Errorf("Action result is missing %d output blobs: %s", len(resp.MissingBlobDigests), strings.Join(digests, ", "))
	}
	return outputBlobs, nil
}

// blobsForActionResult returns the list of blobs on which the given action
// result depends (directly and indirectly).
// An action result can point to the following blobs:
// - blobs with output directories and files
// - a blob containing the standard output of the action
// - a blob containing the standard error of the action
func (c *collector) blobsForActionResult(ctx context.Context, ar *pb.ActionResult) ([]*pb.Digest, error) {
	digests := make([]*pb.Digest, 0, len(ar.OutputDirectories))
	for _, d := range ar.OutputDirectories {
		dgs, err := c.blobsForOutputDir(ctx, d)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return nil, err
			}
		}
		digests = append(digests, dgs...)
	}
	for _, f := range ar.OutputFiles {
		digests = append(digests, f.Digest)
	}
	if ar.StdoutDigest != nil {
		digests = append(digests, ar.StdoutDigest)
	}
	if ar.StderrDigest != nil {
		digests = append(digests, ar.StderrDigest)
	}
	return digests, nil
}

// blobsForOutputDir returns the list of blob digests on which an output
// directory depends (directly and indirectly).
// An output directory only points to a tree, which in turns points to more
// blobs (see blobsForTree).
func (c *collector) blobsForOutputDir(ctx context.Context, d *pb.OutputDirectory) ([]*pb.Digest, error) {
	tree := &pb.Tree{}
	if _, err := c.client.ReadProto(ctx, digest.NewFromProtoUnvalidated(d.TreeDigest), tree); err != nil {
		return nil, err
	}

	return append(c.blobsForTree(ctx, tree), d.TreeDigest), nil
}

// blobsForTree returns the list of blob digests on which a tree depends
// (directly and indirectly).
// A tree points to the root directory blob, as well as child directory blobs.
// For each of those we first check whether they exist. If not, we try to
// repair (see checkDirectories).
func (c *collector) blobsForTree(ctx context.Context, tree *pb.Tree) []*pb.Digest {
	// Here we attempt to fix up any outputs that don't also have the input facet.
	// This is an incredibly sucky part of the API; the output doesn't contain some of the blobs
	// that will get used on the input facet, so it's really nonobvious where they come from.
	c.checkDirectories(ctx, append(tree.Children, tree.Root))

	digests := c.blobsForDirectory(tree.Root)
	for _, child := range tree.Children {
		digests = append(digests, c.blobsForDirectory(child)...)
	}
	return digests
}

// checkDirectory checks that the directory protos from a Tree still exist in the CAS and uploads it if not.
func (c *collector) checkDirectories(ctx context.Context, dirs []*pb.Directory) {
	entries := make([]*uploadinfo.Entry, len(dirs))
	for i, d := range dirs {
		e, _ := uploadinfo.EntryFromProto(d)
		entries[i] = e
	}
	if !c.dryRun {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
		defer cancel()
		if _, _, err := c.client.UploadIfMissing(ctx, entries...); err != nil {
			log.Warning("Failed to upload missing directory protos: %s", err)
		}
	}
}

// blobsForDirectory returns the list of blob digests on which a directory
// depends (directly and indirectly).
// A directory consists of file and sub-directory blobs. It can also point to
// a mettle pack, which we include in this list.
func (c *collector) blobsForDirectory(d *pb.Directory) []*pb.Digest {
	// If the dir has any node properties, save a copy of one that doesn't.
	cp := d
	if d.NodeProperties != nil {
		cp = proto.Clone(d).(*pb.Directory)
		cp.NodeProperties = nil
	}
	ue, _ := uploadinfo.EntryFromProto(cp)
	digests := []*pb.Digest{ue.Digest.ToProto()}
	for _, f := range d.Files {
		digests = append(digests, f.Digest)
	}
	for _, d := range d.Directories {
		digests = append(digests, d.Digest)
	}
	// If this directory has a pack associated, mark that too.
	if pack := rexclient.PackDigest(d); pack.Hash != "" {
		digests = append(digests, pack.ToProto())
	}
	return digests
}
