// Package gc implements the garbage collection logic for Purity.
package gc

import (
	"context"
	"encoding/hex"
	"fmt"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/dustin/go-humanize"
	"github.com/hashicorp/go-multierror"
	"github.com/peterebden/go-cli-init/v4/logging"
	"github.com/sirupsen/logrus"

	ppb "github.com/please-build/mettle/proto/purity"
	"github.com/please-build/mettle/rexclient"
)

var log = logging.MustGetLogger()
var logr = logrus.New()

// We use eternity to indicate cases where we don't care about max blob age.
const eternity = 1000000 * time.Hour

// RunForever runs indefinitely, periodically hitting the remote server and possibly GC'ing it.
func RunForever(url, instanceName, tokenFile string, tls bool, minAge, frequency time.Duration, replicationFactor int) {
	for range time.NewTicker(frequency).C {
		if err := Run(url, instanceName, tokenFile, tls, minAge, replicationFactor, false); err != nil {
			log.Error("Failed to GC: %s", err)
		}
	}
}

// Run runs once against the remote servers and triggers a GC if needed.
func Run(url, instanceName, tokenFile string, tls bool, minAge time.Duration, replicationFactor int, dryRun bool) error {
	start := time.Now()
	gc, err := newCollector(url, instanceName, tokenFile, tls, dryRun, minAge)
	if err != nil {
		return err
	} else if err := gc.LoadAllBlobs(); err != nil {
		return err
	} else if err := gc.RemoveActionResults(); err != nil {
		return err
	} else if err := gc.MarkReferencedBlobs(); err != nil {
		return err
	} else if err := gc.RemoveBlobs(); err != nil {
		return err
	} else if err := gc.RemoveBrokenBlobs(); err != nil {
		return err
	} else if err := gc.ReplicateBlobs(replicationFactor); err != nil {
		return err
	}
	log.Notice("Complete in %s!", time.Since(start).Truncate(time.Second))
	return nil
}

// Delete deletes a series of build actions from the remote server.
func Delete(url, instanceName, tokenFile string, tls bool, actions []*pb.Digest) error {
	gc, err := newCollector(url, instanceName, tokenFile, tls, false, 0)
	if err != nil {
		return err
	}
	return gc.RemoveSpecificBlobs(actions)
}

// Clean cleans any build actions referencing missing blobs from the server.
func Clean(url, instanceName, tokenFile string, tls, dryRun bool) error {
	gc, err := newCollector(url, instanceName, tokenFile, tls, dryRun, eternity)
	if err != nil {
		return err
	} else if err := gc.LoadAllBlobs(); err != nil {
		return err
	}
	gc.MarkActionResults()
	if err := gc.MarkReferencedBlobs(); err != nil {
		return err
	}
	return gc.RemoveBrokenBlobs()
}

// Sizes returns the sizes of the top N actions.
func Sizes(url, instanceName, tokenFile string, tls bool, n int) ([]Action, error) {
	gc, err := newCollector(url, instanceName, tokenFile, tls, true, eternity)
	if err != nil {
		return nil, err
	} else if err := gc.LoadAllBlobs(); err != nil {
		return nil, err
	} else if err := gc.MarkReferencedBlobs(); err != nil {
		return nil, err
	}
	return gc.Sizes(n), nil
}

// Replicate re-replicates any underreplicated blobs.
func Replicate(url, instanceName, tokenFile string, tls bool, replicationFactor int, dryRun bool) error {
	gc, err := newCollector(url, instanceName, tokenFile, tls, dryRun, eternity)
	if err != nil {
		return err
	} else if err := gc.LoadAllBlobs(); err != nil {
		return err
	}
	return gc.ReplicateBlobs(replicationFactor)
}

// BlobUsage returns the a series of blobs for analysis of how much they're used.
func BlobUsage(url, instanceName, tokenFile string, tls bool) ([]Blob, error) {
	gc, err := newCollector(url, instanceName, tokenFile, tls, true, eternity)
	if err != nil {
		return nil, err
	} else if err := gc.LoadAllBlobs(); err != nil {
		return nil, err
	}
	return gc.BlobUsage()
}

// An Action is a convenience type returned from Sizes.
type Action struct {
	pb.Digest
	InputSize, OutputSize int
}

type collector struct {
	client            *client.Client
	gcclient          ppb.GCClient
	actionResults     []*ppb.ActionResult
	liveActionResults map[string]int64
	allBlobs          map[string]*ppb.Blob
	blobSizes         map[string]int64
	actionSizes       map[string]int64
	referencedBlobs   map[string]struct{}
	brokenResults     map[string]int64
	inputSizes        map[string]int
	outputSizes       map[string]int
	actionRFs         map[string]int
	blobRFs           map[string]int
	mutex             sync.Mutex
	ageThreshold      int64
	missingInputs     int64
	dryRun            bool
	parallelism       int
}

func newCollector(url, instanceName, tokenFile string, tls, dryRun bool, minAge time.Duration) (*collector, error) {
	client, err := rexclient.New(instanceName, url, tls, tokenFile)
	if err != nil {
		return nil, err
	}
	return &collector{
		client:            client,
		gcclient:          ppb.NewGCClient(client.Connection),
		dryRun:            dryRun,
		allBlobs:          map[string]*ppb.Blob{},
		blobSizes:         map[string]int64{},
		actionSizes:       map[string]int64{},
		liveActionResults: map[string]int64{},
		referencedBlobs: map[string]struct{}{
			digest.Empty.Hash: {}, // The empty blob always counts as referenced.
		},
		brokenResults: map[string]int64{},
		inputSizes:    map[string]int{},
		outputSizes:   map[string]int{},
		actionRFs:     map[string]int{},
		blobRFs:       map[string]int{},
		ageThreshold:  time.Now().Add(-minAge).Unix(),
		parallelism:   16,
	}, nil
}

func (c *collector) LoadAllBlobs() error {
	log.Notice("Receiving current list of items...")
	ch := newProgressBar("Enumerating blobs", 256)
	defer func() {
		close(ch)
		time.Sleep(10 * time.Millisecond) // obviously yuck but helps the progress bar get out of the way
		log.Notice("Received %d action results and %d blobs", len(c.actionResults), len(c.allBlobs))
	}()
	var g multierror.Group
	var mutex sync.Mutex
	for i := 0; i < 16; i++ {
		g.Go(func() error {
			for j := 0; j < 16; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
				defer cancel()
				resp, err := c.gcclient.List(ctx, &ppb.ListRequest{
					Prefix: hex.EncodeToString([]byte{byte(i*16 + j)}),
				})
				if err != nil {
					return err
				}
				mutex.Lock()
				c.actionResults = append(c.actionResults, resp.ActionResults...)
				for _, ar := range resp.ActionResults {
					c.actionRFs[ar.Hash] = int(ar.Replicas)
					c.actionSizes[ar.Hash] = ar.SizeBytes
				}
				for _, b := range resp.Blobs {
					c.allBlobs[b.Hash] = b
					c.blobSizes[b.Hash] = b.SizeBytes
					c.blobRFs[b.Hash] = int(b.Replicas)
				}
				mutex.Unlock()
				ch <- 1
			}
			return nil
		})
	}
	return g.Wait().ErrorOrNil()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MarkReferencedBlobs traverses the internal list of "live" action results
// and for each of them marks all the blobs they point to (either directly or
// indirectly) as "referenced" by adding their digest to an internal map. See
// `RemoveBlobs` on how this map is then used.
// Any missing output blob will get the action result marked as "broken" and
// none of the blobs will be marked as referenced. On the other hand, missing
// input blobs are non-fatal for 2 reasons:
//   - since the action has already been run, we no longer need them (at least
//     in the case of that specific action). The only thing that will be used
//     by the mettle is the output blobs;
//   - input blobs of an action are output blobs of another, so following the
//     statement on output blobs above, if any input blob is missing, the action
//     that was supposed to produce those will be marked as broken (and removed)
//     anyway.
func (c *collector) MarkReferencedBlobs() error {
	log.Notice("Finding referenced blobs...")
	ch := newProgressBar("Checking action results", len(c.actionResults))
	var live int64
	defer func() {
		close(ch)
		time.Sleep(10 * time.Millisecond)
		log.Notice("Found %d live action results and %d referenced blobs", live, len(c.referencedBlobs))
		if c.missingInputs > 0 {
			log.Warning("Missing inputs for %d action results", c.missingInputs)
		}
	}()
	var wg sync.WaitGroup
	// Loop one extra time to catch the remaining ars as the step size is rounded down
	wg.Add(c.parallelism + 1)
	step := len(c.actionResults) / c.parallelism
	for i := 0; i < (c.parallelism + 1); i++ {
		go func(ars []*ppb.ActionResult) {
			for _, ar := range ars {
				if _, present := c.liveActionResults[ar.Hash]; present {
					if err := c.markReferencedBlobs(ar); err != nil {
						// Not fatal otherwise one bad action result will stop the whole show.
						logr.WithFields(logrus.Fields{
							"hash": ar.Hash,
						}).WithError(err).Debug("Failed to find referenced blobs")
						c.markBroken(ar.Hash, ar.SizeBytes)
					}
					atomic.AddInt64(&live, 1)
				}
				ch <- 1
			}
			wg.Done()
		}(c.actionResults[step*i : min(step*(i+1), len(c.actionResults))])
	}
	wg.Wait()
	return nil
}

// markBroken marks an action result as missing some relevant files.
func (c *collector) markBroken(hash string, size int64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.brokenResults[hash] = size
}

func (c *collector) MarkActionResults() {
	for _, ar := range c.actionResults {
		if !c.shouldDelete(ar) {
			c.liveActionResults[ar.Hash] = ar.SizeBytes
		}
	}
}

// RemoveActionResults removes ARs that should be deleted or marks them live.
func (c *collector) RemoveActionResults() error {
	log.Notice("Determining action results to remove...")
	ars := []*ppb.Blob{}
	numArs := 0
	var totalSize int64
	for _, ar := range c.actionResults {
		if c.shouldDelete(ar) {
			logr.WithFields(logrus.Fields{
				"hash": ar.Hash,
			}).Debug("Identified action result for deletion")
			ars = append(ars, &ppb.Blob{Hash: ar.Hash, SizeBytes: ar.SizeBytes, CachePrefix: ar.CachePrefix})
			totalSize += ar.SizeBytes
			numArs++
		} else {
			c.liveActionResults[ar.Hash] = ar.SizeBytes
		}
	}
	if c.dryRun {
		log.Notice("Would delete %d action results, total size %s", numArs, humanize.Bytes(uint64(totalSize)))
		return nil
	}
	log.Notice("Deleting %d action results, total size %s", numArs, humanize.Bytes(uint64(totalSize)))
	ch := newProgressBar("Deleting action results", len(ars))
	defer func() {
		close(ch)
		time.Sleep(10 * time.Millisecond)
	}()
	var wg sync.WaitGroup
	wg.Add(c.parallelism + 1)
	step := numArs / c.parallelism
	for i := 0; i < (c.parallelism + 1); i++ {
		go func(ars []*ppb.Blob) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
			defer cancel()
			for _, ar := range ars {
				logr.WithFields(logrus.Fields{
					"hash": ar.Hash,
				}).Debug("Removing action result")
				if _, err := c.gcclient.Delete(ctx, &ppb.DeleteRequest{
					Prefix:        ar.Hash[:2],
					ActionResults: []*ppb.Blob{ar},
					Hard:          true,
				}); err != nil {
					logr.WithFields(logrus.Fields{
						"hash":   ar.Hash,
						"prefix": ar.CachePrefix,
					}).WithError(err).Warn("Failed to delete action result marking as live")
					c.mutex.Lock()
					c.liveActionResults[ar.Hash] = ar.SizeBytes
					c.mutex.Unlock()
				} else {
					logr.WithFields(logrus.Fields{
						"hash": ar.Hash,
					}).Debug("Deleted action result")
					c.mutex.Lock()
					delete(c.actionRFs, ar.Hash)
					c.mutex.Unlock()
				}
				ch <- 1
			}
			wg.Done()
		}(ars[step*i : min(step*(i+1), numArs)])
	}
	wg.Wait()
	return nil
}

// RemoveBlobs goes through the list of blobs and tells mettle
// to remove any that is not referenced by a live action result
// (see `MarkReferencedBlobs` for this process).
func (c *collector) RemoveBlobs() error {
	log.Notice("Determining blobs to remove...")
	blobs := make(map[string][]*ppb.Blob)
	numBlobs := 0
	var totalSize int64
	for hash, blob := range c.allBlobs {
		if _, present := c.referencedBlobs[hash]; !present {
			logr.WithFields(logrus.Fields{
				"hash": hash,
			}).Debug("Identified blob for deletion")
			key := blob.Hash[:2]
			blobs[key] = append(blobs[key], blob)
			delete(c.blobRFs, hash)
			totalSize += blob.SizeBytes
			numBlobs++
		}
	}
	if c.dryRun {
		log.Notice("Would delete %d blobs, total size %s", numBlobs, humanize.Bytes(uint64(totalSize)))
		return nil
	}
	log.Notice("Deleting %d blobs, total size %s", numBlobs, humanize.Bytes(uint64(totalSize)))
	ch := newProgressBar("Deleting blobs", 256)
	defer func() {
		close(ch)
		time.Sleep(10 * time.Millisecond)
	}()
	var wg sync.WaitGroup
	wg.Add(len(blobs))
	for k, v := range blobs {
		go func(prefix string, blobs []*ppb.Blob) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Hour)
			defer cancel()
			_, err := c.gcclient.Delete(ctx, &ppb.DeleteRequest{
				Prefix: prefix,
				Blobs:  blobs,
				Hard:   true,
			})
			if err != nil {
				log.Warning("Failed to delete blobs: %v", err)
			}
			ch <- 1
			wg.Done()
		}(k, v)
	}
	wg.Wait()
	return nil
}

func (c *collector) shouldDelete(ar *ppb.ActionResult) bool {
	return ar.LastAccessed < c.ageThreshold || len(ar.Hash) != 64
}

// RemoveSpecificBlobs removes blobs from the cache. It's best effort and returns a multierror or nil
func (c *collector) RemoveSpecificBlobs(digests []*pb.Digest) error {
	for _, d := range digests {
		delete(c.actionRFs, d.Hash)
	}
	if c.dryRun {
		log.Notice("Would remove %d actions:", len(digests))
		for _, h := range digests {
			log.Info("Would remove action %s", h)
		}
		return nil
	} else if len(digests) == 0 {
		log.Notice("Nothing to do")
		return nil
	}
	ch := newProgressBar("Deleting actions", len(digests))
	defer func() {
		close(ch)
		time.Sleep(10 * time.Millisecond)
		log.Notice("Deleted %d action results", len(digests))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	var merr *multierror.Error
	for _, digest := range digests {
		cachePrefix := fmt.Sprintf("ac/%s/", digest.Hash[:2])
		logr.WithFields(logrus.Fields{
			"hash":  digest.Hash,
			"cache": cachePrefix,
		}).Debug("Removing action result")
		if _, err := c.gcclient.Delete(ctx, &ppb.DeleteRequest{
			Prefix:        digest.Hash[:2],
			ActionResults: []*ppb.Blob{{Hash: digest.Hash, SizeBytes: digest.SizeBytes, CachePrefix: cachePrefix}},
			Hard:          true,
		}); err != nil {
			merr = multierror.Append(merr, err)
		}
		ch <- 1
	}
	return merr.ErrorOrNil()
}

// RemoveBrokenBlobs removes any blobs previously marked as broken.
func (c *collector) RemoveBrokenBlobs() error {
	digests := make([]*pb.Digest, 0, len(c.brokenResults))
	for h, s := range c.brokenResults {
		digests = append(digests, &pb.Digest{Hash: h, SizeBytes: s})
	}
	return c.RemoveSpecificBlobs(digests)
}

// Sizes returns the sizes of the top n biggest actions.
func (c *collector) Sizes(n int) []Action {
	ret := make([]Action, len(c.actionResults))
	for i, ar := range c.actionResults {
		ret[i] = Action{
			Digest: pb.Digest{
				Hash:      ar.Hash,
				SizeBytes: c.allBlobs[ar.Hash].SizeBytes,
			},
			InputSize:  c.inputSizes[ar.Hash],
			OutputSize: c.outputSizes[ar.Hash],
		}
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].InputSize+ret[i].OutputSize > ret[j].InputSize+ret[j].OutputSize
	})
	if len(ret) > n {
		return ret[:n]
	}
	return ret
}

// ReplicateBlobs re-replicates any blobs with a replication factor lower than expected.
func (c *collector) ReplicateBlobs(rf int) error {
	blobs := c.underreplicatedDigests(c.blobRFs, c.blobSizes, rf)
	ars := c.underreplicatedDigests(c.actionRFs, c.actionSizes, rf)
	if err := c.replicateBlobs("blobs", blobs, func(dg *pb.Digest) error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		blob, _, err := c.client.ReadBlob(ctx, digest.NewFromProtoUnvalidated(dg))
		if err != nil {
			return err
		}
		_, err = c.client.WriteBlob(ctx, blob)
		return err
	}); err != nil {
		return err
	}
	return c.replicateBlobs("action results", ars, func(dg *pb.Digest) error {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()
		ar, err := c.client.GetActionResult(ctx, &pb.GetActionResultRequest{
			InstanceName: c.client.InstanceName,
			ActionDigest: dg,
		})
		if err != nil {
			return err
		}
		_, err = c.client.UpdateActionResult(ctx, &pb.UpdateActionResultRequest{
			InstanceName: c.client.InstanceName,
			ActionDigest: dg,
			ActionResult: ar,
		})
		return err
	})
}

func (c *collector) replicateBlobs(name string, blobs []*pb.Digest, f func(*pb.Digest) error) error {
	if len(blobs) == 0 {
		log.Notice("No underreplicated %s found!", name)
		return nil
	}
	var size int64
	for _, blob := range blobs {
		size += blob.SizeBytes
	}
	log.Notice("Found %d underreplicated %s, total size %s", len(blobs), name, humanize.Bytes(uint64(size)))
	if c.dryRun {
		return nil
	}
	ch := newProgressBar("Replicating "+name, len(blobs))
	defer func() {
		close(ch)
		time.Sleep(10 * time.Millisecond)
		log.Notice("Replicated %d %s", len(blobs), name)
	}()
	var me *multierror.Error
	for _, b := range blobs {
		if err := f(b); err != nil {
			me = multierror.Append(me, err)
		}
		ch <- 1
	}
	return me.ErrorOrNil()
}

func (c *collector) underreplicatedDigests(blobs map[string]int, sizes map[string]int64, rf int) []*pb.Digest {
	ret := []*pb.Digest{}
	for hash, replicas := range blobs {
		if replicas < rf {
			ret = append(ret, &pb.Digest{
				Hash:      hash,
				SizeBytes: sizes[hash],
			})
		}
	}
	return ret
}

func (c *collector) BlobUsage() ([]Blob, error) {
	blobs := map[string]*Blob{}
	var mutex sync.Mutex

	markBlob := func(dg *pb.Digest, filename string) {
		mutex.Lock()
		defer mutex.Unlock()
		if blob, present := blobs[dg.Hash]; present {
			blob.Count++
		} else {
			blobs[dg.Hash] = &Blob{
				Hash:      dg.Hash,
				SizeBytes: dg.SizeBytes,
				Count:     1,
				Filename:  filename,
			}
		}
	}
	var markBlobs func(m map[string]*pb.Directory, digest *pb.Digest, root string)
	markBlobs = func(m map[string]*pb.Directory, digest *pb.Digest, root string) {
		if digest == nil {
			return
		}
		dir, present := m[digest.Hash]
		if !present {
			logr.WithFields(logrus.Fields{
				"hash": digest.Hash,
			}).Error("Failed to find input directory with hash")
			return
		}
		for _, file := range dir.Files {
			markBlob(file.Digest, path.Join(root, file.Name))
		}
		for _, dir := range dir.Directories {
			markBlobs(m, dir.Digest, path.Join(root, dir.Name))
		}
	}

	log.Notice("Finding all input blobs...")
	ch := newProgressBar("Searching input actions", len(c.actionResults))
	defer func() {
		close(ch)
		time.Sleep(10 * time.Millisecond)
	}()
	var wg sync.WaitGroup
	// Loop one extra time to catch the remaining ars as the step size is rounded down
	wg.Add(c.parallelism + 1)
	step := len(c.actionResults) / c.parallelism
	for i := 0; i < (c.parallelism + 1); i++ {
		go func(ars []*ppb.ActionResult) {
			for _, ar := range ars {
				action, dirs, err := c.inputDirs(ar)
				if err != nil {
					logr.WithFields(logrus.Fields{
						"hash": ar.Hash,
					}).WithError(err).Error("failed to get input dir")
				} else {
					markBlobs(c.inputDirMap(dirs), action.InputRootDigest, "")
				}
				ch <- 1
			}
			wg.Done()
		}(c.actionResults[step*i : min(step*(i+1), len(c.actionResults))])
	}
	wg.Wait()
	ret := make([]Blob, 0, len(blobs))
	for _, blob := range blobs {
		ret = append(ret, *blob)
	}
	return ret, nil
}

func (c *collector) inputDirs(ar *ppb.ActionResult) (*pb.Action, []*pb.Directory, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	action := &pb.Action{}
	blob, present := c.allBlobs[ar.Hash]
	if !present {
		return nil, nil, fmt.Errorf("missing action for %s", ar.Hash)
	}
	if _, err := c.client.ReadProto(ctx, digest.Digest{
		Hash: ar.Hash,
		Size: blob.SizeBytes,
	}, action); err != nil {
		return nil, nil, fmt.Errorf("Failed to read action %s: %w", ar.Hash, err)
	}
	if action.InputRootDigest == nil {
		return nil, nil, fmt.Errorf("nil input root for %s", ar.Hash)
	}
	dirs, err := c.client.GetDirectoryTree(ctx, action.InputRootDigest)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to read directory tree for %s (input root %s): %w", ar.Hash, action.InputRootDigest, err)
	}
	return action, dirs, nil
}

func (c *collector) inputDirMap(dirs []*pb.Directory) map[string]*pb.Directory {
	m := map[string]*pb.Directory{}
	for _, dir := range dirs {
		dg, _ := digest.NewFromMessage(dir)
		m[dg.Hash] = dir
	}
	return m
}

// A Blob is a representation of a blob with its usage.
type Blob struct {
	Filename  string
	Hash      string
	SizeBytes int64
	Count     int64
}
