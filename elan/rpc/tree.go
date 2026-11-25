package rpc

import (
	"context"
	"sync"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/hashicorp/go-multierror"

	"github.com/please-build/mettle/rexclient"
)

// getTree returns a channel to iterate over all the directories recursively under the given one.
// The responses are not necessarily returned in any particular order.
func (s *server) getTree(digest *pb.Digest, stopAtPack bool) ([]*pb.Directory, error) {
	dir := &pb.Directory{}
	if digest.SizeBytes == 0 && digest.Hash == emptyHash {
		return []*pb.Directory{dir}, nil
	}
	if dir, present := s.dirCache.Get(digest.Hash); present {
		d := dir.([]*pb.Directory)
		dirCacheHits.Add(float64(len(d)))
		return d, nil
	}
	dirCacheMisses.Inc()
	if err := s.readBlobIntoMessage(context.Background(), "cas", digest, dir); err != nil {
		return nil, err
	}
	ret := []*pb.Directory{dir}
	if stopAtPack && rexclient.PackDigest(dir).Hash != "" {
		// We've found a pack here, don't need to go any further down.
		s.dirCache.Set(digest.Hash, ret, int64(len(ret)))
		return ret, nil
	}
	var g multierror.Group
	var mutex sync.Mutex
	for _, child := range dir.Directories {
		g.Go(func() error {
			dirs, err := s.getTree(child.Digest, stopAtPack)
			mutex.Lock()
			defer mutex.Unlock()
			ret = append(ret, dirs...)
			return err
		})
	}
	if err := g.Wait(); err != nil {
		return ret, err
	}
	s.dirCache.Set(digest.Hash, ret, int64(len(ret)))
	return ret, nil
}
