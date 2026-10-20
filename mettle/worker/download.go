package worker

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"os"
	"path"
	"strings"
	"time"

	sdkdigest "github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/golang/protobuf/proto"
	"github.com/klauspost/compress/zstd"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/thought-machine/please-servers/mettle/common"
	"github.com/thought-machine/please-servers/rexclient"
)

// maxBlobBatchSize is the maximum size of a single blob batch we'll ever request.
const maxBlobBatchSize = 4012000 // 4000 Kelly-Bootle standard units

// downloadParallelism is the maximum number of parallel downloads we'll allow.
const downloadParallelism = 4

// ioParallelism is the maximum number of parallel disk writes we'll allow.
const ioParallelism = 10

// zstdMagic is the magic number of zstd compressed data.
var zstdMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}

// emptyHash is the sha256 hash of the empty file.
// Technically checking the size is sufficient but we add this as well for general sanity in case something
// else lost the size for some reason (it will be more obvious to debug that mismatch than mysteriously empty files).
const emptyHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// A fileNode contains the minimal fields we need to write a pb.FileNode.
type fileNode struct {
	Name         string
	IsExecutable bool
}

// downloadDirectory downloads & writes out a single Directory proto and all its children.
func (w *worker) downloadDirectory(digest *pb.Digest, usePacks bool) error {
	ts1 := time.Now()
	dirs, err := w.client.GetDirectoryTree(digest, usePacks)
	if err != nil {
		return err
	}
	ts2 := time.Now()
	m := make(map[string]*pb.Directory, len(dirs))
	for _, dir := range dirs {
		m[digestProto(dir).Hash] = dir
	}
	files := map[sdkdigest.Digest][]fileNode{}
	packs := map[sdkdigest.Digest][]string{}
	if err := w.createDirectory(m, files, packs, w.dir, digest, usePacks); err != nil {
		return err
	}
	ts3 := time.Now()
	err = w.downloadAllFiles(files, packs)
	w.metadataFetch = ts2.Sub(ts1)
	w.dirCreation = ts3.Sub(ts2)
	w.fileDownload = time.Since(ts3)
	return err
}

// createDirectory creates a directory & all its children
func (w *worker) createDirectory(dirs map[string]*pb.Directory, files map[sdkdigest.Digest][]fileNode, packs map[sdkdigest.Digest][]string, root string, digest *pb.Digest, usePacks bool) error {
	if err := os.MkdirAll(root, os.ModeDir|0775); err != nil {
		return err
	}
	if digest.Hash == emptyHash {
		return nil // Nothing to be done.
	}
	dir, present := dirs[digest.Hash]
	if !present {
		return fmt.Errorf("Missing directory %s", digest.Hash)
	}
	if usePacks {
		if dg := rexclient.PackDigest(dir); dg.Hash != "" {
			logr.WithFields(logrus.Fields{
				"hash": dg.Hash,
				"root": root,
				"size": dg.Size,
			}).Debug("Replacing dir with pack digest")
			packs[dg] = append(packs[dg], root)
			return nil
		}
	}
	for _, file := range dir.Files {
		if err := common.CheckPath(file.Name); err != nil {
			return err
		} else if err := makeDirIfNeeded(root, file.Name); err != nil {
			return err
		}
		dg := sdkdigest.NewFromProtoUnvalidated(file.Digest)
		files[dg] = append(files[dg], fileNode{
			Name:         path.Join(root, file.Name),
			IsExecutable: file.IsExecutable},
		)
	}
	for _, dir := range dir.Directories {
		if err := common.CheckPath(dir.Name); err != nil {
			return err
		} else if err := w.createDirectory(dirs, files, packs, path.Join(root, dir.Name), dir.Digest, usePacks); err != nil {
			return err
		}
	}
	for _, sym := range dir.Symlinks {
		if err := common.CheckPath(sym.Name); err != nil {
			return err
		} else if err := makeDirIfNeeded(root, sym.Name); err != nil {
			return err
		}
		if err := os.Symlink(sym.Target, path.Join(root, sym.Name)); err != nil {
			return err
		}
	}
	return nil
}

// downloadAllFiles downloads all the files for a single build action.
func (w *worker) downloadAllFiles(files map[sdkdigest.Digest][]fileNode, packs map[sdkdigest.Digest][]string) error {
	var g errgroup.Group

	fileNodes := map[sdkdigest.Digest][]fileNode{}
	var totalSize int64
	for dg, filenames := range files {
		// Optimise out any empty files. The empty blob is surprisingly popular and obviously we always know what
		// it will contain, so save the RPCs.
		if dg.Size == 0 && dg.Hash == emptyHash {
			fns := filenames
			dg := dg
			g.Go(func() error {
				return w.writeFiles(fns, nil, dg)
			})
			continue
		}
		if w.fileCache != nil && w.fileCache.Retrieve(dg.Hash, filenames[0].Name, fileMode(filenames[0].IsExecutable)) {
			cacheHits.Inc()
			w.cachedBytes += dg.Size
			if err := w.linkAll(filenames); err != nil {
				return err
			}
			continue
		}
		if dg.Size > maxBlobBatchSize {
			// This blob is big enough that it must always be done on its own.
			w.downloadedBytes += dg.Size
			fns := filenames
			dg := dg
			g.Go(func() error { return w.downloadFile(fns, dg) })
			continue
		}
		// Check cache for this blob (we never cache blobs that are big enough not to be batchable)
		if blob, present := w.cache.Get(dg.Hash); present {
			cacheHits.Inc()
			w.cachedBytes += dg.Size
			fns := filenames
			dg := dg
			g.Go(func() error {
				return w.writeFiles(fns, blob.([]byte), dg)
			})
			continue
		}
		cacheMisses.Inc()
		if totalSize+dg.Size > maxBlobBatchSize {
			// This blob on its own is OK but will exceed the total.
			// Download what we have so far then deal with it.
			fns := fileNodes
			g.Go(func() error { return w.downloadFiles(fns) })
			fileNodes = map[sdkdigest.Digest][]fileNode{}
			totalSize = 0
		}
		fileNodes[dg] = filenames
		totalSize += dg.Size
		w.downloadedBytes += dg.Size
	}
	// If we have anything left over, handle them now.
	if len(fileNodes) != 0 {
		g.Go(func() error { return w.downloadFiles(fileNodes) })
	}
	for dg, paths := range packs {
		g.Go(func() error {
			return w.downloadPack(dg, paths)
		})
	}
	return g.Wait()
}

// downloadPack downloads a pack file to the given path
func (w *worker) downloadPack(dg sdkdigest.Digest, paths []string) error {
	log.Debug("Downloading pack of %d bytes...", dg.Size)
	packsDownloaded.Inc()
	if w.fileCache != nil {
		if downloaded, err := w.downloadPackFromCache(dg, paths); err != nil {
			return err
		} else if downloaded {
			return nil
		}
	}
	w.limiter <- struct{}{}
	defer func() { <-w.limiter }()
	// TODO(peterebden): Look into streaming this sometime.
	b, err := w.client.ReadBlob(dg.ToProto())
	if err != nil {
		return err
	}
	if err := w.writePack(bytes.NewReader(b), paths); err != nil {
		return fmt.Errorf("extracting pack for %s: %w", dg.Hash, err)
	}
	packBytesRead.Add(float64(dg.Size))
	log.Debug("Downloaded pack of %d bytes", dg.Size)
	return nil
}

// downloadPack downloads a pack file to the given path
func (w *worker) downloadPackFromCache(dg sdkdigest.Digest, paths []string) (bool, error) {
	w.iolimiter <- struct{}{}
	defer func() { <-w.iolimiter }()
	if rc := w.fileCache.RetrieveStream(dg.Hash); rc != nil {
		defer rc.Close()
		if err := w.writePack(rc, paths); err != nil {
			return false, fmt.Errorf("extracting pack for %s: %w", dg.Hash, err)
		}
		return true, nil
	}
	return false, nil
}

// writePack writes a pack to the given set of paths.
func (w *worker) writePack(r io.Reader, paths []string) error {
	// Packs are always zstd-compressed tarballs.
	zr, err := zstd.NewReader(r)
	if err != nil {
		return err
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("extracting tarball: %w", err)
		} else if err := common.CheckPath(hdr.Name); err != nil {
			return err
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			for _, p := range paths {
				if err := os.MkdirAll(path.Join(p, hdr.Name), 0755|os.ModeDir); err != nil {
					return fmt.Errorf("creating directory: %w", err)
				}
			}
		case tar.TypeReg:
			p1 := path.Join(paths[0], hdr.Name)
			if f, err := os.OpenFile(p1, os.O_WRONLY|os.O_CREATE, os.FileMode(hdr.Mode)); err != nil {
				return err
			} else if n, err := io.Copy(f, tr); err != nil {
				f.Close() // don't forget this!
				return err
			} else if err := f.Close(); err != nil {
				return err
			} else if n != hdr.Size {
				return fmt.Errorf("Short read for %s: read %d bytes, expected %d", hdr.Name, n, hdr.Size)
			}
			// Link the file into all the other locations
			for _, p := range paths[1:] {
				if err := os.Link(p1, path.Join(p, hdr.Name)); err != nil {
					return err
				}
			}
		case tar.TypeSymlink:
			for _, p := range paths {
				if err := os.Symlink(hdr.Linkname, path.Join(p, hdr.Name)); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("Unknown type in pack for %s: %d", hdr.Name, hdr.Typeflag)
		}
	}
}

// downloadFiles downloads a set of files to disk in a batch.
// The total size must be lower than whatever limits are considered relevant.
func (w *worker) downloadFiles(files map[sdkdigest.Digest][]fileNode) error {
	w.limiter <- struct{}{}
	defer func() { <-w.limiter }()

	log.Debug("Downloading batch of %d files...", len(files))
	digests := make([]sdkdigest.Digest, 0, len(files))
	for dg := range files {
		digests = append(digests, dg)
	}
	responses, err := w.client.BatchDownload(digests)
	if err != nil {
		return err
	}
	if len(responses) != len(digests) {
		return grpcstatus.Errorf(codes.InvalidArgument, "Unexpected response, requested %d blobs, got %d", len(digests), len(responses))
	}
	for dg, data := range responses {
		if fileNodes, present := files[dg]; !present {
			return grpcstatus.Errorf(codes.InvalidArgument, "Unknown digest %s in response", dg.Hash)
		} else if err := w.writeFiles(fileNodes, data, dg); err != nil {
			return err
		}
		w.cache.Set(dg.Hash, data, int64(len(data)))
	}
	return nil
}

// downloadFile downloads a single file to disk.
func (w *worker) downloadFile(files []fileNode, dg sdkdigest.Digest) error {
	w.limiter <- struct{}{}
	defer func() { <-w.limiter }()

	log.Debug("Downloading file of %d bytes...", dg.Size)
	filename := files[0].Name
	if err := w.client.ReadToFile(dg, filename, w.compressor(files, dg.Size) != pb.Compressor_ZSTD); err != nil {
		return grpcstatus.Errorf(grpcstatus.Code(err), "Failed to download file: %s", err)
	} else if err := os.Chmod(filename, fileMode(files[0].IsExecutable)); err != nil {
		return fmt.Errorf("Failed to chmod file: %s", err)
	}
	w.fileCache.Store(w.dir, filename, dg.Hash)
	return w.linkAll(files)
}

// linkAll hardlinks all the given files in the list (assuming the first has already been written)
func (w *worker) linkAll(files []fileNode) error {
	if len(files) == 1 {
		return nil
	}
	done := map[string]struct{}{
		files[0].Name: {},
	}
	for _, f := range files[1:] {
		// This should technically not be necessary (REAPI says all child names must be unique)
		// but we have observed it happen occasionally, so let's defend against it for now.
		if _, present := done[f.Name]; !present {
			if err := os.Link(files[0].Name, f.Name); err != nil {
				return err
			}
			done[f.Name] = struct{}{}
		}
	}
	return nil
}

// writeFiles writes a blob to a series of files.
func (w *worker) writeFiles(files []fileNode, data []byte, dg sdkdigest.Digest) error {
	w.iolimiter <- struct{}{}
	defer func() { <-w.iolimiter }()
	if err := os.WriteFile(files[0].Name, data, fileMode(files[0].IsExecutable)); err != nil {
		return err
	} else if err := w.linkAll(files); err != nil {
		return err
	}
	w.fileCache.StoreAny(w.dir, files, dg.Hash)
	return nil
}

// makeDirIfNeeded makes a new subdir if the given name specifies a subdir (i.e. contains a path separator)
func makeDirIfNeeded(root, name string) error {
	if strings.ContainsRune(name, '/') {
		return os.MkdirAll(path.Join(root, path.Dir(name)), os.ModeDir|0755)
	}
	return nil
}

// digestProto returns a digest for a proto message.
func digestProto(msg proto.Message) *pb.Digest {
	blob, _ := proto.Marshal(msg)
	h := sha256.Sum256(blob)
	return &pb.Digest{Hash: hex.EncodeToString(h[:]), SizeBytes: int64(len(blob))}
}

func fileMode(isExecutable bool) os.FileMode {
	if isExecutable {
		return 0555
	}
	return 0444
}

// compressor returns the compressor to use for downloading the digest for a given set of files.
func (w *worker) compressor(fileNodes []fileNode, size int64) pb.Compressor_Value {
	// Using the first filename only here is a little suboptimal but we assume it won't
	// typically matter (since it is only an optimisation to choose whether to request
	// compression or not). We could potentially find another strategy in future if we
	// decide this is significantly suboptimal.
	return w.oneCompressor(fileNodes[0].Name, size)
}

// oneCompressor returns the compressor to use for a given filename
func (w *worker) oneCompressor(filename string, size int64) pb.Compressor_Value {
	if size >= rexclient.CompressionThreshold && shouldCompress(filename) {
		return pb.Compressor_ZSTD
	}
	return pb.Compressor_IDENTITY
}

// entryCompressor returns the compressor to use for an upload entry.
func (w *worker) entryCompressor(ue *uploadinfo.Entry) pb.Compressor_Value {
	if len(ue.Contents) > 0 {
		// Contents are set, if it looks like we've compressed already, don't do it again.
		if len(ue.Contents) < rexclient.CompressionThreshold || bytes.HasPrefix(ue.Contents, zstdMagic) {
			return pb.Compressor_IDENTITY
		}
		return pb.Compressor_ZSTD
	}
	return w.oneCompressor(ue.Path, ue.Digest.Size)
}

// shouldCompress returns true if the given filename should be compressed.
func shouldCompress(filename string) bool {
	return !(strings.HasSuffix(filename, ".zip") || strings.HasSuffix(filename, ".pex") ||
		strings.HasSuffix(filename, ".jar") || strings.HasSuffix(filename, ".gz") ||
		strings.HasSuffix(filename, ".bz2") || strings.HasSuffix(filename, ".xz"))
}
