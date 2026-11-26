// Package main implements a simple utility to visualise build actions.
package main

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/client"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/dustin/go-humanize"
	"github.com/peterebden/go-cli-init/v4/logging"
	"google.golang.org/protobuf/proto"

	"github.com/please-build/mettle/cli"
	"github.com/please-build/mettle/purity/gc"
	"github.com/please-build/mettle/rexclient"
)

var log = logging.MustGetLogger()

var opts = struct {
	Usage   string
	Logging cli.LoggingOpts `group:"Options controlling logging output"`
	Storage struct {
		InstanceName string `short:"i" long:"instance" default:"mettle" description:"Instance name"`
		Storage      string `short:"s" long:"storage" required:"true" description:"URL to connect to the CAS server on, e.g. localhost:7878"`
		TLS          bool   `long:"tls" description:"Use TLS for communication with the storage server"`
	} `group:"Options controlling connection to the CAS server"`
	Diff struct {
		Before cli.Action `short:"b" long:"before" required:"true" description:"'Before' action hash"`
		After  cli.Action `short:"a" long:"after" required:"true" description:"'After' action hash"`
	} `command:"diff" description:"Show differences between two actions"`
	Show struct {
		NoInputs bool `long:"no_inputs" description:"Don't list input files"`
		Args     struct {
			Actions []cli.Action `positional-arg-name:"action" required:"true" description:"Hashes of actions to display"`
		} `positional-args:"true"`
	} `command:"show" description:"Show detail about an action or series of them"`
	TopN struct {
		N          int    `short:"n" long:"number" default:"100" description:"Number of actions to display"`
		BrowserURL string `long:"browser_url" description:"Browser base URL to display links to"`
	} `command:"topn" description:"Display information on the top N actions with biggest inputs / outputs"`
	MostUsed struct {
		N       int      `short:"n" long:"number" default:"100" description:"Number of blobs to display"`
		Include []string `short:"i" long:"include" description:"Filename prefixes to include"`
		Exclude []string `short:"e" long:"exclude" description:"Filename prefixes to exclude"`
	} `command:"mostused" description:"Display information on the most-downloaded blobs"`
}{
	Usage: `
Discern is a simple binary for visualising build actions; either showing differences
between two or displaying the inputs to a single one.
This can be useful for a "what's changed" kind of question.

Note that it does not support every field exhaustively right now - notably we leave
out NodeProperties since we aren't using them (yet?).

It doesn't quite follow our naming scheme (should be "discerning") but it
also isn't a server so #dealwithit
`,
}

func main() {
	ctx := context.Background()
	cmd, _ := cli.ParseFlagsOrDie("Discern", &opts, &opts.Logging)
	if cmd == "topn" {
		if err := topn(ctx); err != nil {
			log.Fatalf("Failed to find action results: %s", err)
		}
		return
	} else if cmd == "mostused" {
		if err := mostused(ctx); err != nil {
			log.Fatalf("Failed to find blob info: %s", err)
		}
		return
	}
	client := rexclient.MustNew(ctx, opts.Storage.InstanceName, opts.Storage.Storage, opts.Storage.TLS, "")
	defer client.Close()
	if cmd == "diff" {
		diff(ctx, client)
	} else {
		show(ctx, client)
	}
}

func diff(ctx context.Context, client *client.Client) {
	before := &pb.Action{}
	after := &pb.Action{}
	mustGetProto(ctx, client, opts.Diff.Before.ToProto(), before)
	mustGetProto(ctx, client, opts.Diff.After.ToProto(), after)
	if before.CommandDigest.Hash == after.CommandDigest.Hash {
		log.Notice("Commands are identical")
	} else {
		log.Warning("Commands differ: %s vs. %s", before.CommandDigest.Hash, after.CommandDigest.Hash)
		beforeCmd := &pb.Command{}
		afterCmd := &pb.Command{}
		mustGetProto(ctx, client, before.CommandDigest, beforeCmd)
		mustGetProto(ctx, client, after.CommandDigest, afterCmd)
		compareCommands(beforeCmd, afterCmd)
	}
	if before.InputRootDigest.Hash == after.InputRootDigest.Hash {
		log.Notice("Input roots are identical")
	} else {
		log.Warning("Input roots differ: %s vs. %s", before.InputRootDigest, after.InputRootDigest)
		compareDirectories(ctx, client, before.InputRootDigest, after.InputRootDigest, "")
	}
	if !proto.Equal(before.Timeout, after.Timeout) {
		log.Warning("Timeouts are different: %s / %s", before.Timeout, after.Timeout)
	}
	if before.DoNotCache != after.DoNotCache {
		log.Warning("DoNotCache differs: %v / %v", before.DoNotCache, after.DoNotCache)
	}
}

func mustGetProto(ctx context.Context, client *client.Client, dg *pb.Digest, msg proto.Message) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if _, err := client.ReadProto(ctx, digest.NewFromProtoUnvalidated(dg), msg); err != nil {
		log.Fatalf("Failed to fetch digest %s: %s", dg.Hash, err)
	}
}

func compareCommands(b, a *pb.Command) {
	compareRepeatedString("Arguments", b.Arguments, a.Arguments)
	if compareRepeatedString("OutputPaths", b.OutputPaths, a.OutputPaths) {
		// Don't repeat these two if output paths differ (since that is basically a superset)
		compareRepeatedString("OutputFiles", b.OutputFiles, a.OutputFiles)
		compareRepeatedString("OutputDirectories", b.OutputDirectories, a.OutputDirectories)
	}
	enva := map[string]string{}
	envb := map[string]string{}
	for _, v := range a.EnvironmentVariables {
		enva[v.Name] = v.Value
	}
	for _, v := range b.EnvironmentVariables {
		envb[v.Name] = v.Value
	}
	for _, va := range a.EnvironmentVariables {
		if vb, present := envb[va.Name]; !present {
			log.Warning("Environment variables differ: %s=%s / <not present>", va.Name, va.Value)
		} else if va.Value != vb {
			log.Warning("Environment variables differ: %s=%s / %s=%s", va.Name, va.Value, va.Name, vb)
		}
	}
	for _, vb := range b.EnvironmentVariables {
		if _, present := enva[vb.Name]; !present {
			log.Warning("Environment variables differ: <not present> / %s=%s", vb.Name, vb.Value)
		}
		// Don't re-report differences here.
	}
	// TODO(peterebden): check platform properties too
}

func compareRepeatedString(name string, b, a []string) bool {
	ret := false
	for i, s1 := range b {
		if i >= len(a) {
			log.Warning("%s differ; %s not in 'after' action", name, s1)
			ret = true
		} else if s2 := a[i]; s1 != s2 {
			log.Warning("%s differ: %s / %s", name, s1, s2)
			ret = true
		}
	}
	return ret
}

func compareDirectories(ctx context.Context, client *client.Client, before, after *pb.Digest, indent string) {
	b := &pb.Directory{}
	a := &pb.Directory{}
	mustGetProto(ctx, client, before, b)
	mustGetProto(ctx, client, after, a)
	for i, f1 := range b.Files {
		if i >= len(a.Files) {
			log.Warning("%s%s %s%s / <missing>", indent, f1.Name, f1.Digest.Hash, exe(f1.IsExecutable))
		} else if f2 := a.Files[i]; !proto.Equal(f1, f2) {
			log.Warning("%s%s %s%s / %s %s%s", indent, f1.Name, f1.Digest.Hash, exe(f1.IsExecutable), f2.Name, f2.Digest.Hash, exe(f2.IsExecutable))
		}
	}
	for i, d1 := range b.Directories {
		if i >= len(a.Directories) {
			log.Warning("%s%s %s / <missing>", indent, d1.Name, d1.Digest.Hash)
		} else if d2 := a.Directories[i]; !proto.Equal(d1, d2) {
			log.Warning("%s%s %s / %s %s", indent, d1.Name, d1.Digest.Hash, d2.Name, d2.Digest.Hash)
			compareDirectories(ctx, client, d1.Digest, d2.Digest, indent+"  ")
		}
	}
	for i, s1 := range b.Symlinks {
		if i >= len(a.Symlinks) {
			log.Warning("%s%s -> %s / <missing>", indent, s1.Name, s1.Target)
		} else if s2 := a.Symlinks[i]; !proto.Equal(s1, s2) {
			log.Warning("%s%s -> %s / %s -> %s", indent, s1.Name, s1.Target, s2.Name, s2.Target)
		}
	}
}

func exe(is bool) string {
	if is {
		return " (exe)"
	}
	return ""
}

func show(ctx context.Context, client *client.Client) {
	for i, a := range opts.Show.Args.Actions {
		if i > 0 {
			fmt.Print("\n\n")
		}
		log.Notice("Action %s/%d:", a.Hash, a.Size)
		action := &pb.Action{}
		command := &pb.Command{}
		mustGetProto(ctx, client, a.ToProto(), action)
		mustGetProto(ctx, client, action.CommandDigest, command)
		if !opts.Show.NoInputs {
			log.Notice("Inputs:")
			showDir(ctx, client, action.InputRootDigest, "")
		}
		if ar, err := client.CheckActionCache(ctx, &pb.Digest{Hash: a.Hash, SizeBytes: int64(a.Size)}); err != nil {
			log.Error("Error retrieving action result: %s", err)
		} else if ar == nil {
			log.Notice("No result exists for this action")
		} else {
			log.Notice("Outputs:")
			log.Notice("[%s/%08d] Action result", a.Hash, a.Size)
			showActionResult(ctx, client, ar)
			showOutput(ctx, client, ar, ar.StdoutDigest, "Standard output")
			showOutput(ctx, client, ar, ar.StdoutDigest, "Standard error")
		}
	}
}

func showDir(ctx context.Context, client *client.Client, dg *pb.Digest, indent string) {
	dir := &pb.Directory{}
	if _, err := client.ReadProto(ctx, digest.NewFromProtoUnvalidated(dg), dir); err != nil {
		log.Error("[%s/%08d] %s: Not found!", dg.Hash, dg.SizeBytes, indent)
		return
	}
	if dg := rexclient.PackDigest(dir); dg.Hash != "" {
		if dgs, err := client.MissingBlobs(ctx, []digest.Digest{dg}); err != nil || len(dgs) > 0 {
			log.Error("[%s/%08d] %s└─> Mettle pack not found!", dg.Hash, dg.Size, indent)
		} else {
			log.Notice("[%s/%08d] %s└─> Mettle pack", dg.Hash, dg.Size, indent)
		}
	}
	for _, d := range dir.Directories {
		log.Notice("[%s/%08d] %s%s/", d.Digest.Hash, d.Digest.SizeBytes, indent, d.Name)
		showDir(ctx, client, d.Digest, indent+"  ")
	}
	for _, s := range dir.Symlinks {
		log.Notice("[%s/%08d]%s%-50s -> %s", strings.Repeat(" ", 64), 0, indent, s.Name, s.Target)
	}
	showFiles(ctx, client, dir.Files, indent)
}

func showFiles(ctx context.Context, client *client.Client, files []*pb.FileNode, indent string) {
	digests := make([]*pb.Digest, len(files))
	for i, f := range files {
		digests[i] = f.Digest
	}
	resp, err := client.FindMissingBlobs(ctx, &pb.FindMissingBlobsRequest{
		InstanceName: client.InstanceName,
		BlobDigests:  digests,
	})
	if err != nil {
		log.Error("%s: Request failed! %s", indent, err)
		return
	}
	m := map[string]bool{}
	for _, r := range resp.MissingBlobDigests {
		m[r.Hash] = true
	}
	for _, f := range files {
		if m[f.Digest.Hash] {
			log.Error("[%s/%08d] %s%s Not found!", f.Digest.Hash, f.Digest.SizeBytes, indent, f.Name)
		} else {
			log.Notice("[%s/%08d] %s%s", f.Digest.Hash, f.Digest.SizeBytes, indent, f.Name)
		}
	}
}

func showActionResult(ctx context.Context, client *client.Client, ar *pb.ActionResult) {
	files := make([]*pb.FileNode, len(ar.OutputFiles))
	for i, f := range ar.OutputFiles {
		files[i] = &pb.FileNode{
			Name:   f.Path,
			Digest: f.Digest,
		}
	}
	showFiles(ctx, client, files, "")
	for _, d := range ar.OutputDirectories {
		tree := &pb.Tree{}
		if _, err := client.ReadProto(ctx, digest.NewFromProtoUnvalidated(d.TreeDigest), tree); err != nil {
			log.Error("Failed to download output tree: %s", err)
		} else {
			log.Notice("[%s/%08d] %s", d.TreeDigest.Hash, d.TreeDigest.SizeBytes, d.Path)
			dg, _ := digest.NewFromMessage(tree.Root)
			showDir(ctx, client, dg.ToProto(), strings.Repeat("  ", strings.Count(d.Path, "/")+1))
		}
	}
}

func showOutput(ctx context.Context, client *client.Client, ar *pb.ActionResult, dg *pb.Digest, name string) {
	if dg == nil {
		return
	}
	log.Notice("%s [%s/%08d]:", name, dg.Hash, dg.SizeBytes)
	b, _, err := client.ReadBlob(ctx, digest.NewFromProtoUnvalidated(dg))
	if err != nil {
		log.Errorf("Failed to fetch blob: %s", err)
		return
	}
	fmt.Println(string(b))
}

func topn(ctx context.Context) error {
	actions, err := gc.Sizes(ctx, opts.Storage.Storage, opts.Storage.InstanceName, "", opts.Storage.TLS, opts.TopN.N)
	if err != nil {
		return err
	}
	log.Notice("Top %d actions:", opts.TopN.N)
	for i, a := range actions { //nolint:govet
		in := humanize.Bytes(uint64(a.InputSize))
		out := humanize.Bytes(uint64(a.OutputSize))
		if opts.TopN.BrowserURL != "" {
			log.Notice("%d: %s (in) %s (out): %s/action/%s/%s/%d/", i, in, out, opts.TopN.BrowserURL, opts.Storage.InstanceName, a.Hash, a.SizeBytes)
		} else {
			log.Notice("%d: %s (in) %s (out): %s/%d", i, in, out, a.Hash, a.SizeBytes)
		}
	}
	return nil
}

func mostused(ctx context.Context) error {
	allBlobs, err := gc.BlobUsage(ctx, opts.Storage.Storage, opts.Storage.InstanceName, "", opts.Storage.TLS)
	if err != nil {
		return err
	}
	blobs := make([]gc.Blob, 0, len(allBlobs))
	for _, blob := range allBlobs {
		if shouldInclude(blob.Filename) {
			blobs = append(blobs, blob)
		}
	}
	sort.Slice(blobs, func(i, j int) bool {
		return blobs[i].SizeBytes*blobs[i].Count > blobs[j].SizeBytes*blobs[j].Count
	})
	if len(blobs) > opts.MostUsed.N {
		blobs = blobs[:opts.MostUsed.N]
	}
	log.Notice("Most used %d blobs:", opts.MostUsed.N)
	var size, total int64
	for _, blob := range blobs {
		log.Notice("%s/%08d: %s (%s, used %d times, total %s)", blob.Hash, blob.SizeBytes, blob.Filename, humanize.Bytes(uint64(blob.SizeBytes)), blob.Count, humanize.Bytes(uint64(blob.SizeBytes*blob.Count)))
		size += blob.SizeBytes
		total += blob.SizeBytes * blob.Count
	}
	log.Notice("Total size %s, total downloads %s", humanize.Bytes(uint64(size)), humanize.Bytes(uint64(total)))
	return nil
}

func shouldInclude(name string) bool {
	for _, excl := range opts.MostUsed.Exclude {
		if strings.HasPrefix(name, excl) {
			return false
		}
	}
	if len(opts.MostUsed.Include) == 0 {
		return true
	}
	for _, incl := range opts.MostUsed.Include {
		if strings.HasPrefix(name, incl) {
			return true
		}
	}
	return false
}
