package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/digest"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/uploadinfo"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

// runPreflightAction runs a simple known action to ensure we can process them correctly.
func (w *worker) runPreflightAction(ctx context.Context) error {
	log.Notice("Preparing pre-flight action...")
	const fileContents = "thirty-five ham and cheese sandwiches\n"
	cmd := &pb.Command{
		Arguments: []string{
			"bash", "--noprofile", "--norc", "-c", "cp $SRCS $OUTS",
		},
		EnvironmentVariables: []*pb.Command_EnvironmentVariable{
			{Name: "SRCS", Value: "in.txt"},
			{Name: "OUTS", Value: "out.txt"},
		},
		OutputPaths: []string{
			"out.txt",
		},
	}
	input := &pb.Directory{
		Files: []*pb.FileNode{
			{
				Name:   "in.txt",
				Digest: digest.NewFromBlob([]byte(fileContents)).ToProto(),
			},
		},
	}
	action := &pb.Action{
		CommandDigest:   mustNewDigestFromMessage(cmd),
		InputRootDigest: mustNewDigestFromMessage(input),
		DoNotCache:      false, // We don't set DoNotCache to make sure we can write a request to the server
		Timeout:         durationpb.New(10 * time.Second),
	}
	req := &pb.ExecuteRequest{
		SkipCacheLookup: true,
		ActionDigest:    mustNewDigestFromMessage(action),
	}
	if err := w.client.UploadIfMissing(ctx, []*uploadinfo.Entry{
		uploadinfo.EntryFromBlob([]byte(fileContents)),
		mustEntryFromProto(cmd),
		mustEntryFromProto(input),
		mustEntryFromProto(action),
	}, []pb.Compressor_Value{
		pb.Compressor_IDENTITY,
		pb.Compressor_IDENTITY,
		pb.Compressor_IDENTITY,
		pb.Compressor_IDENTITY,
	}); err != nil {
		return err
	}
	log.Notice("Running pre-flight action...")
	if resp := w.runTaskRequest(ctx, req); resp.Status.Code != 0 {
		return fmt.Errorf("Failed to run pre-flight request: %s %s", resp.Status, resp.Message)
	} else if resp.Result.ExitCode != 0 {
		return fmt.Errorf("Failed to run pre-flight request: exit code %d: %s", resp.Result.ExitCode, resp.Message)
	}
	log.Notice("Pre-flight action run successfully!")
	return nil
}

func mustEntryFromProto(msg proto.Message) *uploadinfo.Entry {
	entry, err := uploadinfo.EntryFromProto(msg)
	if err != nil {
		panic(err)
	}
	return entry
}

func mustNewDigestFromMessage(msg proto.Message) *pb.Digest {
	dg, err := digest.NewFromMessage(msg)
	if err != nil {
		panic(err)
	}
	return dg.ToProto()
}
