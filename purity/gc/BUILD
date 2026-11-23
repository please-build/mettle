go_library(
    name = "gc",
    srcs = glob(["*.go"]),
    visibility = [
        "//discern/...",
        "//purity/...",
    ],
    deps = [
        "///third_party/go/github.com_bazelbuild_remote-apis-sdks//go/pkg/client",
        "///third_party/go/github.com_bazelbuild_remote-apis-sdks//go/pkg/digest",
        "///third_party/go/github.com_bazelbuild_remote-apis-sdks//go/pkg/uploadinfo",
        "///third_party/go/github.com_bazelbuild_remote-apis//build/bazel/remote/execution/v2",
        "///third_party/go/github.com_dustin_go-humanize//:go-humanize",
        "///third_party/go/github.com_golang_protobuf//proto",
        "///third_party/go/github.com_hashicorp_go-multierror//:go-multierror",
        "///third_party/go/github.com_peterebden_go-cli-init_v4//logging",
        "///third_party/go/github.com_sirupsen_logrus//:logrus",
        "///third_party/go/golang.org_x_term//:term",
        "///third_party/go/google.golang.org_grpc//codes",
        "///third_party/go/google.golang.org_grpc//status",
        "//proto/purity",
        "//rexclient",
    ],
)
