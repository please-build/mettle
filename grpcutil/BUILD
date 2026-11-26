go_library(
    name = "grpcutil",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    visibility = ["PUBLIC"],
    deps = [
        "///third_party/go/github.com_grpc-ecosystem_go-grpc-middleware_providers_prometheus//:prometheus",
        "///third_party/go/github.com_grpc-ecosystem_go-grpc-middleware_v2//interceptors/recovery",
        "///third_party/go/github.com_mostynb_go-grpc-compression//zstd",
        "///third_party/go/github.com_peterebden_go-cli-init_v4//logging",
        "///third_party/go/github.com_prometheus_client_golang//prometheus",
        "///third_party/go/google.golang.org_grpc//:grpc",
        "///third_party/go/google.golang.org_grpc//codes",
        "///third_party/go/google.golang.org_grpc//credentials",
        "///third_party/go/google.golang.org_grpc//encoding/gzip",
        "///third_party/go/google.golang.org_grpc//grpclog",
        "///third_party/go/google.golang.org_grpc//health",
        "///third_party/go/google.golang.org_grpc//health/grpc_health_v1",
        "///third_party/go/google.golang.org_grpc//keepalive",
        "///third_party/go/google.golang.org_grpc//reflection",
        "///third_party/go/google.golang.org_grpc//status",
        "///third_party/go/gopkg.in_op_go-logging.v1//:go-logging.v1",
        "//auth",
    ],
)

go_test(
    name = "grpcutil_test",
    srcs = glob(["*_test.go"]),
    deps = [
        ":grpcutil",
        "///third_party/go/github.com_stretchr_testify//assert",
        "///third_party/go/github.com_stretchr_testify//require",
        "///third_party/go/google.golang.org_grpc//metadata",
    ],
)
