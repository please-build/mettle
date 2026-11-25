go_library(
    name = "auth",
    srcs = ["auth.go"],
    deps = [
        "///third_party/go/github.com_hashicorp_golang-lru_v2//:v2",
        "///third_party/go/google.golang.org_api//idtoken",
        "///third_party/go/google.golang.org_grpc//metadata",
        "///third_party/go/gopkg.in_yaml.v3//:yaml.v3",
    ],
)

go_test(
    name = "auth_test",
    srcs = ["auth_test.go"],
    deps = [
        ":auth",
        "///third_party/go/github.com_stretchr_testify//assert",
        "///third_party/go/github.com_stretchr_testify//require",
    ],
)
