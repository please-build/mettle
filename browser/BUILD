# This is for local development only, runs a local version of bb-browser to help
# visualise some of the outputs.
BROWSER_ARCH = CONFIG.ARCH
if CONFIG.OS == "darwin" and CONFIG.ARCH == "arm64":
    BROWSER_ARCH = "amd64"

remote_file(
    name = "bb_browser",
    out = "bb-browser",
    binary = True,
    hashes = [
        "f70c6e03abec5bc453d9adddc668d5da2c54f61e5d6c6da8d2816474e1cfd16e",
        "b01f200070805cf5180fc7aa1bc01b5bebdf516e176710fe8a54ee90a6db0084",
        "8d9f4d6fc5c53c5461b4d8f8e8763156d26296d6a2eadd6ab7b58b5a53104918",
    ],
    url = f"https://github.com/please-build/mettle/releases/download/v9.2.1/bb_browser_{CONFIG.OS}_{BROWSER_ARCH}",
)

filegroup(
    name = "config",
    srcs = ["config.jsonnet"],
    binary = True,
)

sh_cmd(
    name = "run_local",
    srcs = [":bb_browser"],
    cmd = "cd plz-out/bin/browser && exec ./bb-browser config.jsonnet",
    deps = [
        ":config",
    ],
)
