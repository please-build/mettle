# AGENTS.md

This file provides guidance to coding agents when working with code in this repository.

## Build System

This project uses **Please** (https://please.build), a high-performance build system.

### Common Commands

```bash
# Build everything
./pleasew build //...

# Run tests
./pleasew test //...

# Run a specific test
./pleasew test //mettle/api:api_test

# Build and run all servers locally for development
./pleasew runlocal

# Build all servers (but don't run)
./pleasew buildlocal

# Run linter
./pleasew puku lint

# Format code
./pleasew puku fmt
```

### Running Individual Servers Locally

```bash
# Run Elan (CAS server) on port 7777
./pleasew run //elan:run_local

# Run Mettle (execution server) in dual mode on port 7778
./pleasew run //mettle:run_local

# Run Lucidity (fleet management) on port 7774
./pleasew run //lucidity:run_local

# Run Zeal (asset fetcher) on port 7772
./pleasew run //zeal:run_local

# Run Flair (proxy/load balancer) on port 7773
./pleasew run //flair:run_local

# Run Redis on port 6379
./pleasew run //redis:run_local

# Run Browser (web UI) on port 7779
./pleasew run //browser:run_local
```

### Testing

- Unit tests: `./pleasew test //package:target_test`
- Integration tests: Located in `tests/` directory, orchestrated by `runner.go`
- Test against local servers: Use the `localremote` profile with Please

## Architecture Overview

This repository contains a suite of servers implementing the Bazel Remote Execution API, designed for distributed build execution. All servers are written in Go and communicate via gRPC.

### Server Components

**Mettle** - Remote Execution Server (`//mettle`)
- Executes build actions for remote builds
- Runs in three modes:
  - **API mode**: Handles client requests, submits jobs via Cloud Pub/Sub
  - **Worker mode**: Executes jobs, communicates with CAS
  - **Dual mode**: Combined API + worker with in-memory queue (local testing only)
- Key files:
  - `mettle/api/`: API server implementation
  - `mettle/worker/`: Worker execution logic
  - `mettle/common/`: Shared utilities for both modes

**Elan** - Content Addressable Storage (`//elan`)
- Implements CAS, ActionCache, ByteStream, and Capabilities services
- Backend: GCS buckets (production), file/memory (testing)
- Supports compression (zstd), "packs" (compressed directory tarballs), and Redis indexing
- Key files in `elan/rpc/`: Storage abstraction, tree caching, GC API

**Zeal** - Remote Asset Fetcher (`//zeal`)
- Implements FetchBlob RPC from Remote Asset API
- Downloads artifacts via HTTP(S) with SRI checksums
- Stores results in connected CAS server
- Key files in `zeal/rpc/`

**Flair** - Proxy and Load Balancer (`//flair`)
- Routes requests across multiple backend servers
- Hash-space partitioning using a trie data structure
- Supports replication for write operations
- Key files: `flair/trie/` (partitioning logic), `flair/rpc/` (proxy implementation)

**Lucidity** - Fleet Management Dashboard (`//lucidity`)
- Monitors Mettle worker fleet
- HTTP interface for viewing worker status
- Workers report health, version, and current tasks periodically
- Can remotely enable/disable workers
- Key files in `lucidity/rpc/`

**Discern** - Build Action Analyzer (`//discern`)
- CLI utility for analyzing build actions
- Commands: show, diff, top, blobs
- Useful for debugging cache misses and understanding action inputs/outputs

**Purity** - Garbage Collector (`//purity`)
- Cleans unreferenced blobs from Elan storage
- Analyzes action results to determine retention
- Can run one-shot or periodically
- Key files in `purity/gc/`

### Shared Packages

**grpcutil** (`//grpcutil`)
- Common gRPC infrastructure: TLS, auth, interceptors, metrics
- Token-based authentication (Bearer tokens)
- Prometheus metrics and health checks

**rexclient** (`//rexclient`)
- Wrapper around Bazel Remote Execution SDK client
- Handles compression and "pack" optimization
- Used by workers to communicate with CAS

**redis** (`//redis`)
- Redis client configuration with primary/replica support
- Rate limiter implementation
- Used by Mettle (coordination) and Elan (blob indexing)

**cli** (`//cli`)
- Common CLI flags, logging setup, admin HTTP server
- Action digest parsing utilities
- Currency type for cost tracking

### Communication Patterns

1. **Client → Mettle API**: gRPC Remote Execution API (Execute, WaitExecution)
2. **Mettle API ↔ Workers**: Cloud Pub/Sub (production) or in-memory queue (local)
   - Request topic: API → Worker (job submissions)
   - Response topic: Worker → API (job results)
3. **Mettle Workers ↔ Elan**: Direct gRPC (CAS, ByteStream, ActionCache)
4. **Mettle Workers → Lucidity**: Periodic status updates via gRPC
5. **Zeal → Elan**: Downloads from HTTP, uploads to CAS via gRPC
6. **Flair → Backends**: Proxies and routes all requests by hash
7. **Purity → Elan**: Uses GC API to analyze and delete blobs

### Storage and Pub/Sub Abstraction

The codebase uses `gocloud.dev` for abstraction:

**Storage** (via `gocloud.dev/blob`):
- Production: `gs://bucket-name` (GCS)
- Local testing: `file:///path` or `mem://`

**Pub/Sub** (via `gocloud.dev/pubsub`):
- Production: `gcppubsub://project/topic-name`
- Local testing: `mem://topic-name` (custom implementation in `mettle/mempubsub`)

### Key Architectural Patterns

**Compression**
- zstd compression for blobs > 1024 bytes
- Compressed blobs stored with `zstd_cas` prefix
- Archives detected and skipped to avoid double-compression

**"Packs" Optimization**
- Compressed tarballs representing entire directory trees
- Stored as node properties in Directory messages
- Allows downloading large trees without individual blob fetches
- Metadata key: `mettle.stop-at-pack`

**Authentication**
- Token-based (Bearer tokens in `token_file`)
- Read operations: No auth by default
- Write operations: Require auth
- Can force auth on all RPCs with `--auth_read_only=false`

**Resource Management**
- Workers check disk space and memory before accepting jobs
- Parallel request limits in Elan
- Redis rate limiting for large blobs
- Disk/memory thresholds configurable

**Sandboxing**
- Build actions run in sandboxed environments
- Sandbox binaries: `//sandbox:sandbox` and `//sandbox:alt_sandbox`
- Linux namespaces for isolation

**Graceful Shutdown**
- Signal handling for SIGTERM/SIGINT
- Workers complete current job before exiting
- Context cancellation propagated throughout

## Development Workflow

### Local Development Setup

1. Start all services: `./pleasew runlocal`
2. Servers will start with these ports:
   - Elan (CAS): 7777
   - Mettle (execution): 7778
   - Browser (UI): 7779
   - Redis: 6379
   - Zeal (assets): 7772
   - Flair (proxy): 7773
   - Lucidity (fleet): 7774
3. Logs are written to `plz-out/log/*.log`
4. Storage/state in `plz-out/` directories

### Testing Locally

Use Please with the `localremote` profile to test against local servers:
```bash
plz build --profile localremote //...
```

### Code Style

- Linting: golangci-lint via `./pleasew puku lint`
- Formatting: `./pleasew puku fmt`
- Configuration: `.golangci.yml`

### Proto Definitions

- Standard Bazel Remote APIs imported from `//third_party/proto`
- Custom protos in `//proto/`:
  - `mettle.proto`: Bootstrap API for zero-downtime deployments
  - `lucidity.proto`: Worker status reporting
  - `purity.proto`: GC operations
- Generate code: Handled automatically by Please build rules

### Adding Dependencies

- Go modules: Edit `go.mod`, then run `./pleasew puku sync`
- Proto dependencies: Update `//third_party/proto/BUILD`
- Third-party Go packages: Managed in `//third_party/go/BUILD`

## Important Notes

- This is not externally supported software but it is used internally so you should be
  careful about compatibility, especially for external code (notably remote-apis-sdks).
- It is a single-client ecosystem: we only use Please as the client, supporting its
  behaviours are critical, but we do not need to worry about historic behaviours
  (we stay up-to-date).
- Any behavioural changes need to be compatible and you must have a plan about how
  a client would upgrade out-of-sync with the server.
- Designed for Google Cloud Platform (Pub/Sub, Storage)
- Your comments should be brief and to-the-point. They should not reference older
  behaviours, only the current behaviours of the code.
