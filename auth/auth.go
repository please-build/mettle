// Package auth defines common authentication primitives for Mettle.
package auth

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/hashicorp/golang-lru/v2"
	"google.golang.org/api/idtoken"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

// The set of permissions that govern various RPCs
const (
	CASRead      = "cas:read"
	CASWrite     = "cas:write"
	ACRead       = "ac:read"
	ACWrite      = "ac:write"
	ExecuteRead  = "execute:read"
	ExecuteWrite = "execute:write"
	Fetch        = "fetch"
	Bootstrap    = "bootstrap"
	Capabilities = "capabilities"
	GC           = "gc"
	Reflection   = "reflection"
	Health       = "health"
	WorkersRead  = "workers:read"
	WorkersWrite = "workers:write"
)

// The anonymous role
const anonymous = "anonymous"

// The legacy static role
const legacy = "legacy"

// Temporary exports for backwards compat
const (
	AnonymousRole      = anonymous
	LegacyRole         = legacy
	AnonymousPrincipal = anonymous
	LegacyPrincipal    = legacy
)

// Permissions required for each RPC
var authenticatedMethods = map[string]string{
	"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities":                   Capabilities,
	"/build.bazel.remote.asset.v1.Fetch/FetchBlob":                                    Fetch,
	"/build.bazel.remote.asset.v1.Fetch/FetchDirectory":                               Fetch,
	"/build.bazel.remote.execution.v2.ActionCache/GetActionResult":                    ACRead,
	"/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult":                 ACWrite,
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs":       CASRead,
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs":     CASWrite,
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs":     CASRead,
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/GetTree":              CASRead,
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/SplitBlob":            CASRead,
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/GetChunkMapping":      CASRead,
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/SpliceBlob":           CASWrite,
	"/build.bazel.remote.execution.v2.ContentAddressableStorage/RegisterChunkMapping": CASWrite,
	"/build.bazel.remote.execution.v2.Execution/Execute":                              ExecuteWrite,
	"/build.bazel.remote.execution.v2.Execution/WaitExecution":                        ExecuteRead,
	"/build.please.remote.mettle.Bootstrap/ServeExecutions":                           Bootstrap,
	"/build.please.remote.purity.GC/List":                                             GC,
	"/build.please.remote.purity.GC/Delete":                                           GC,
	"/build.please.remote.lucidity.Lucidity/Update":                                   WorkersWrite,
	"/build.please.remote.lucidity.Lucidity/ListWorkers":                              WorkersRead,
	"/google.bytestream.ByteStream/QueryWriteStatus":                                  CASWrite,
	"/google.bytestream.ByteStream/Read":                                              CASRead,
	"/google.bytestream.ByteStream/Write":                                             CASWrite,
	"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo":                  Reflection,
	"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo":                       Reflection,
	"/grpc.health.v1.Health/Check":                                                    Health,
	"/grpc.health.v1.Health/Watch":                                                    Health,
	"/grpc.health.v1.Health/List":                                                     Health,
}

// A Config defines the authentication setup that a Mettle server accepts.
type Config struct {
	// Definitions of roles (map of role name -> details)
	// Note that 'anonymous' is the principal name that unauthorised users are given.
	Roles map[string]Role `yaml:"roles"`

	// Details of Google tokens (via Workload Identity or whatever)
	Google struct {
		// The expected audience
		Audience string `yaml:"audience"`
		// The LRU cache size for tokens
		CacheSize int `yaml:"cache_size"`
	} `yaml:"google"`

	// Legacy static token support
	Static struct {
		// The file to load the static token from
		File string `yaml:"file"`
	} `yaml:"static"`
}

// A Role grants a set of permissions to a set of principals.
type Role struct {
	// The permissions this role gets
	Permissions []string `yaml:"permissions"`
	// The names of incoming principals that get this role
	Principals []string `yaml:"principals"`
}

// LoadConfig loads config from the given file
func LoadConfig(filename string) (*Config, error) {
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	cfg := &Config{}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil && err != io.EOF {
		return nil, fmt.Errorf("parse config: %w", err)
	} else if len(cfg.Roles) == 0 {
		return nil, fmt.Errorf("no roles defined in config")
	}
	return cfg, nil
}

// MustLoadConfig is like LoadConfig but dies on errors.
func MustLoadConfig(filename string) *Config {
	cfg, err := LoadConfig(filename)
	if err != nil {
		panic(err)
	}
	return cfg
}

// An Authenticator auths incoming RPCs.
type Authenticator struct {
	config *Config
	// Map of RPC method name -> permission required to call it
	methods map[string]string
	// Map of principal -> permissions it holds
	principals map[string][]string
	// The legacy static token
	token     string
	validator *idtoken.Validator
	lruCache  *lru.Cache[string, *idtoken.Payload]
}

// NewAuthenticator creates an authenticator from the given config
func NewAuthenticator(ctx context.Context, cfg *Config) (*Authenticator, error) {
	auth := &Authenticator{
		config:     cfg,
		methods:    authenticatedMethods,
		principals: map[string][]string{},
	}
	if cfg.Static.File != "" {
		token, err := os.ReadFile(cfg.Static.File)
		if err != nil {
			return nil, fmt.Errorf("read static token file: %w", err)
		}
		auth.token = strings.TrimSpace(string(token))
	}
	for _, role := range cfg.Roles {
		for _, principal := range role.Principals {
			if _, present := auth.principals[principal]; present {
				// Conceptually we could return the union of their granted permissions, but for now this seems simpler
				return nil, fmt.Errorf("principal %s is defined on multiple roles", principal)
			}
			auth.principals[principal] = role.Permissions
		}
	}
	if cfg.Google.Audience != "" {
		validator, err := idtoken.NewValidator(ctx)
		if err != nil {
			return nil, err
		}
		auth.validator = validator
		cache, _ := lru.New[string, *idtoken.Payload](cmp.Or(cfg.Google.CacheSize, 1000))
		auth.lruCache = cache
	}
	return auth, nil
}

// MustNewAuthenticator is like NewAuthenticator but dies on errors
func MustNewAuthenticator(ctx context.Context, cfg *Config) *Authenticator {
	auth, err := NewAuthenticator(ctx, cfg)
	if err != nil {
		panic(err)
	}
	return auth
}

// AuthRPC returns nil if the given RPC is allowed to be called with this set of permissions, otherwise a gRPC status error
// describing why not: Unauthenticated if credentials are missing or invalid, PermissionDenied if they are insufficient.
// A nil Authenticator permits all RPCs.
func (auth *Authenticator) AuthRPC(ctx context.Context, method string) error {
	if auth == nil {
		return nil
	}
	required, present := auth.methods[method]
	if !present {
		return status.Errorf(codes.PermissionDenied, "unregistered method %s", method)
	}
	principal, err := auth.findPrincipal(ctx)
	if err != nil {
		return status.Error(codes.Unauthenticated, err.Error())
	}
	// Anonymous callers get Unauthenticated so clients know that providing credentials may help.
	code := codes.PermissionDenied
	if principal == anonymous {
		code = codes.Unauthenticated
	}
	permissions, present := auth.principals[principal]
	if !present {
		return status.Errorf(code, "principal %s is not authorized", principal)
	}
	if !slices.Contains(permissions, required) {
		return status.Errorf(code, "principal %s is not authorized to call %s (needs %s, has [%s])", principal, method, required, strings.Join(permissions, ", "))
	}
	return nil // Allowed!
}

// findPrincipal returns the principal for an incoming request
func (auth *Authenticator) findPrincipal(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return anonymous, nil
	}
	hdr, present := md["authorization"]
	if !present {
		return anonymous, nil
	} else if len(hdr) != 1 {
		return "", fmt.Errorf("must provide exactly 1 authorization header (was %d)", len(hdr))
	}
	tok, ok := strings.CutPrefix(hdr[0], "Bearer ")
	if !ok {
		return "", fmt.Errorf("not a bearer token")
	}
	if auth.token != "" && tok == auth.token {
		return legacy, nil
	}
	if auth.validator == nil {
		return "", fmt.Errorf("no validator configured")
	}
	payload, err := auth.validateIDToken(ctx, tok)
	if err != nil {
		return "", err
	}
	email, present := payload.Claims["email"]
	if !present {
		return "", fmt.Errorf("missing email claim")
	}
	s, ok := email.(string)
	if !ok {
		return "", fmt.Errorf("invalid type for email claim (was %T)", email)
	}
	return s, nil
}

// validateIDToken validates an incoming ID token, possibly using a cached version.
func (auth *Authenticator) validateIDToken(ctx context.Context, token string) (*idtoken.Payload, error) {
	if payload, ok := auth.lruCache.Get(token); ok {
		// Need to re-check expiry in case it's no longer valid
		if payload.Expires < time.Now().Unix() {
			auth.lruCache.Remove(token)
			return nil, fmt.Errorf("token has expired")
		}
		return payload, nil
	}
	payload, err := auth.validator.Validate(ctx, token, auth.config.Google.Audience)
	if err != nil {
		return nil, fmt.Errorf("validate token: %w", err)
	}
	auth.lruCache.Add(token, payload)
	return payload, nil
}
