// Package auth defines common authentication primitives for Mettle.
package auth

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/hashicorp/golang-lru/v2"
	"google.golang.org/api/idtoken"
	"google.golang.org/grpc/metadata"
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
	"/build.please.remote.purity.GC/Info":                                             GC,
	"/build.please.remote.purity.GC/List":                                             GC,
	"/build.please.remote.purity.GC/Delete":                                           GC,
	"/google.bytestream.ByteStream/QueryWriteStatus":                                  CASWrite,
	"/google.bytestream.ByteStream/Read":                                              CASRead,
	"/google.bytestream.ByteStream/Write":                                             CASWrite,
	"/grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo":                  Reflection,
	"/grpc.reflection.v1.ServerReflection/ServerReflectionInfo":                       Reflection,
	"/grpc.health.v1.Health/Check":                                                    Health,
}

// A Config defines the authentication setup that a Mettle server accepts.
type Config struct {
	// Definitions of roles (map of role name -> details)
	// Note that 'anonymous' is the role name that unauthorised users are given.
	Roles map[string]struct {
		// The permissions this role gets
		Permissions []string `yaml:"permissions"`
		// The names of incoming principals that get this role
		Principals []string `yaml:"principals"`
	} `yaml:"roles"`

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

// LoadConfig loads config from the given file
func LoadConfig(filename string) (*Config, error) {
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
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
	config     *Config
	principals map[string][]string
	token      string
	validator  *idtoken.Validator
	lruCache   *lru.Cache[string, *idtoken.Payload]
}

// NewAuthenticator creates an authenticator from the given config
func NewAuthenticator(ctx context.Context, cfg *Config) (*Authenticator, error) {
	auth := &Authenticator{
		config:     cfg,
		principals: map[string][]string{},
	}
	if cfg.Static.File != "" {
		token, err := os.ReadFile(cfg.Static.File)
		if err != nil {
			return nil, fmt.Errorf("failed to read static token file: %w", err)
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
		cache, _ := lru.New[string, *idtoken.Payload](max(cfg.Google.CacheSize, 1000))
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

// AuthRPC returns nil if the given RPC is allowed to be called with this set of permissions, otherwise an error describing why not.
func (auth *Authenticator) AuthRPC(ctx context.Context, method string) error {
	required, present := authenticatedMethods[method]
	if !present {
		return fmt.Errorf("unregistered method %s", method)
	}
	principal, err := auth.findPrincipal(ctx)
	if err != nil {
		return err
	}
	permissions, present := auth.principals[principal]
	if !present {
		return fmt.Errorf("principal %s is not authorized", principal)
	}
	if !slices.Contains(permissions, required) {
		return fmt.Errorf("principal %s is not authorized to call %s (needs %s, has %v)", principal, required, permissions)
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
	if tok == auth.token {
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
		return nil, fmt.Errorf("failed to validate token: %w", err)
	}
	auth.lruCache.Add(token, payload)
	return payload, nil
}
