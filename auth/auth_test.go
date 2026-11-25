package auth

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/idtoken"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

func writeFile(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, []byte(contents), 0644))
	return path
}

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	tokenFile := writeFile(t, dir, "token", "  s3cr3t\n")
	cfgFile := writeFile(t, dir, "config.yaml", `
roles:
  anonymous:
    permissions:
      - cas:read
      - ac:read
  builder:
    permissions:
      - cas:read
      - cas:write
      - execute:write
    principals:
      - builder@project.iam.gserviceaccount.com
      - ci@project.iam.gserviceaccount.com
  legacy:
    permissions:
      - cas:write
      - ac:write
google:
  audience: https://mettle.example.com
static:
  file: `+tokenFile,
	)
	cfg, err := LoadConfig(cfgFile)
	require.NoError(t, err)

	require.Len(t, cfg.Roles, 3)
	assert.Equal(t, []string{CASRead, ACRead}, cfg.Roles["anonymous"].Permissions)
	assert.Empty(t, cfg.Roles["anonymous"].Principals)
	assert.Equal(t, []string{CASRead, CASWrite, ExecuteWrite}, cfg.Roles["builder"].Permissions)
	assert.Equal(t, []string{
		"builder@project.iam.gserviceaccount.com",
		"ci@project.iam.gserviceaccount.com",
	}, cfg.Roles["builder"].Principals)

	assert.Equal(t, "https://mettle.example.com", cfg.Google.Audience)

	assert.Equal(t, tokenFile, cfg.Static.File)
	assert.Equal(t, []string{CASWrite, ACWrite}, cfg.Roles["legacy"].Permissions)
}

func TestLoadConfigNoStaticFile(t *testing.T) {
	cfgFile := writeFile(t, t.TempDir(), "config.yaml", `
google:
  audience: aud
`)
	cfg, err := LoadConfig(cfgFile)
	require.NoError(t, err)
	assert.Equal(t, "aud", cfg.Google.Audience)
	assert.Empty(t, cfg.Static.File)
	assert.Empty(t, cfg.Roles)
}

func TestLoadConfigEmpty(t *testing.T) {
	cfgFile := writeFile(t, t.TempDir(), "config.yaml", "")
	cfg, err := LoadConfig(cfgFile)
	require.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Empty(t, cfg.Roles)
}

func TestLoadConfigMissingFile(t *testing.T) {
	_, err := LoadConfig(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	assert.ErrorContains(t, err, "failed to load config")
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestLoadConfigInvalidYAML(t *testing.T) {
	cfgFile := writeFile(t, t.TempDir(), "config.yaml", "roles: [this is not a map")
	_, err := LoadConfig(cfgFile)
	assert.ErrorContains(t, err, "failed to parse config")
}

func TestLoadConfigWrongType(t *testing.T) {
	cfgFile := writeFile(t, t.TempDir(), "config.yaml", `
roles:
  - anonymous
`)
	_, err := LoadConfig(cfgFile)
	assert.ErrorContains(t, err, "failed to parse config")
}

func TestLoadConfigUnknownKey(t *testing.T) {
	cfgFile := writeFile(t, t.TempDir(), "config.yaml", `
roles:
  builder:
    permisions:
      - cas:write
`)
	_, err := LoadConfig(cfgFile)
	assert.ErrorContains(t, err, "failed to parse config")
	assert.ErrorContains(t, err, "field permisions not found")
}

func TestLoadConfigUnknownTopLevelKey(t *testing.T) {
	cfgFile := writeFile(t, t.TempDir(), "config.yaml", `
gogle:
  audience: aud
`)
	_, err := LoadConfig(cfgFile)
	assert.ErrorContains(t, err, "field gogle not found")
}

func TestLoadConfigCommentsOnly(t *testing.T) {
	cfgFile := writeFile(t, t.TempDir(), "config.yaml", "# nothing here\n")
	cfg, err := LoadConfig(cfgFile)
	require.NoError(t, err)
	assert.Empty(t, cfg.Roles)
}

func TestMustLoadConfig(t *testing.T) {
	cfgFile := writeFile(t, t.TempDir(), "config.yaml", `
google:
  audience: aud
`)
	assert.Equal(t, "aud", MustLoadConfig(cfgFile).Google.Audience)
}

func TestMustLoadConfigPanics(t *testing.T) {
	assert.Panics(t, func() {
		MustLoadConfig(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	})
}

// testConfig builds a Config from YAML, with the static token file pointing at tokenFile if non-empty.
func testConfig(t *testing.T, tokenFile string) *Config {
	t.Helper()
	cfg := &Config{}
	require.NoError(t, yaml.Unmarshal([]byte(`
roles:
  anonymous:
    permissions:
      - cas:read
      - capabilities
    principals:
      - anonymous
  builder:
    permissions:
      - cas:read
      - cas:write
      - execute:write
    principals:
      - builder@project.iam.gserviceaccount.com
  legacy:
    permissions:
      - cas:read
      - cas:write
      - ac:write
    principals:
      - legacy
`), cfg))
	cfg.Static.File = tokenFile
	return cfg
}

func withAuth(header ...string) context.Context {
	md := metadata.MD{}
	for _, h := range header {
		md.Append("authorization", h)
	}
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestNewAuthenticator(t *testing.T) {
	tokenFile := writeFile(t, t.TempDir(), "token", "  s3cr3t\n")
	a, err := NewAuthenticator(context.Background(), testConfig(t, tokenFile))
	require.NoError(t, err)
	assert.Equal(t, "s3cr3t", a.token)
	assert.Equal(t, map[string][]string{
		"anonymous": {CASRead, Capabilities},
		"builder@project.iam.gserviceaccount.com": {CASRead, CASWrite, ExecuteWrite},
		"legacy": {CASRead, CASWrite, ACWrite},
	}, a.principals)
	assert.Nil(t, a.validator)
	assert.Nil(t, a.lruCache)
}

func TestNewAuthenticatorMissingTokenFile(t *testing.T) {
	_, err := NewAuthenticator(context.Background(), testConfig(t, filepath.Join(t.TempDir(), "nonexistent")))
	assert.ErrorContains(t, err, "failed to read static token file")
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestNewAuthenticatorDuplicatePrincipal(t *testing.T) {
	cfg := testConfig(t, "")
	role := cfg.Roles["builder"]
	role.Principals = append(role.Principals, "legacy")
	cfg.Roles["builder"] = role
	_, err := NewAuthenticator(context.Background(), cfg)
	assert.ErrorContains(t, err, "principal legacy is defined on multiple roles")
}

func TestNewAuthenticatorGoogle(t *testing.T) {
	cfg := testConfig(t, "")
	cfg.Google.Audience = "https://mettle.example.com"
	a, err := NewAuthenticator(context.Background(), cfg)
	require.NoError(t, err)
	assert.NotNil(t, a.validator)
	assert.NotNil(t, a.lruCache)
}

func TestMustNewAuthenticator(t *testing.T) {
	assert.NotNil(t, MustNewAuthenticator(context.Background(), testConfig(t, "")))
}

func TestMustNewAuthenticatorPanics(t *testing.T) {
	assert.Panics(t, func() {
		MustNewAuthenticator(context.Background(), testConfig(t, filepath.Join(t.TempDir(), "nonexistent")))
	})
}

func TestAuthRPCAnonymous(t *testing.T) {
	a := MustNewAuthenticator(context.Background(), testConfig(t, ""))
	// No metadata at all
	assert.NoError(t, a.AuthRPC(context.Background(), "/google.bytestream.ByteStream/Read"))
	// Metadata but no authorization header
	assert.NoError(t, a.AuthRPC(withAuth(), "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities"))
	err := a.AuthRPC(context.Background(), "/google.bytestream.ByteStream/Write")
	assert.ErrorContains(t, err, "principal anonymous is not authorized")
}

func TestAuthRPCNoAnonymousRole(t *testing.T) {
	cfg := testConfig(t, "")
	delete(cfg.Roles, "anonymous")
	a := MustNewAuthenticator(context.Background(), cfg)
	err := a.AuthRPC(context.Background(), "/google.bytestream.ByteStream/Read")
	assert.ErrorContains(t, err, "principal anonymous is not authorized")
}

func TestAuthRPCUnregisteredMethod(t *testing.T) {
	a := MustNewAuthenticator(context.Background(), testConfig(t, ""))
	err := a.AuthRPC(context.Background(), "/some.Service/Method")
	assert.ErrorContains(t, err, "unregistered method /some.Service/Method")
}

func TestAuthRPCStaticToken(t *testing.T) {
	tokenFile := writeFile(t, t.TempDir(), "token", "s3cr3t\n")
	a := MustNewAuthenticator(context.Background(), testConfig(t, tokenFile))
	ctx := withAuth("Bearer s3cr3t")
	assert.NoError(t, a.AuthRPC(ctx, "/google.bytestream.ByteStream/Write"))
	assert.NoError(t, a.AuthRPC(ctx, "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult"))
	err := a.AuthRPC(ctx, "/build.bazel.remote.execution.v2.Execution/Execute")
	assert.ErrorContains(t, err, "principal legacy is not authorized")
}

func TestAuthRPCBadHeaders(t *testing.T) {
	tokenFile := writeFile(t, t.TempDir(), "token", "s3cr3t")
	a := MustNewAuthenticator(context.Background(), testConfig(t, tokenFile))
	const method = "/google.bytestream.ByteStream/Read"

	err := a.AuthRPC(withAuth("Bearer s3cr3t", "Bearer s3cr3t"), method)
	assert.ErrorContains(t, err, "must provide exactly 1 authorization header (was 2)")

	err = a.AuthRPC(withAuth("Basic s3cr3t"), method)
	assert.ErrorContains(t, err, "not a bearer token")

	err = a.AuthRPC(withAuth("s3cr3t"), method)
	assert.ErrorContains(t, err, "not a bearer token")
}

func TestAuthRPCWrongTokenNoValidator(t *testing.T) {
	tokenFile := writeFile(t, t.TempDir(), "token", "s3cr3t")
	a := MustNewAuthenticator(context.Background(), testConfig(t, tokenFile))
	err := a.AuthRPC(withAuth("Bearer wrong"), "/google.bytestream.ByteStream/Read")
	assert.ErrorContains(t, err, "no validator configured")
}

// newGoogleAuthenticator returns an Authenticator with Google validation configured and the given tokens pre-cached.
func newGoogleAuthenticator(t *testing.T, tokens map[string]*idtoken.Payload) *Authenticator {
	t.Helper()
	cfg := testConfig(t, "")
	cfg.Google.Audience = "https://mettle.example.com"
	a := MustNewAuthenticator(context.Background(), cfg)
	for tok, payload := range tokens {
		a.lruCache.Add(tok, payload)
	}
	return a
}

func TestAuthRPCGoogleToken(t *testing.T) {
	expiry := time.Now().Add(time.Hour).Unix()
	a := newGoogleAuthenticator(t, map[string]*idtoken.Payload{
		"builder": {Expires: expiry, Claims: map[string]any{"email": "builder@project.iam.gserviceaccount.com"}},
		"unknown": {Expires: expiry, Claims: map[string]any{"email": "someone@example.com"}},
	})

	assert.NoError(t, a.AuthRPC(withAuth("Bearer builder"), "/build.bazel.remote.execution.v2.Execution/Execute"))
	assert.NoError(t, a.AuthRPC(withAuth("Bearer builder"), "/google.bytestream.ByteStream/Write"))
	err := a.AuthRPC(withAuth("Bearer builder"), "/build.bazel.remote.execution.v2.ActionCache/UpdateActionResult")
	assert.ErrorContains(t, err, "principal builder@project.iam.gserviceaccount.com is not authorized")

	err = a.AuthRPC(withAuth("Bearer unknown"), "/google.bytestream.ByteStream/Read")
	assert.ErrorContains(t, err, "principal someone@example.com is not authorized")
}

func TestAuthRPCGoogleTokenBadClaims(t *testing.T) {
	expiry := time.Now().Add(time.Hour).Unix()
	a := newGoogleAuthenticator(t, map[string]*idtoken.Payload{
		"noemail":  {Expires: expiry, Claims: map[string]any{"sub": "12345"}},
		"intemail": {Expires: expiry, Claims: map[string]any{"email": 12345}},
	})
	const method = "/google.bytestream.ByteStream/Read"

	err := a.AuthRPC(withAuth("Bearer noemail"), method)
	assert.ErrorContains(t, err, "missing email claim")

	err = a.AuthRPC(withAuth("Bearer intemail"), method)
	assert.ErrorContains(t, err, "invalid type for email claim (was int)")
}

func TestAuthRPCGoogleTokenExpired(t *testing.T) {
	a := newGoogleAuthenticator(t, map[string]*idtoken.Payload{
		"expired": {
			Expires: time.Now().Add(-time.Minute).Unix(),
			Claims:  map[string]any{"email": "builder@project.iam.gserviceaccount.com"},
		},
	})
	err := a.AuthRPC(withAuth("Bearer expired"), "/google.bytestream.ByteStream/Read")
	assert.ErrorContains(t, err, "token has expired")
	assert.False(t, a.lruCache.Contains("expired"), "expired token should be evicted from the cache")
}

func TestAuthRPCGoogleTokenInvalid(t *testing.T) {
	a := newGoogleAuthenticator(t, nil)
	// Not a JWT, so this fails to parse before any network access is attempted.
	err := a.AuthRPC(withAuth("Bearer notajwt"), "/google.bytestream.ByteStream/Read")
	assert.ErrorContains(t, err, "failed to validate token")
	assert.False(t, a.lruCache.Contains("notajwt"))
}

func TestAuthRPCEmptyTokenIsNotLegacy(t *testing.T) {
	a := MustNewAuthenticator(context.Background(), testConfig(t, ""))
	err := a.AuthRPC(withAuth("Bearer "), "/google.bytestream.ByteStream/Read")
	assert.ErrorContains(t, err, "no validator configured")
}

func TestAuthRPCPermissionDeniedMessage(t *testing.T) {
	a := MustNewAuthenticator(context.Background(), testConfig(t, ""))
	err := a.AuthRPC(context.Background(), "/google.bytestream.ByteStream/Write")
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.Equal(t, "principal anonymous is not authorized to call /google.bytestream.ByteStream/Write (needs cas:write, has [cas:read capabilities])", status.Convert(err).Message())
}

func TestAuthRPCStatusCodes(t *testing.T) {
	tokenFile := writeFile(t, t.TempDir(), "token", "s3cr3t")
	a := MustNewAuthenticator(context.Background(), testConfig(t, tokenFile))
	g := newGoogleAuthenticator(t, map[string]*idtoken.Payload{
		"unknown": {Expires: time.Now().Add(time.Hour).Unix(), Claims: map[string]any{"email": "someone@example.com"}},
	})
	const read = "/google.bytestream.ByteStream/Read"
	const write = "/google.bytestream.ByteStream/Write"
	const execute = "/build.bazel.remote.execution.v2.Execution/Execute"
	for name, tc := range map[string]struct {
		auth   *Authenticator
		ctx    context.Context
		method string
		code   codes.Code
	}{
		"allowed":                {a, withAuth("Bearer s3cr3t"), write, codes.OK},
		"anonymous insufficient": {a, context.Background(), write, codes.Unauthenticated},
		"wrong token":            {a, withAuth("Bearer wrong"), read, codes.Unauthenticated},
		"not bearer":             {a, withAuth("Basic s3cr3t"), read, codes.Unauthenticated},
		"multiple headers":       {a, withAuth("Bearer s3cr3t", "Bearer s3cr3t"), read, codes.Unauthenticated},
		"legacy insufficient":    {a, withAuth("Bearer s3cr3t"), execute, codes.PermissionDenied},
		"principal without role": {g, withAuth("Bearer unknown"), read, codes.PermissionDenied},
		"unregistered method":    {a, withAuth("Bearer s3cr3t"), "/some.Service/Method", codes.PermissionDenied},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.code, status.Code(tc.auth.AuthRPC(tc.ctx, tc.method)))
		})
	}
}

func TestAuthRPCNilAuthenticator(t *testing.T) {
	var a *Authenticator
	assert.NoError(t, a.AuthRPC(context.Background(), "/google.bytestream.ByteStream/Write"))
	assert.NoError(t, a.AuthRPC(withAuth("Bearer whatever"), "/some.Service/Method"))
}
