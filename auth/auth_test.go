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
	for name, tc := range map[string]struct {
		yaml     string
		expected *Config
	}{
		"full": {
			yaml: `
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
google:
  audience: https://mettle.example.com
  cache_size: 50
static:
  file: ` + tokenFile,
			expected: func() *Config {
				cfg := &Config{Roles: map[string]Role{
					"anonymous": {Permissions: []string{CASRead, ACRead}},
					"builder": {
						Permissions: []string{CASRead, CASWrite, ExecuteWrite},
						Principals: []string{
							"builder@project.iam.gserviceaccount.com",
							"ci@project.iam.gserviceaccount.com",
						},
					},
				}}
				cfg.Google.Audience = "https://mettle.example.com"
				cfg.Google.CacheSize = 50
				cfg.Static.File = tokenFile
				return cfg
			}(),
		},
		"roles only": {
			yaml: `
roles:
  anonymous:
    permissions:
      - cas:read
`,
			expected: &Config{Roles: map[string]Role{
				"anonymous": {Permissions: []string{CASRead}},
			}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := LoadConfig(writeFile(t, t.TempDir(), "config.yaml", tc.yaml))
			require.NoError(t, err)
			assert.Equal(t, tc.expected, cfg)
		})
	}
}

func TestLoadConfigErrors(t *testing.T) {
	for name, tc := range map[string]struct {
		yaml string
		err  string
	}{
		"empty":                 {"", "no roles defined in config"},
		"comments only":         {"# nothing here\n", "no roles defined in config"},
		"no roles":              {"google:\n  audience: aud\n", "no roles defined in config"},
		"invalid yaml":          {"roles: [this is not a map", "parse config"},
		"wrong type":            {"roles:\n  - anonymous\n", "parse config"},
		"unknown key":           {"roles:\n  builder:\n    permisions:\n      - cas:write\n", "field permisions not found"},
		"unknown top-level key": {"gogle:\n  audience: aud\n", "field gogle not found"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := LoadConfig(writeFile(t, t.TempDir(), "config.yaml", tc.yaml))
			assert.ErrorContains(t, err, tc.err)
		})
	}
}

func TestLoadConfigMissingFile(t *testing.T) {
	_, err := LoadConfig(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	assert.ErrorContains(t, err, "load config")
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestMustLoadConfig(t *testing.T) {
	cfgFile := writeFile(t, t.TempDir(), "config.yaml", `
roles:
  anonymous:
    permissions:
      - cas:read
`)
	assert.Equal(t, []string{CASRead}, MustLoadConfig(cfgFile).Roles["anonymous"].Permissions)
}

func TestMustLoadConfigPanics(t *testing.T) {
	assert.Panics(t, func() {
		MustLoadConfig(filepath.Join(t.TempDir(), "nonexistent.yaml"))
	})
}

// Test methods, named for the permission they require.
const (
	casReadMethod      = "/test.Service/CASRead"
	casWriteMethod     = "/test.Service/CASWrite"
	acWriteMethod      = "/test.Service/ACWrite"
	executeWriteMethod = "/test.Service/ExecuteWrite"
	unregisteredMethod = "/test.Service/Unregistered"
)

var testMethods = map[string]string{
	casReadMethod:      CASRead,
	casWriteMethod:     CASWrite,
	acWriteMethod:      ACWrite,
	executeWriteMethod: ExecuteWrite,
}

const builderPrincipal = "builder@project.iam.gserviceaccount.com"

// testConfig returns a Config with the static token file pointing at tokenFile if non-empty.
func testConfig(tokenFile string) *Config {
	cfg := &Config{Roles: map[string]Role{
		"anonymous": {
			Permissions: []string{CASRead},
			Principals:  []string{anonymous},
		},
		"builder": {
			Permissions: []string{CASRead, CASWrite, ExecuteWrite},
			Principals:  []string{builderPrincipal},
		},
		"legacy": {
			Permissions: []string{CASRead, CASWrite, ACWrite},
			Principals:  []string{legacy},
		},
	}}
	cfg.Static.File = tokenFile
	return cfg
}

// newTestAuthenticator creates an Authenticator from the given config that authorises testMethods.
func newTestAuthenticator(t *testing.T, cfg *Config) *Authenticator {
	t.Helper()
	a, err := NewAuthenticator(t.Context(), cfg)
	require.NoError(t, err)
	a.methods = testMethods
	return a
}

// newGoogleAuthenticator returns a test Authenticator with Google validation configured and the given tokens pre-cached.
func newGoogleAuthenticator(t *testing.T, tokens map[string]*idtoken.Payload) *Authenticator {
	t.Helper()
	cfg := testConfig("")
	cfg.Google.Audience = "https://mettle.example.com"
	a := newTestAuthenticator(t, cfg)
	for tok, payload := range tokens {
		a.lruCache.Add(tok, payload)
	}
	return a
}

func withAuth(t *testing.T, header ...string) context.Context {
	md := metadata.MD{}
	for _, h := range header {
		md.Append("authorization", h)
	}
	return metadata.NewIncomingContext(t.Context(), md)
}

func TestNewAuthenticator(t *testing.T) {
	tokenFile := writeFile(t, t.TempDir(), "token", "  s3cr3t\n")
	a, err := NewAuthenticator(t.Context(), testConfig(tokenFile))
	require.NoError(t, err)
	assert.Equal(t, "s3cr3t", a.token)
	assert.Equal(t, authenticatedMethods, a.methods)
	assert.Equal(t, map[string][]string{
		anonymous:        {CASRead},
		builderPrincipal: {CASRead, CASWrite, ExecuteWrite},
		legacy:           {CASRead, CASWrite, ACWrite},
	}, a.principals)
	assert.Nil(t, a.validator)
	assert.Nil(t, a.lruCache)
}

func TestNewAuthenticatorMissingTokenFile(t *testing.T) {
	_, err := NewAuthenticator(t.Context(), testConfig(filepath.Join(t.TempDir(), "nonexistent")))
	assert.ErrorContains(t, err, "read static token file")
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestNewAuthenticatorDuplicatePrincipal(t *testing.T) {
	cfg := testConfig("")
	role := cfg.Roles["builder"]
	role.Principals = append(role.Principals, legacy)
	cfg.Roles["builder"] = role
	_, err := NewAuthenticator(t.Context(), cfg)
	assert.ErrorContains(t, err, "principal legacy is defined on multiple roles")
}

func TestNewAuthenticatorGoogle(t *testing.T) {
	cfg := testConfig("")
	cfg.Google.Audience = "https://mettle.example.com"
	a, err := NewAuthenticator(t.Context(), cfg)
	require.NoError(t, err)
	assert.NotNil(t, a.validator)
	assert.NotNil(t, a.lruCache)
}

func TestMustNewAuthenticator(t *testing.T) {
	assert.NotNil(t, MustNewAuthenticator(t.Context(), testConfig("")))
}

func TestMustNewAuthenticatorPanics(t *testing.T) {
	assert.Panics(t, func() {
		MustNewAuthenticator(t.Context(), testConfig(filepath.Join(t.TempDir(), "nonexistent")))
	})
}

func TestAuthRPC(t *testing.T) {
	tokenFile := writeFile(t, t.TempDir(), "token", "s3cr3t\n")
	static := newTestAuthenticator(t, testConfig(tokenFile))
	noAnonCfg := testConfig("")
	delete(noAnonCfg.Roles, "anonymous")
	noAnon := newTestAuthenticator(t, noAnonCfg)
	expiry := time.Now().Add(time.Hour).Unix()
	google := newGoogleAuthenticator(t, map[string]*idtoken.Payload{
		"builder":  {Expires: expiry, Claims: map[string]any{"email": builderPrincipal}},
		"unknown":  {Expires: expiry, Claims: map[string]any{"email": "someone@example.com"}},
		"noemail":  {Expires: expiry, Claims: map[string]any{"sub": "12345"}},
		"intemail": {Expires: expiry, Claims: map[string]any{"email": 12345}},
	})

	for name, tc := range map[string]struct {
		auth   *Authenticator
		ctx    context.Context
		method string
		code   codes.Code
		err    string
	}{
		"anonymous without metadata": {static, t.Context(), casReadMethod, codes.OK, ""},
		"anonymous without header":   {static, withAuth(t), casReadMethod, codes.OK, ""},
		"anonymous insufficient": {static, t.Context(), casWriteMethod, codes.Unauthenticated,
			"principal anonymous is not authorized to call /test.Service/CASWrite (needs cas:write, has [cas:read])"},
		"no anonymous role":   {noAnon, t.Context(), casReadMethod, codes.Unauthenticated, "principal anonymous is not authorized"},
		"unregistered method": {static, withAuth(t, "Bearer s3cr3t"), unregisteredMethod, codes.PermissionDenied, "unregistered method /test.Service/Unregistered"},
		"static token":        {static, withAuth(t, "Bearer s3cr3t"), casWriteMethod, codes.OK, ""},
		"static token ac":     {static, withAuth(t, "Bearer s3cr3t"), acWriteMethod, codes.OK, ""},
		"static token insufficient": {static, withAuth(t, "Bearer s3cr3t"), executeWriteMethod, codes.PermissionDenied,
			"principal legacy is not authorized to call /test.Service/ExecuteWrite (needs execute:write, has [cas:read, cas:write, ac:write])"},
		"multiple headers":         {static, withAuth(t, "Bearer s3cr3t", "Bearer s3cr3t"), casReadMethod, codes.Unauthenticated, "must provide exactly 1 authorization header (was 2)"},
		"basic auth":               {static, withAuth(t, "Basic s3cr3t"), casReadMethod, codes.Unauthenticated, "not a bearer token"},
		"no scheme":                {static, withAuth(t, "s3cr3t"), casReadMethod, codes.Unauthenticated, "not a bearer token"},
		"wrong token no validator": {static, withAuth(t, "Bearer wrong"), casReadMethod, codes.Unauthenticated, "no validator configured"},
		"empty token is not legacy": {newTestAuthenticator(t, testConfig("")), withAuth(t, "Bearer "), casReadMethod, codes.Unauthenticated,
			"no validator configured"},
		"google token":         {google, withAuth(t, "Bearer builder"), executeWriteMethod, codes.OK, ""},
		"google token write":   {google, withAuth(t, "Bearer builder"), casWriteMethod, codes.OK, ""},
		"google insufficient":  {google, withAuth(t, "Bearer builder"), acWriteMethod, codes.PermissionDenied, "principal builder@project.iam.gserviceaccount.com is not authorized"},
		"google without role":  {google, withAuth(t, "Bearer unknown"), casReadMethod, codes.PermissionDenied, "principal someone@example.com is not authorized"},
		"google missing email": {google, withAuth(t, "Bearer noemail"), casReadMethod, codes.Unauthenticated, "missing email claim"},
		"google int email":     {google, withAuth(t, "Bearer intemail"), casReadMethod, codes.Unauthenticated, "invalid type for email claim (was int)"},
	} {
		t.Run(name, func(t *testing.T) {
			err := tc.auth.AuthRPC(tc.ctx, tc.method)
			assert.Equal(t, tc.code, status.Code(err))
			if tc.err == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.err)
			}
		})
	}
}

func TestAuthRPCGoogleTokenExpired(t *testing.T) {
	a := newGoogleAuthenticator(t, map[string]*idtoken.Payload{
		"expired": {
			Expires: time.Now().Add(-time.Minute).Unix(),
			Claims:  map[string]any{"email": builderPrincipal},
		},
	})
	err := a.AuthRPC(withAuth(t, "Bearer expired"), casReadMethod)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.ErrorContains(t, err, "token has expired")
	assert.False(t, a.lruCache.Contains("expired"), "expired token should be evicted from the cache")
}

func TestAuthRPCGoogleTokenInvalid(t *testing.T) {
	a := newGoogleAuthenticator(t, nil)
	// Not a JWT, so this fails to parse before any network access is attempted; no concern of calling out to the Google JWKS server.
	err := a.AuthRPC(withAuth(t, "Bearer notajwt"), casReadMethod)
	assert.Equal(t, codes.Unauthenticated, status.Code(err))
	assert.ErrorContains(t, err, "validate token")
	assert.False(t, a.lruCache.Contains("notajwt"))
}

func TestAuthRPCNilAuthenticator(t *testing.T) {
	var a *Authenticator
	assert.NoError(t, a.AuthRPC(t.Context(), casWriteMethod))
	assert.NoError(t, a.AuthRPC(withAuth(t, "Bearer whatever"), unregisteredMethod))
}
