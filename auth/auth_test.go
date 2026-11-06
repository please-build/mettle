package auth

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
google:
  audience: https://mettle.example.com
static:
  file: `+tokenFile+`
  permissions:
    - cas:write
    - ac:write
`)
	cfg, err := LoadConfig(cfgFile)
	require.NoError(t, err)

	require.Len(t, cfg.Roles, 2)
	assert.Equal(t, []string{CASRead, ACRead}, cfg.Roles["anonymous"].Permissions)
	assert.Empty(t, cfg.Roles["anonymous"].Principals)
	assert.Equal(t, []string{CASRead, CASWrite, ExecuteWrite}, cfg.Roles["builder"].Permissions)
	assert.Equal(t, []string{
		"builder@project.iam.gserviceaccount.com",
		"ci@project.iam.gserviceaccount.com",
	}, cfg.Roles["builder"].Principals)

	assert.Equal(t, "https://mettle.example.com", cfg.Google.Audience)

	assert.Equal(t, tokenFile, cfg.Static.File)
	assert.Equal(t, "s3cr3t", cfg.Static.Token)
	assert.Equal(t, []string{CASWrite, ACWrite}, cfg.Static.Permissions)
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
	assert.Empty(t, cfg.Static.Token)
	assert.Empty(t, cfg.Roles)
}

func TestLoadConfigEmpty(t *testing.T) {
	cfgFile := writeFile(t, t.TempDir(), "config.yaml", "")
	cfg, err := LoadConfig(cfgFile)
	require.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Empty(t, cfg.Roles)
}

func TestLoadConfigTokenIsNotReadFromYAML(t *testing.T) {
	cfgFile := writeFile(t, t.TempDir(), "config.yaml", `
static:
  token: should-be-ignored
`)
	cfg, err := LoadConfig(cfgFile)
	require.NoError(t, err)
	assert.Empty(t, cfg.Static.Token)
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

func TestLoadConfigMissingStaticTokenFile(t *testing.T) {
	dir := t.TempDir()
	cfgFile := writeFile(t, dir, "config.yaml", `
static:
  file: `+filepath.Join(dir, "nonexistent")+`
`)
	_, err := LoadConfig(cfgFile)
	assert.ErrorContains(t, err, "failed to read static token file")
	assert.ErrorIs(t, err, os.ErrNotExist)
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
