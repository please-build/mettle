// Package auth defines common authentication primitives for Mettle.
package auth

import (
	"fmt"
	"os"
	"strings"

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
)

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
	} `yaml:"google"`

	// Legacy static token support
	Static struct {
		// The file to load the static token from
		File string `yaml:"file"`
		// The loaded token
		Token string `yaml:"-"`
		// The set of permissions this token grants
		Permissions []string `yaml:"permissions"`
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
	if cfg.Static.File != "" {
		token, err := os.ReadFile(cfg.Static.File)
		if err != nil {
			return nil, fmt.Errorf("failed to read static token file: %w", err)
		}
		cfg.Static.Token = strings.TrimSpace(string(token))
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
