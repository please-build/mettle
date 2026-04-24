package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopyPlatformProperties(t *testing.T) {
	original := map[string]string{
		"filesystem":  "disk",
		"worker_pool": "large",
	}
	copied := copyPlatformProperties(original)

	assert.Equal(t, original, copied)

	original["filesystem"] = "tmpfs"
	assert.Equal(t, "disk", copied["filesystem"])
}

func TestCopyPlatformPropertiesNilWhenEmpty(t *testing.T) {
	assert.Nil(t, copyPlatformProperties(nil))
	assert.Nil(t, copyPlatformProperties(map[string]string{}))
}

func TestFormatPlatformProperties(t *testing.T) {
	props := map[string]string{
		"worker_pool": "large",
		"filesystem":  "disk",
	}

	assert.Equal(t, "filesystem=disk, worker_pool=large", formatPlatformProperties(props))
}

func TestFormatPlatformPropertiesEmpty(t *testing.T) {
	assert.Equal(t, "", formatPlatformProperties(nil))
	assert.Equal(t, "", formatPlatformProperties(map[string]string{}))
}
