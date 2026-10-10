package config

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testInner struct {
	MaxBatch int `yaml:"maxBatch"`
}

type testConfig struct {
	Host      string `yaml:"host"`
	Port      uint16 `yaml:"port"`
	Size      uint64 `yaml:"segmentSize"`
	testInner `yaml:",inline"`
	Nested    struct {
		L0FileTarget int `yaml:"l0FileTarget"`
	} `yaml:"nested"`
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "c.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))
	return path
}

func load(t *testing.T, cfg *testConfig, args ...string) error {
	t.Helper()
	return Load(cfg, "QT", flag.NewFlagSet("t", flag.ContinueOnError), args)
}

func TestLoadLayersFileThenEnvThenFlags(t *testing.T) {
	path := writeConfig(t, "host: file\nport: 1\nsegmentSize: 16MiB\nmaxBatch: 3\nnested:\n  l0FileTarget: 5\n")
	t.Setenv("QT_PORT", "2")
	t.Setenv("QT_NESTED_L0_FILE_TARGET", "6")

	cfg := testConfig{Host: "default", Port: 9}
	require.NoError(t, load(t, &cfg, "-config", path, "-port", "3", "-maxBatch", "4"))

	assert.Equal(t, "file", cfg.Host)
	assert.Equal(t, uint16(3), cfg.Port, "flag beats env and file")
	assert.Equal(t, uint64(16<<20), cfg.Size, "file sizes take unit suffixes")
	assert.Equal(t, 4, cfg.MaxBatch, "inline fields have no prefix")
	assert.Equal(t, 6, cfg.Nested.L0FileTarget, "env beats file")
}

func TestLoadKeepsDefaultsWithoutSources(t *testing.T) {
	cfg := testConfig{Host: "default", Size: 64 << 20}
	require.NoError(t, load(t, &cfg))
	assert.Equal(t, testConfig{Host: "default", Size: 64 << 20}, cfg)
}

func TestLoadRejectsBadInput(t *testing.T) {
	t.Run("unknown file key", func(t *testing.T) {
		path := writeConfig(t, "nested:\n  l0FileTargte: 1\n")
		require.ErrorContains(t, load(t, &testConfig{}, "-config", path), "unknown key nested.l0FileTargte")
	})
	t.Run("out of range flag", func(t *testing.T) {
		require.Error(t, load(t, &testConfig{}, "-port", "70000"))
	})
	t.Run("unit overflows field", func(t *testing.T) {
		require.Error(t, load(t, &testConfig{}, "-port", "64KiB"))
	})
	t.Run("bad env value", func(t *testing.T) {
		t.Setenv("QT_SEGMENT_SIZE", "64MB")
		require.ErrorContains(t, load(t, &testConfig{}), "QT_SEGMENT_SIZE")
	})
}
