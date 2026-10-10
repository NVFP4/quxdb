package sst

import (
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testLogger = slog.New(slog.DiscardHandler)

func TestOpenRejectsTruncatedFiles(t *testing.T) {
	for _, ext := range []string{".qdat", ".qidx", ".qfltr"} {
		t.Run(ext, func(t *testing.T) {
			info, err := os.Stat(tableFile(testTable(t, t.TempDir()), ext))
			require.NoError(t, err)

			for size := int64(0); size < info.Size(); size++ {
				meta := testTable(t, t.TempDir())
				require.NoError(t, os.Truncate(tableFile(meta, ext), size))

				registry := NewRegistry(testLogger)
				assert.Error(t, registry.Open([]*Metadata{meta}), "size %d", size)
				registry.Retire([]*Metadata{meta})
				require.NoError(t, registry.Close())
			}
		})
	}
}

func TestOpenRejectsMissingFiles(t *testing.T) {
	for _, ext := range []string{".qdat", ".qidx", ".qfltr"} {
		t.Run(ext, func(t *testing.T) {
			meta := testTable(t, t.TempDir())
			require.NoError(t, os.Remove(tableFile(meta, ext)))

			registry := NewRegistry(testLogger)
			assert.Error(t, registry.Open([]*Metadata{meta}))
			registry.Retire([]*Metadata{meta})
			require.NoError(t, registry.Close())
		})
	}
}

func TestOpenRejectsCorruptPayload(t *testing.T) {
	for _, ext := range []string{".qidx", ".qfltr"} {
		t.Run(ext, func(t *testing.T) {
			meta := testTable(t, t.TempDir())
			path := tableFile(meta, ext)
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			data[len(data)-footerLen-5] ^= 0x01 // last payload byte, before its crc and the footer
			require.NoError(t, os.WriteFile(path, data, 0o644))

			registry := NewRegistry(testLogger)
			require.ErrorIs(t, registry.Open([]*Metadata{meta}), ErrChecksumMismatch)
			registry.Retire([]*Metadata{meta})
			require.NoError(t, registry.Close())
		})
	}
}

func TestRemoveOrphansKeepsLiveTablesAndForeignFiles(t *testing.T) {
	dir := t.TempDir()
	live := buildTable(t, dir, 1)
	orphan := buildTable(t, dir, 2)
	liveTmp := live.Path + ".tmp"
	crashedTmp := filepath.Join(dir, tablesDir, sstName(3)+".tmp")
	foreign := filepath.Join(dir, tablesDir, "notes.txt")
	require.NoError(t, os.Mkdir(liveTmp, 0o755))
	require.NoError(t, os.Mkdir(crashedTmp, 0o755))
	require.NoError(t, os.WriteFile(foreign, nil, 0o644))

	require.NoError(t, RemoveOrphans(dir, []*Metadata{live}, testLogger))

	assert.NoDirExists(t, orphan.Path)
	assert.NoDirExists(t, liveTmp)
	assert.NoDirExists(t, crashedTmp)
	assert.FileExists(t, foreign)
	registry := NewRegistry(testLogger)
	view := requireView(t, registry, []*Metadata{live})
	assert.Equal(t, []byte("one"), tableValue(t, view.Table(live.ID), []byte("a")))
	view.Release()
	require.NoError(t, registry.Close())
}

func TestRemoveOrphansWithoutTablesDir(t *testing.T) {
	require.NoError(t, RemoveOrphans(t.TempDir(), nil, testLogger))
}

func tableFile(meta *Metadata, ext string) string {
	return filepath.Join(meta.Path, sstName(meta.ID)+ext)
}
