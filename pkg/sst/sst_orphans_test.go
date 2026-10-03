package sst_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashgorana/quxdb/pkg/sst"
)

func TestRemoveOrphansKeepsLiveTablesAndForeignFiles(t *testing.T) {
	dir := t.TempDir()
	live := buildTable(t, dir, 1)
	orphan := buildTable(t, dir, 2)
	tablesDir := filepath.Dir(live.Path)
	liveTmp := live.Path + ".tmp"
	crashedTmp := filepath.Join(tablesDir, "00000000000000000003.tmp")
	foreign := filepath.Join(tablesDir, "notes.txt")
	require.NoError(t, os.Mkdir(liveTmp, 0o755))
	require.NoError(t, os.Mkdir(crashedTmp, 0o755))
	require.NoError(t, os.WriteFile(foreign, nil, 0o644))

	require.NoError(t, sst.RemoveOrphans(dir, []*sst.Metadata{live}))

	assert.NoDirExists(t, orphan.Path)
	assert.NoDirExists(t, liveTmp)
	assert.NoDirExists(t, crashedTmp)
	assert.FileExists(t, foreign)
	registry := sst.NewRegistry()
	view := requireView(t, registry, []*sst.Metadata{live})
	assert.Equal(t, []byte("one"), tableValue(t, view.Table(live.ID), []byte("a")))
	view.Release()
	require.NoError(t, registry.Close())
}

func TestRemoveOrphansWithoutTablesDir(t *testing.T) {
	require.NoError(t, sst.RemoveOrphans(t.TempDir(), nil))
}
