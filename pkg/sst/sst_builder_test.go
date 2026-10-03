package sst_test

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashgorana/quxdb/pkg/sst"
)

func TestBuilderReplacesLeftoversOfUncommittedID(t *testing.T) {
	dir := t.TempDir()
	first := buildTable(t, dir, 5)
	require.NoError(t, os.Mkdir(first.Path+".tmp", 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(first.Path+".tmp", "partial"), nil, 0o644))

	second := buildTable(t, dir, 5)

	assert.Equal(t, first.Path, second.Path)
	assert.NoDirExists(t, second.Path+".tmp")
	registry := sst.NewRegistry()
	view := requireView(t, registry, []*sst.Metadata{second})
	assert.Equal(t, []byte("one"), tableValue(t, view.Table(second.ID), []byte("a")))
	view.Release()
	require.NoError(t, registry.Close())
}

func TestTableDirsSortInIDOrder(t *testing.T) {
	dir := t.TempDir()
	ids := []uint64{9, 10, 100}
	var names []string
	for _, id := range ids {
		names = append(names, filepath.Base(buildTable(t, dir, id).Path))
	}

	assert.True(t, slices.IsSorted(names))
}

func buildTable(t *testing.T, dir string, id uint64) *sst.Metadata {
	t.Helper()
	builder, err := sst.NewBuilder(sst.BuilderOpts{Dir: dir, ID: id, Keys: 1, SizeBytes: 64 << 10})
	require.NoError(t, err)
	require.NoError(t, builder.Add(sst.Record{OrderedKey: []byte("a"), FilterKey: []byte("a"), Value: []byte("one")}))
	meta, err := builder.Finalize()
	require.NoError(t, err)
	return meta
}
