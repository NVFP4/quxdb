package sst

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuilderReplacesLeftoversOfUncommittedID(t *testing.T) {
	dir := t.TempDir()
	first := buildTable(t, dir, 5)
	require.NoError(t, os.Mkdir(first.Path+".tmp", 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(first.Path+".tmp", "partial"), nil, 0o644))

	second := buildTable(t, dir, 5)

	assert.Equal(t, first.Path, second.Path)
	assert.NoDirExists(t, second.Path+".tmp")
	registry := NewRegistry()
	view := requireView(t, registry, []*Metadata{second})
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

func TestBuilderOversizedFirstRecord(t *testing.T) {
	dir := t.TempDir()
	big := bytes.Repeat([]byte("x"), 1<<20)
	builder, err := NewBuilder(BuilderOpts{Dir: dir, ID: 1, SizeBytes: 2 << 20})
	require.NoError(t, err)
	require.NoError(t, builder.Add(Record{OrderedKey: []byte("a"), FilterKey: []byte("a"), Value: big}))
	require.NoError(t, builder.Add(Record{OrderedKey: []byte("b"), FilterKey: []byte("b"), Value: []byte("two")}))
	meta, err := builder.Finalize()
	require.NoError(t, err)

	registry := NewRegistry()
	view := requireView(t, registry, []*Metadata{meta})
	assert.Equal(t, big, tableValue(t, view.Table(meta.ID), []byte("a")))
	assert.Equal(t, []byte("two"), tableValue(t, view.Table(meta.ID), []byte("b")))
	view.Release()
	require.NoError(t, registry.Close())
}

func buildTable(t *testing.T, dir string, id uint64) *Metadata {
	t.Helper()
	builder, err := NewBuilder(BuilderOpts{Dir: dir, ID: id, SizeBytes: 64 << 10})
	require.NoError(t, err)
	require.NoError(t, builder.Add(Record{OrderedKey: []byte("a"), FilterKey: []byte("a"), Value: []byte("one")}))
	meta, err := builder.Finalize()
	require.NoError(t, err)
	return meta
}
