package sst

import (
	"bytes"
	"encoding/binary"
	"fmt"
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

func TestBuilderReusedBufferWritesNoStaleBytes(t *testing.T) {
	dir := t.TempDir()
	builder, err := NewBuilder(BuilderOpts{Dir: dir, ID: 1, SizeBytes: 4 << 20})
	require.NoError(t, err)

	// shrinking non-zero values leave stale bytes behind in the reused block buffer
	var keys, vals [][]byte
	for i := range 400 {
		keys = append(keys, fmt.Appendf(nil, "key-%04d", i))
		vals = append(vals, bytes.Repeat([]byte{byte(i%255) + 1}, 8<<10-i*20))
		require.NoError(t, builder.Add(Record{OrderedKey: keys[i], FilterKey: keys[i], Value: vals[i]}))
	}
	meta, err := builder.Finalize()
	require.NoError(t, err)

	registry := NewRegistry()
	view := requireView(t, registry, []*Metadata{meta})
	table := view.Table(meta.ID)
	cursor := table.Cursor(nil, nil)
	for i := range keys {
		key, val, ok := cursor.Next()
		require.True(t, ok)
		require.Equal(t, keys[i], key)
		require.Equal(t, vals[i], val)

		_, val, ok, err = table.Seek(keys[i])
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, vals[i], val)
	}
	_, _, ok := cursor.Next()
	require.False(t, ok)
	require.NoError(t, cursor.Err())
	view.Release()
	require.NoError(t, registry.Close())

	// blocks are page aligned with zero padding, the last block is followed by the sst footer
	data, err := os.ReadFile(tableFile(meta, ".qdat"))
	require.NoError(t, err)
	blocks := 0
	for off := 0; ; blocks++ {
		end := off + int(binary.LittleEndian.Uint32(data[off+4:]))
		if end+sstFooterLen == len(data) {
			break
		}
		next := alignUpPage(end)
		require.Equal(t, make([]byte, next-end), data[end:next], "padding after block at %d", off)
		off = next
	}
	require.Greater(t, blocks, 10)
}

func TestSeekFindsRecordsNearBlockEnd(t *testing.T) {
	dir := t.TempDir()
	builder, err := NewBuilder(BuilderOpts{Dir: dir, ID: 1, SizeBytes: 64 << 10})
	require.NoError(t, err)

	// records shorter than the block header sit within its length of the record region end
	keys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
	for _, key := range keys {
		require.NoError(t, builder.Add(Record{OrderedKey: key, FilterKey: key, Value: key}))
	}
	meta, err := builder.Finalize()
	require.NoError(t, err)

	registry := NewRegistry()
	view := requireView(t, registry, []*Metadata{meta})
	for _, key := range keys {
		gotKey, val, ok, err := view.Table(meta.ID).Seek(key)
		require.NoError(t, err)
		require.True(t, ok, "seek %q", key)
		require.Equal(t, key, gotKey)
		require.Equal(t, key, val)
	}
	view.Release()
	require.NoError(t, registry.Close())
}

func TestAbortAfterFailedFinalizeRemovesTmpDir(t *testing.T) {
	dir := t.TempDir()
	builder, err := NewBuilder(BuilderOpts{Dir: dir, ID: 1, SizeBytes: 64 << 10})
	require.NoError(t, err)
	require.NoError(t, builder.Add(Record{OrderedKey: []byte("a"), FilterKey: []byte("a"), Value: []byte("one")}))

	// an existing index file fails its exclusive create
	require.NoError(t, os.WriteFile(filepath.Join(builder.sstDir, sstIndexName(1)), nil, 0o644))
	_, err = builder.Finalize()
	require.ErrorIs(t, err, os.ErrExist)

	require.NoError(t, builder.Abort())
	assert.NoDirExists(t, builder.sstDir)
	assert.NoDirExists(t, sstDirPath(dir, 1))
}

func TestFinalizeRejectsEmptyBuild(t *testing.T) {
	dir := t.TempDir()
	builder, err := NewBuilder(BuilderOpts{Dir: dir, ID: 1, SizeBytes: 64 << 10})
	require.NoError(t, err)

	_, err = builder.Finalize()
	require.ErrorIs(t, err, ErrEmptyBuild)

	require.NoError(t, builder.Abort())
	assert.NoDirExists(t, builder.sstDir)
	assert.NoDirExists(t, sstDirPath(dir, 1))
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
