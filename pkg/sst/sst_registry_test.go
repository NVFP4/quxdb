package sst

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFailedOpenAndRetirementStillRemovesTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-table")
	require.NoError(t, os.Mkdir(path, 0o755))
	meta := &Metadata{ID: 7, Path: path}
	registry := NewRegistry()

	require.Error(t, registry.Open([]*Metadata{meta}))

	registry.Retire([]*Metadata{meta})
	assert.NoDirExists(t, path)
	require.NoError(t, registry.Close())
}

func TestRetiredTableStaysReadableUntilViewRelease(t *testing.T) {
	registry := NewRegistry()
	meta := testTable(t, t.TempDir())
	view := requireView(t, registry, []*Metadata{meta})

	registry.Retire([]*Metadata{meta})
	assert.DirExists(t, meta.Path)
	assert.Equal(t, []byte("two"), tableValue(t, view.Table(meta.ID), []byte("b")))

	view.Release()
	requireNoDirEventually(t, meta.Path)
	require.NoError(t, registry.Close())
}

func TestRetirementWaitsForEveryView(t *testing.T) {
	registry := NewRegistry()
	meta := testTable(t, t.TempDir())
	first := requireView(t, registry, []*Metadata{meta})
	second := registry.View([]*Metadata{meta})

	registry.Retire([]*Metadata{meta})
	first.Release()
	assert.DirExists(t, meta.Path)
	second.Release()
	requireNoDirEventually(t, meta.Path)
	require.NoError(t, registry.Close())
}

func TestRetiringUnpinnedTableRemovesItImmediately(t *testing.T) {
	registry := NewRegistry()
	meta := testTable(t, t.TempDir())

	registry.Retire([]*Metadata{meta})
	assert.NoDirExists(t, meta.Path)
	require.NoError(t, registry.Close())
}

func TestViewPinsMultipleTablesIndependently(t *testing.T) {
	registry := NewRegistry()
	first := testTable(t, t.TempDir())
	second := testTable(t, t.TempDir())
	view := requireView(t, registry, []*Metadata{first, second})

	registry.Retire([]*Metadata{first})
	assert.DirExists(t, first.Path)
	assert.DirExists(t, second.Path)

	view.Release()
	requireNoDirEventually(t, first.Path)
	assert.DirExists(t, second.Path)
	require.NoError(t, registry.Close())
	assert.DirExists(t, second.Path)
}

func TestCloseWaitsForBackgroundRemoval(t *testing.T) {
	registry := NewRegistry()
	meta := testTable(t, t.TempDir())
	view := requireView(t, registry, []*Metadata{meta})

	registry.Retire([]*Metadata{meta})
	view.Release()
	require.NoError(t, registry.Close())
	assert.NoDirExists(t, meta.Path)
}

func tableValue(t *testing.T, table *SST, target []byte) []byte {
	t.Helper()
	cursor := table.Cursor(nil, nil)
	for {
		key, value, ok := cursor.Next()
		if !ok {
			require.NoError(t, cursor.Err())
			return nil
		}
		if bytes.Equal(key, target) {
			return value
		}
	}
}

// opens tables, so call once per table set and use registry.View for more views.
func requireView(t testing.TB, registry *Registry, tables []*Metadata) *View {
	t.Helper()
	require.NoError(t, registry.Open(tables))
	return registry.View(tables)
}

func requireNoDirEventually(t *testing.T, path string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := os.Stat(path)
		return errors.Is(err, fs.ErrNotExist)
	}, time.Second, time.Millisecond)
}

var testTableID atomic.Uint64

func testTable(t *testing.T, dir string) *Metadata {
	t.Helper()
	builder, err := NewBuilder(BuilderOpts{
		Dir:       dir,
		ID:        testTableID.Add(1),
		Level:     0,
		Keys:      2,
		SizeBytes: 64 << 10,
	})
	require.NoError(t, err)
	for _, record := range []Record{
		{OrderedKey: []byte("a"), FilterKey: []byte("a"), Value: []byte("one")},
		{OrderedKey: []byte("b"), FilterKey: []byte("b"), Value: []byte("two")},
	} {
		require.NoError(t, builder.Add(record), fmt.Sprintf("record %q", record.OrderedKey))
	}
	meta, err := builder.Finalize()
	require.NoError(t, err)
	return meta
}
