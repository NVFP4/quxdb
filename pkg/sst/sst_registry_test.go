package sst_test

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
	"github.com/yashgorana/quxdb/pkg/sst"
)

func TestFailedOpenAndRetirementStillRemovesTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-table")
	require.NoError(t, os.Mkdir(path, 0o755))
	meta := &sst.Metadata{ID: 7, Path: path}
	registry := sst.NewRegistry()

	require.Error(t, registry.Open([]*sst.Metadata{meta}))

	registry.Retire([]*sst.Metadata{meta})
	assert.NoDirExists(t, path)
	require.NoError(t, registry.Close())
}

func TestRetiredTableStaysReadableUntilViewRelease(t *testing.T) {
	registry := sst.NewRegistry()
	meta := testTable(t, t.TempDir())
	view := requireView(t, registry, []*sst.Metadata{meta})

	registry.Retire([]*sst.Metadata{meta})
	assert.DirExists(t, meta.Path)
	assert.Equal(t, []byte("two"), tableValue(t, view.Table(meta.ID), []byte("b")))

	view.Release()
	requireNoDirEventually(t, meta.Path)
	require.NoError(t, registry.Close())
}

func TestRetirementWaitsForEveryView(t *testing.T) {
	registry := sst.NewRegistry()
	meta := testTable(t, t.TempDir())
	first := requireView(t, registry, []*sst.Metadata{meta})
	second := registry.View([]*sst.Metadata{meta})

	registry.Retire([]*sst.Metadata{meta})
	first.Release()
	assert.DirExists(t, meta.Path)
	second.Release()
	requireNoDirEventually(t, meta.Path)
	require.NoError(t, registry.Close())
}

func TestRetiringUnpinnedTableRemovesItImmediately(t *testing.T) {
	registry := sst.NewRegistry()
	meta := testTable(t, t.TempDir())

	registry.Retire([]*sst.Metadata{meta})
	assert.NoDirExists(t, meta.Path)
	require.NoError(t, registry.Close())
}

func TestViewPinsMultipleTablesIndependently(t *testing.T) {
	registry := sst.NewRegistry()
	first := testTable(t, t.TempDir())
	second := testTable(t, t.TempDir())
	view := requireView(t, registry, []*sst.Metadata{first, second})

	registry.Retire([]*sst.Metadata{first})
	assert.DirExists(t, first.Path)
	assert.DirExists(t, second.Path)

	view.Release()
	requireNoDirEventually(t, first.Path)
	assert.DirExists(t, second.Path)
	require.NoError(t, registry.Close())
	assert.DirExists(t, second.Path)
}

func TestCloseWaitsForBackgroundRemoval(t *testing.T) {
	registry := sst.NewRegistry()
	meta := testTable(t, t.TempDir())
	view := requireView(t, registry, []*sst.Metadata{meta})

	registry.Retire([]*sst.Metadata{meta})
	view.Release()
	require.NoError(t, registry.Close())
	assert.NoDirExists(t, meta.Path)
}

func tableValue(t *testing.T, table *sst.SST, target []byte) []byte {
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
func requireView(t testing.TB, registry *sst.Registry, tables []*sst.Metadata) *sst.View {
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

func testTable(t *testing.T, dir string) *sst.Metadata {
	t.Helper()
	builder, err := sst.NewBuilder(sst.BuilderOpts{
		Dir:       dir,
		ID:        testTableID.Add(1),
		Level:     0,
		Keys:      2,
		SizeBytes: 64 << 10,
	})
	require.NoError(t, err)
	for _, record := range []sst.Record{
		{OrderedKey: []byte("a"), FilterKey: []byte("a"), Value: []byte("one")},
		{OrderedKey: []byte("b"), FilterKey: []byte("b"), Value: []byte("two")},
	} {
		require.NoError(t, builder.Add(record), fmt.Sprintf("record %q", record.OrderedKey))
	}
	meta, err := builder.Finalize()
	require.NoError(t, err)
	return meta
}
