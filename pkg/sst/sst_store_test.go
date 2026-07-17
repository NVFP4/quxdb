package sst_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashgorana/quxdb/pkg/sst"
)

func TestViewDoesNotOpenTableUntilRequested(t *testing.T) {
	store := sst.NewStore()
	meta := sst.Metadata{ID: 1, Path: filepath.Join(t.TempDir(), "missing")}

	view := requireStoreView(t, store, []sst.Metadata{meta})
	require.NotNil(t, view)
	require.NoError(t, view.Release())
}

func TestViewTreatsDuplicateMetadataAsOneTable(t *testing.T) {
	store := sst.NewStore()
	meta := testStoreTable(t, t.TempDir())
	view := requireStoreView(t, store, []sst.Metadata{meta, meta, meta})

	require.NoError(t, store.RetireTable(meta))
	assert.DirExists(t, meta.Path)
	require.NoError(t, view.Release())
	assert.NoDirExists(t, meta.Path)
}

func TestStoreOpenReadsTable(t *testing.T) {
	store := sst.NewStore()
	meta := testStoreTable(t, t.TempDir())

	reader, err := store.Open(meta)
	require.NoError(t, err)
	cursor := reader.Cursor(nil, nil)
	key, value, found := cursor.Next()
	assert.True(t, found)
	assert.Equal(t, []byte("a"), key)
	assert.Equal(t, []byte("one"), value)
	assert.NoError(t, cursor.Err())

	require.NoError(t, reader.Close())
	require.NoError(t, store.Close())
	assert.DirExists(t, meta.Path)
}

func TestViewCanOpenSameTableMultipleTimes(t *testing.T) {
	store := sst.NewStore()
	meta := testStoreTable(t, t.TempDir())
	view := requireStoreView(t, store, []sst.Metadata{meta})

	first, err := view.Open(meta)
	require.NoError(t, err)
	second, err := view.Open(meta)
	require.NoError(t, err)

	assert.Equal(t, []byte("one"), tableValue(t, &first, []byte("a")))
	assert.Equal(t, []byte("two"), tableValue(t, &second, []byte("b")))

	require.NoError(t, first.Close())
	require.NoError(t, second.Close())
	require.NoError(t, view.Release())
	require.NoError(t, store.Close())
}

func TestViewRejectsTableOutsideView(t *testing.T) {
	store := sst.NewStore()
	first := testStoreTable(t, t.TempDir())
	second := testStoreTable(t, t.TempDir())
	view := requireStoreView(t, store, []sst.Metadata{first})

	_, err := view.Open(second)
	require.Error(t, err)

	require.NoError(t, view.Release())
	require.NoError(t, store.Close())
}

func TestViewCanOpenTableAfterRetirement(t *testing.T) {
	store := sst.NewStore()
	meta := testStoreTable(t, t.TempDir())
	view := requireStoreView(t, store, []sst.Metadata{meta})

	require.NoError(t, store.RetireTable(meta))
	assert.DirExists(t, meta.Path)

	_, err := store.Open(meta)
	require.ErrorIs(t, err, sst.ErrTableRetired)
	retiredView, err := store.View([]sst.Metadata{meta})
	require.ErrorIs(t, err, sst.ErrTableRetired)
	assert.Nil(t, retiredView)

	reader, err := view.Open(meta)
	require.NoError(t, err)
	assert.Equal(t, []byte("two"), tableValue(t, &reader, []byte("b")))

	require.NoError(t, reader.Close())
	assert.DirExists(t, meta.Path)
	require.NoError(t, view.Release())
	assert.NoDirExists(t, meta.Path)
}

func TestRetirementWaitsForOpenReader(t *testing.T) {
	store := sst.NewStore()
	meta := testStoreTable(t, t.TempDir())
	reader, err := store.Open(meta)
	require.NoError(t, err)

	require.NoError(t, store.RetireTable(meta))
	assert.DirExists(t, meta.Path)
	assert.Equal(t, []byte("one"), tableValue(t, &reader, []byte("a")))

	require.NoError(t, reader.Close())
	assert.NoDirExists(t, meta.Path)
}

func TestRepeatedRetirementDoesNotBypassExistingView(t *testing.T) {
	store := sst.NewStore()
	meta := testStoreTable(t, t.TempDir())
	view := requireStoreView(t, store, []sst.Metadata{meta})

	require.NoError(t, store.RetireTable(meta))
	require.NoError(t, store.RetireTable(meta))
	assert.DirExists(t, meta.Path)

	require.NoError(t, view.Release())
	assert.NoDirExists(t, meta.Path)
}

func TestRetirementWaitsForEveryView(t *testing.T) {
	store := sst.NewStore()
	meta := testStoreTable(t, t.TempDir())
	first := requireStoreView(t, store, []sst.Metadata{meta})
	second := requireStoreView(t, store, []sst.Metadata{meta})

	require.NoError(t, store.RetireTable(meta))
	require.NoError(t, first.Release())
	assert.DirExists(t, meta.Path)
	require.NoError(t, second.Release())
	assert.NoDirExists(t, meta.Path)
}

func TestRetiringUnopenedTableRemovesItImmediately(t *testing.T) {
	store := sst.NewStore()
	meta := testStoreTable(t, t.TempDir())

	require.NoError(t, store.RetireTable(meta))
	assert.NoDirExists(t, meta.Path)
}

func TestFailedViewOpenDoesNotPreventRetirement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "invalid-table")
	require.NoError(t, os.Mkdir(path, 0o755))
	meta := sst.Metadata{ID: 7, Path: path}
	store := sst.NewStore()
	view := requireStoreView(t, store, []sst.Metadata{meta})

	_, err := view.Open(meta)
	require.Error(t, err)

	require.NoError(t, store.RetireTable(meta))
	assert.DirExists(t, path)
	require.NoError(t, view.Release())
	assert.NoDirExists(t, path)
}

func TestViewPinsMultipleTablesIndependently(t *testing.T) {
	store := sst.NewStore()
	first := testStoreTable(t, t.TempDir())
	second := testStoreTable(t, t.TempDir())
	view := requireStoreView(t, store, []sst.Metadata{first, second})

	require.NoError(t, store.RetireTable(first))
	assert.DirExists(t, first.Path)
	assert.DirExists(t, second.Path)

	require.NoError(t, view.Release())
	assert.NoDirExists(t, first.Path)
	assert.DirExists(t, second.Path)
	require.NoError(t, store.Close())
	assert.DirExists(t, second.Path)
}

func TestConcurrentViewsDelayRetirementUntilAllRelease(t *testing.T) {
	store := sst.NewStore()
	meta := testStoreTable(t, t.TempDir())
	const viewCount = 32

	views := make([]*sst.TableView, viewCount)
	for i := range views {
		views[i] = requireStoreView(t, store, []sst.Metadata{meta})
	}
	require.NoError(t, store.RetireTable(meta))

	var wg sync.WaitGroup
	errCh := make(chan error, viewCount)
	for _, view := range views {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- view.Release()
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		assert.NoError(t, err)
	}
	assert.NoDirExists(t, meta.Path)
}

func tableValue(t *testing.T, reader *sst.Reader, target []byte) []byte {
	t.Helper()
	cursor := reader.Cursor(nil, nil)
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

func requireStoreView(t testing.TB, store *sst.Store, tables []sst.Metadata) *sst.TableView {
	t.Helper()
	view, err := store.View(tables)
	require.NoError(t, err)
	return view
}

func testStoreTable(t *testing.T, dir string) sst.Metadata {
	t.Helper()
	builder, err := sst.NewBuilder(sst.BuilderOpts{
		Dir:       dir,
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
	return *meta
}
