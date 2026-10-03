package sst_test

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashgorana/quxdb/pkg/sst"
)

func TestViewReadsTable(t *testing.T) {
	registry := sst.NewRegistry()
	meta := testTable(t, t.TempDir())
	view := requireView(t, registry, []*sst.Metadata{meta})

	assert.Equal(t, []byte("one"), tableValue(t, view.Table(meta.ID), []byte("a")))
	assert.Equal(t, []byte("two"), tableValue(t, view.Table(meta.ID), []byte("b")))

	view.Release()
	require.NoError(t, registry.Close())
	assert.DirExists(t, meta.Path)
}

func TestRetainedViewOutlivesCreatorRelease(t *testing.T) {
	registry := sst.NewRegistry()
	meta := testTable(t, t.TempDir())
	view := requireView(t, registry, []*sst.Metadata{meta})
	registry.Retire([]*sst.Metadata{meta})

	require.True(t, view.TryRetain())
	view.Release()
	assert.Equal(t, []byte("one"), tableValue(t, view.Table(meta.ID), []byte("a")))
	assert.DirExists(t, meta.Path)

	view.Release()
	assert.False(t, view.TryRetain())
	requireNoDirEventually(t, meta.Path)
	require.NoError(t, registry.Close())
}

func TestNextViewDropsTableForDeletion(t *testing.T) {
	registry := sst.NewRegistry()
	kept := testTable(t, t.TempDir())
	removed := testTable(t, t.TempDir())
	added := testTable(t, t.TempDir())
	base := requireView(t, registry, []*sst.Metadata{kept, removed})

	require.NoError(t, registry.Open([]*sst.Metadata{added}))
	next := registry.View([]*sst.Metadata{kept, added})
	assert.Equal(t, []byte("one"), tableValue(t, next.Table(added.ID), []byte("a")))

	registry.Retire([]*sst.Metadata{removed})
	base.Release()
	requireNoDirEventually(t, removed.Path)
	assert.Equal(t, []byte("one"), tableValue(t, next.Table(kept.ID), []byte("a")))

	next.Release()
	require.NoError(t, registry.Close())
	assert.DirExists(t, kept.Path)
	assert.DirExists(t, added.Path)
}

func TestFailedOpenLeavesExistingViewUsable(t *testing.T) {
	registry := sst.NewRegistry()
	kept := testTable(t, t.TempDir())
	invalid := &sst.Metadata{ID: 9, Path: filepath.Join(t.TempDir(), "missing")}
	base := requireView(t, registry, []*sst.Metadata{kept})

	require.Error(t, registry.Open([]*sst.Metadata{invalid}))
	registry.Retire([]*sst.Metadata{invalid})

	assert.Equal(t, []byte("one"), tableValue(t, base.Table(kept.ID), []byte("a")))
	base.Release()
	require.NoError(t, registry.Close())
	assert.DirExists(t, kept.Path)
}

func TestConcurrentRetainReleaseDelaysRetirementUntilAllRelease(t *testing.T) {
	registry := sst.NewRegistry()
	meta := testTable(t, t.TempDir())
	view := requireView(t, registry, []*sst.Metadata{meta})
	registry.Retire([]*sst.Metadata{meta})

	const readers = 32
	var wg sync.WaitGroup
	for range readers {
		require.True(t, view.TryRetain())
		wg.Go(func() {
			tableValue(t, view.Table(meta.ID), []byte("a"))
			view.Release()
		})
	}
	view.Release()
	wg.Wait()

	requireNoDirEventually(t, meta.Path)
	require.NoError(t, registry.Close())
}
