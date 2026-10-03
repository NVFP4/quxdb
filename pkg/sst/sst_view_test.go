package sst

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestViewReadsTable(t *testing.T) {
	registry := NewRegistry()
	meta := testTable(t, t.TempDir())
	view := requireView(t, registry, []*Metadata{meta})

	assert.Equal(t, []byte("one"), tableValue(t, view.Table(meta.ID), []byte("a")))
	assert.Equal(t, []byte("two"), tableValue(t, view.Table(meta.ID), []byte("b")))

	view.Release()
	require.NoError(t, registry.Close())
	assert.DirExists(t, meta.Path)
}

func TestRetainedViewOutlivesCreatorRelease(t *testing.T) {
	registry := NewRegistry()
	meta := testTable(t, t.TempDir())
	view := requireView(t, registry, []*Metadata{meta})
	registry.Retire([]*Metadata{meta})

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
	registry := NewRegistry()
	kept := testTable(t, t.TempDir())
	removed := testTable(t, t.TempDir())
	added := testTable(t, t.TempDir())
	base := requireView(t, registry, []*Metadata{kept, removed})

	require.NoError(t, registry.Open([]*Metadata{added}))
	next := registry.View([]*Metadata{kept, added})
	assert.Equal(t, []byte("one"), tableValue(t, next.Table(added.ID), []byte("a")))

	registry.Retire([]*Metadata{removed})
	base.Release()
	requireNoDirEventually(t, removed.Path)
	assert.Equal(t, []byte("one"), tableValue(t, next.Table(kept.ID), []byte("a")))

	next.Release()
	require.NoError(t, registry.Close())
	assert.DirExists(t, kept.Path)
	assert.DirExists(t, added.Path)
}

func TestFailedOpenLeavesExistingViewUsable(t *testing.T) {
	registry := NewRegistry()
	kept := testTable(t, t.TempDir())
	invalid := &Metadata{ID: 9, Path: filepath.Join(t.TempDir(), "missing")}
	base := requireView(t, registry, []*Metadata{kept})

	require.Error(t, registry.Open([]*Metadata{invalid}))
	registry.Retire([]*Metadata{invalid})

	assert.Equal(t, []byte("one"), tableValue(t, base.Table(kept.ID), []byte("a")))
	base.Release()
	require.NoError(t, registry.Close())
	assert.DirExists(t, kept.Path)
}

func TestConcurrentRetainReleaseDelaysRetirementUntilAllRelease(t *testing.T) {
	registry := NewRegistry()
	meta := testTable(t, t.TempDir())
	view := requireView(t, registry, []*Metadata{meta})
	registry.Retire([]*Metadata{meta})

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
