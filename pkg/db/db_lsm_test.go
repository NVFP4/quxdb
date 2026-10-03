package db

import (
	"errors"
	"io/fs"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yashgorana/quxdb/pkg/sst"
	"github.com/yashgorana/quxdb/pkg/wal"
)

func TestLsmStatePinnedViewKeepsRetiredTableReadable(t *testing.T) {
	state := newTestLsmState(t)
	table := buildLsmTestTable(t)
	flushed := state.rolloverMemtable(1, 1)
	require.NoError(t, state.replaceMemtablesWithSSTs([]*quxMemtable{flushed}, []*sst.Metadata{table}))

	oldView := state.acquire()
	require.NoError(t, state.replaceSSTs([]*sst.Metadata{table}, nil))
	assert.DirExists(t, table.Path)
	assert.Empty(t, state.currentVersion().All())

	_, _, found := oldView.tables.Table(table.ID).Lookup([]byte("key"), newSeekStart([]byte("key"), 1))
	assert.True(t, found)

	oldView.release()
	requireNoDirEventually(t, table.Path)
}

func TestLsmStateAncientViewDoesNotBlockLaterTableRetirement(t *testing.T) {
	state := newTestLsmState(t)

	first := buildLsmTestTable(t)
	flushed := state.rolloverMemtable(1, 1)
	require.NoError(t, state.replaceMemtablesWithSSTs([]*quxMemtable{flushed}, []*sst.Metadata{first}))
	ancientView := state.acquire()

	second := buildLsmTestTable(t)
	flushed = state.rolloverMemtable(2, 2)
	require.NoError(t, state.replaceMemtablesWithSSTs([]*quxMemtable{flushed}, []*sst.Metadata{second}))
	laterView := state.acquire()

	require.NoError(t, state.replaceSSTs([]*sst.Metadata{second}, nil))
	assert.DirExists(t, second.Path)

	laterView.release()
	requireNoDirEventually(t, second.Path)
	ancientView.release()
}

func TestLsmStateCompactionPublishesOutputAndRetiresInputs(t *testing.T) {
	state := newTestLsmState(t)

	first := buildLsmTestTable(t)
	second := buildLsmTestTable(t)
	flushed := state.rolloverMemtable(1, 1)
	require.NoError(t, state.replaceMemtablesWithSSTs(
		[]*quxMemtable{flushed},
		[]*sst.Metadata{first, second},
	))
	oldView := state.acquire()

	output := buildLsmTestTable(t)
	require.NoError(t, state.replaceSSTs([]*sst.Metadata{first, second}, []*sst.Metadata{output}))
	assert.DirExists(t, first.Path)
	assert.DirExists(t, second.Path)

	currentView := state.acquire()
	tableIDs := func(view *lsmView) []uint64 {
		var ids []uint64
		for _, table := range view.version.All() {
			ids = append(ids, table.ID)
		}
		return ids
	}
	assert.Equal(t, []uint64{output.ID}, tableIDs(currentView))
	assert.ElementsMatch(t, []uint64{first.ID, second.ID}, tableIDs(oldView))

	oldView.release()
	requireNoDirEventually(t, first.Path)
	requireNoDirEventually(t, second.Path)
	assert.DirExists(t, output.Path)
	currentView.release()
}

func TestLsmStateFlushRemovesOldestMemtables(t *testing.T) {
	state := newTestLsmState(t)
	oldest := state.rolloverMemtable(1, 1)
	middle := state.rolloverMemtable(2, 2)
	newest := state.rolloverMemtable(3, 3)

	flushable := state.flushableMemtables(1)
	require.Equal(t, []*quxMemtable{oldest, middle}, flushable)

	require.NoError(t, state.replaceMemtablesWithSSTs(flushable, []*sst.Metadata{buildLsmTestTable(t)}))
	view := state.current.Load()
	assert.Equal(t, []*quxMemtable{state.activeMemtable(), newest}, view.memtables)
	assert.Equal(t, uint64(2), view.version.Checkpoint().LastSeq)
}

func TestLsmStateConcurrentAcquireDuringEdits(t *testing.T) {
	state := newTestLsmState(t)

	const (
		readerCount = 8
		iterations  = 100
	)
	stop := make(chan struct{})
	var readers sync.WaitGroup
	t.Cleanup(func() {
		close(stop)
		readers.Wait()
	})
	for range readerCount {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				view := state.acquire()
				for _, table := range view.version.All() {
					view.tables.Table(table.ID).MayContain([]byte("key"))
				}
				view.release()
			}
		})
	}

	var retired []string
	for i := range iterations {
		table := buildLsmTestTable(t)
		flushed := state.rolloverMemtable(uint64(i+1), wal.LSN(i+1))
		require.NoError(t, state.replaceMemtablesWithSSTs([]*quxMemtable{flushed}, []*sst.Metadata{table}))
		require.NoError(t, state.replaceSSTs([]*sst.Metadata{table}, nil))
		retired = append(retired, table.Path)
	}
	for _, path := range retired {
		requireNoDirEventually(t, path)
	}
}

func newTestLsmState(t *testing.T) *lsmState {
	t.Helper()
	state, err := newLsmState(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, state.close()) })
	return state
}

func requireNoDirEventually(t *testing.T, path string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := os.Stat(path)
		return errors.Is(err, fs.ErrNotExist)
	}, time.Second, time.Millisecond)
}

func buildLsmTestTable(t *testing.T) *sst.Metadata {
	t.Helper()

	builder, err := sst.NewBuilder(sst.BuilderOpts{
		Dir:       t.TempDir(),
		Level:     0,
		Keys:      1,
		SizeBytes: 64,
	})
	require.NoError(t, err)
	finalized := false
	t.Cleanup(func() {
		if !finalized {
			_ = builder.Abort()
		}
	})

	key := newQuxKey([]byte("key"), 1, quxOpSet)
	require.NoError(t, builder.Add(sst.Record{
		OrderedKey: key,
		FilterKey:  key.UserKey(),
		Value:      []byte("value"),
	}))
	table, err := builder.Finalize()
	require.NoError(t, err)
	finalized = true
	return table
}
