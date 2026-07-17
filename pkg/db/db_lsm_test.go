package db

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yashgorana/quxdb/pkg/sst"
	"github.com/yashgorana/quxdb/pkg/vset"
)

func TestLsmStateTableViewPinsRetiredTable(t *testing.T) {
	state, err := newLsmState(t.TempDir(), maxImt)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, state.close()) })

	table := buildLsmTestTable(t)
	flushed := state.rolloverMemtable(1, 1)
	require.NoError(t, state.replaceMemtablesWithSSTs([]*quxMemtable{flushed}, []*sst.Metadata{table}))

	oldSnapshot := state.acquireReadSnapshot()
	err = state.replaceSSTs(
		[]vset.Table{vset.Table(*table)},
		nil,
	)
	require.NoError(t, err)
	assert.DirExists(t, table.Path)

	_, err = state.store.Open(*table)
	require.ErrorIs(t, err, sst.ErrTableRetired)

	reader, err := oldSnapshot.openTable(*table)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	currentSnapshot := state.acquireReadSnapshot()
	require.NoError(t, oldSnapshot.release())
	assert.NoDirExists(t, table.Path)
	require.NoError(t, currentSnapshot.release())
}

func TestLsmStateAncientViewDoesNotBlockLaterTableRetirement(t *testing.T) {
	state, err := newLsmState(t.TempDir(), maxImt)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, state.close()) })

	first := buildLsmTestTable(t)
	flushed := state.rolloverMemtable(1, 1)
	require.NoError(t, state.replaceMemtablesWithSSTs([]*quxMemtable{flushed}, []*sst.Metadata{first}))
	ancientSnapshot := state.acquireReadSnapshot()

	second := buildLsmTestTable(t)
	flushed = state.rolloverMemtable(2, 2)
	require.NoError(t, state.replaceMemtablesWithSSTs([]*quxMemtable{flushed}, []*sst.Metadata{second}))
	laterSnapshot := state.acquireReadSnapshot()

	err = state.replaceSSTs(
		[]vset.Table{vset.Table(*second)},
		nil,
	)
	require.NoError(t, err)
	assert.DirExists(t, second.Path)

	require.NoError(t, laterSnapshot.release())
	assert.NoDirExists(t, second.Path)
	require.NoError(t, ancientSnapshot.release())
}

func TestLsmStateCompactionPublishesOutputAndRetiresInputs(t *testing.T) {
	state, err := newLsmState(t.TempDir(), maxImt)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, state.close()) })

	first := buildLsmTestTable(t)
	second := buildLsmTestTable(t)
	flushed := state.rolloverMemtable(1, 1)
	require.NoError(t, state.replaceMemtablesWithSSTs(
		[]*quxMemtable{flushed},
		[]*sst.Metadata{first, second},
	))
	oldSnapshot := state.acquireReadSnapshot()

	output := buildLsmTestTable(t)
	err = state.replaceSSTs(
		[]vset.Table{vset.Table(*first), vset.Table(*second)},
		[]*sst.Metadata{output},
	)
	require.NoError(t, err)
	assert.DirExists(t, first.Path)
	assert.DirExists(t, second.Path)

	currentSnapshot := state.acquireReadSnapshot()
	_, err = currentSnapshot.openTable(*first)
	require.Error(t, err)
	outputReader, err := currentSnapshot.openTable(*output)
	require.NoError(t, err)
	require.NoError(t, outputReader.Close())

	for _, table := range []*sst.Metadata{first, second} {
		reader, err := oldSnapshot.openTable(*table)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
	}
	require.NoError(t, oldSnapshot.release())
	assert.NoDirExists(t, first.Path)
	assert.NoDirExists(t, second.Path)
	assert.DirExists(t, output.Path)
	require.NoError(t, currentSnapshot.release())
}

func TestLsmStateRolloverReusesDiskSnapshot(t *testing.T) {
	state, err := newLsmState(t.TempDir(), maxImt)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, state.close()) })

	before := state.current.Load().tableVersion
	state.rolloverMemtable(1, 1)
	after := state.current.Load().tableVersion

	assert.Same(t, before, after)
}

func TestLsmStateConcurrentSnapshotAcquireAndRollover(t *testing.T) {
	state, err := newLsmState(t.TempDir(), maxImt)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, state.close()) })

	const (
		readerCount = 8
		iterations  = 200
	)
	errs := make(chan error, readerCount)
	var readers sync.WaitGroup
	for range readerCount {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for range iterations {
				snapshot := state.acquireReadSnapshot()
				if err := snapshot.release(); err != nil {
					errs <- err
					return
				}
			}
		}()
	}

	for i := range iterations {
		state.rolloverMemtable(uint64(i+1), 0)
	}
	readers.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, int64(1), state.current.Load().tableVersion.refs.Load())
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
