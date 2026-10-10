package quxdb

import (
	"bytes"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/yashgorana/quxdb/pkg/sst"
	"github.com/yashgorana/quxdb/pkg/vset"
)

const (
	mib = int64(1) << 20
	gib = int64(1) << 30
	tib = int64(1) << 40
)

func testMeta(id uint64, level uint8, minKey, maxKey string, size int64) *sst.Metadata {
	return &sst.Metadata{ID: id, Level: level, Path: fmt.Sprint(id), MinKey: []byte(minKey), MaxKey: []byte(maxKey), SizeBytes: uint64(size)}
}

func testVersion(t *testing.T, tables ...*sst.Metadata) *vset.Version {
	t.Helper()
	vs, err := vset.New(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })
	for _, table := range tables {
		require.NoError(t, vs.Apply([]vset.Change{{Op: vset.OpAdd, Table: table}}))
	}
	return vs.CurrentVersion()
}

func tableIDs(tables []*sst.Metadata) []uint64 {
	var ids []uint64
	for _, table := range tables {
		ids = append(ids, table.ID)
	}
	return ids
}

func TestPickCompaction(t *testing.T) {
	t.Run("nothing below the triggers", func(t *testing.T) {
		_, inputs := pickCompaction(testVersion(t,
			testMeta(1, 0, "a", "c", mib), testMeta(2, 0, "d", "f", mib), testMeta(3, 0, "g", "i", mib),
			testMeta(4, 1, "a", "z", mib),
		), DefaultOptions().Compaction)
		assert.Empty(t, inputs)
	})

	t.Run("l0 trigger takes the oldest batch and overlapping l1", func(t *testing.T) {
		level, inputs := pickCompaction(testVersion(t,
			testMeta(1, 0, "b", "c", mib), testMeta(2, 0, "c", "d", mib), testMeta(3, 0, "d", "e", mib),
			testMeta(4, 0, "e", "f", mib), testMeta(5, 0, "x", "y", mib),
			testMeta(6, 1, "a", "b", mib), testMeta(7, 1, "f", "g", mib), testMeta(8, 1, "m", "n", mib),
		), DefaultOptions().Compaction)
		assert.Equal(t, 0, level)
		assert.Equal(t, []uint64{1, 2, 3, 4, 6, 7}, tableIDs(inputs))
	})

	t.Run("level over target picks the least overlapping table", func(t *testing.T) {
		// table 1 would rewrite 10 bytes below per byte moved, table 2 only half a byte
		level, inputs := pickCompaction(testVersion(t,
			testMeta(1, 1, "a", "c", 200*mib), testMeta(2, 1, "m", "n", 200*mib),
			testMeta(3, 2, "b", "d", 2000*mib), testMeta(4, 2, "m", "o", 100*mib),
		), DefaultOptions().Compaction)
		assert.Equal(t, 1, level)
		assert.Equal(t, []uint64{2, 4}, tableIDs(inputs))
	})

	t.Run("bottom level is never picked", func(t *testing.T) {
		_, inputs := pickCompaction(testVersion(t, testMeta(1, vset.MaxLevels-1, "a", "z", tib)), DefaultOptions().Compaction)
		assert.Empty(t, inputs)
	})
}

func TestLevelTargets(t *testing.T) {
	small := levelTargets(testVersion(t, testMeta(1, vset.MaxLevels-1, "a", "z", gib)), DefaultOptions().Compaction)
	assert.Equal(t, [vset.MaxLevels]int64{0, 256 * mib, 2560 * mib, 25600 * mib, 0}, small)

	// a 10 TiB bottom pulls every level up so each pair stays 10x apart
	large := levelTargets(testVersion(t, testMeta(1, vset.MaxLevels-1, "a", "z", 10*tib)), DefaultOptions().Compaction)
	assert.Equal(t, [vset.MaxLevels]int64{0, 10 * tib / 1000, 10 * tib / 100, 10 * tib / 10, 0}, large)
}

func TestCompactionMovesNonOverlappingL0Batch(t *testing.T) {
	state, compactor := newTestCompactor(t)
	var batch []*sst.Metadata
	for _, key := range []string{"a", "b", "c", "d"} {
		batch = append(batch, addCompactionTable(t, state, 0, set(key, 1, key)))
	}

	runCompaction(t, state, compactor, 0, batch...)

	version := state.currentVersion()
	assert.Empty(t, version.Level(0))
	assert.ElementsMatch(t, tableIDs(batch), tableIDs(version.Level(1)))
	for _, table := range version.Level(1) {
		assert.DirExists(t, table.Path)
	}
}

func TestCompactionKeepsNewestVersionsAndTombstonesOverOlderTables(t *testing.T) {
	state, compactor := newTestCompactor(t)
	deeper := addCompactionTable(t, state, 2, set("b", 1, "b1"))
	older := addCompactionTable(t, state, 0, set("a", 1, "a1"), set("b", 2, "b2"), set("c", 3, "c3"))
	newer := addCompactionTable(t, state, 0, set("a", 4, "a4"), del("b", 5))

	runCompaction(t, state, compactor, 0, older, newer)

	version := state.currentVersion()
	assert.Empty(t, version.Level(0))
	require.Len(t, version.Level(1), 1)
	// b@1 still sits in l2, so the tombstone must stay to shadow it
	assert.Equal(t, []string{"a@4=a4", "b@5=deleted", "c@3=c3"}, compactionEntries(t, state, version.Level(1)[0]))
	assert.Equal(t, []string{"b@1=b1"}, compactionEntries(t, state, deeper))
	requireNoDirEventually(t, older.Path)
	requireNoDirEventually(t, newer.Path)
}

func TestCompactionAboveBottomDropsTombstonesWithNothingOlderBelow(t *testing.T) {
	state, compactor := newTestCompactor(t)
	addCompactionTable(t, state, 2, set("x", 1, "x1")) // deeper but outside the inputs' range
	older := addCompactionTable(t, state, 0, set("a", 1, "a1"), set("b", 2, "b2"), set("c", 3, "c3"))
	newer := addCompactionTable(t, state, 0, set("a", 4, "a4"), del("b", 5))

	runCompaction(t, state, compactor, 0, older, newer)

	l1 := state.currentVersion().Level(1)
	require.Len(t, l1, 1)
	assert.Equal(t, []string{"a@4=a4", "c@3=c3"}, compactionEntries(t, state, l1[0]))
}

func TestCompactionKeepsTombstonesOverSameLevelTables(t *testing.T) {
	state, compactor := newTestCompactor(t)
	// overlapping l1 tables are allowed, the one left out still holds b@1
	addCompactionTable(t, state, 1, set("b", 1, "b1"))
	source := addCompactionTable(t, state, 1, set("a", 4, "a4"), del("b", 5))
	next := addCompactionTable(t, state, 2, set("a", 2, "a2")) // an l2 input forces a rewrite over a move

	runCompaction(t, state, compactor, 1, source, next)

	l2 := state.currentVersion().Level(2)
	require.Len(t, l2, 1)
	assert.NotEqual(t, next.ID, l2[0].ID)
	assert.Equal(t, []string{"a@4=a4", "b@5=deleted"}, compactionEntries(t, state, l2[0]))
}

func TestCompactionIntoBottomDropsTombstones(t *testing.T) {
	state, compactor := newTestCompactor(t)
	source := vset.MaxLevels - 2
	older := addCompactionTable(t, state, source, set("a", 1, "a1"), set("b", 2, "b2"), set("c", 3, "c3"))
	newer := addCompactionTable(t, state, source, set("a", 4, "a4"), del("b", 5))

	runCompaction(t, state, compactor, source, older, newer)

	bottom := state.currentVersion().Level(vset.MaxLevels - 1)
	require.Len(t, bottom, 1)
	assert.Equal(t, []string{"a@4=a4", "c@3=c3"}, compactionEntries(t, state, bottom[0]))
}

func TestCompactionMovesLoneTableWithoutRewriting(t *testing.T) {
	state, compactor := newTestCompactor(t)
	table := addCompactionTable(t, state, 1, set("a", 2, "a2"), set("a", 1, "a1"))

	runCompaction(t, state, compactor, 1, table)

	version := state.currentVersion()
	assert.Empty(t, version.Level(1))
	require.Len(t, version.Level(2), 1)
	moved := version.Level(2)[0]
	assert.Equal(t, table.ID, moved.ID)
	assert.Equal(t, table.Path, moved.Path)
	// a move keeps the file as written, older versions included
	assert.Equal(t, []string{"a@2=a2", "a@1=a1"}, compactionEntries(t, state, moved))
}

func TestCompactionRewritesLoneTableIntoBottom(t *testing.T) {
	state, compactor := newTestCompactor(t)
	source := vset.MaxLevels - 2
	table := addCompactionTable(t, state, source, set("a", 2, "a2"), set("a", 1, "a1"), del("b", 3))

	runCompaction(t, state, compactor, source, table)

	bottom := state.currentVersion().Level(vset.MaxLevels - 1)
	require.Len(t, bottom, 1)
	assert.NotEqual(t, table.ID, bottom[0].ID)
	assert.Equal(t, []string{"a@2=a2"}, compactionEntries(t, state, bottom[0]))
	requireNoDirEventually(t, table.Path)
}

func TestCompactionSplitsOutputsAtTargetSize(t *testing.T) {
	state, compactor := newTestCompactor(t, WithTableTargetBytes(128))
	var entries []entry
	for i := range 20 {
		entries = append(entries, set(fmt.Sprintf("k%02d", i), quxSeq(i+1), "0123456789abcdef"))
	}
	first := addCompactionTable(t, state, 0, entries[:10]...)
	second := addCompactionTable(t, state, 0, entries[10:]...)

	runCompaction(t, state, compactor, 0, first, second)

	outputs := state.currentVersion().Level(1)
	require.Greater(t, len(outputs), 1)
	var all []string
	for i, output := range outputs {
		if i > 0 {
			assert.Negative(t, bytes.Compare(outputs[i-1].MaxKey, output.MinKey), "outputs must not overlap")
		}
		all = append(all, compactionEntries(t, state, output)...)
	}
	assert.Len(t, all, 20)
}

func newTestCompactor(t *testing.T, opts ...Option) (*lsmState, *lsmCompactor) {
	t.Helper()
	o := DefaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	dir := t.TempDir()
	state, err := newLsmState(dir, o)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, state.close()) })
	return state, newLsmCompactor(dir, state, o, func() {})
}

// builds a table from entries and publishes it at level.
func addCompactionTable(t *testing.T, state *lsmState, level int, entries ...entry) *sst.Metadata {
	t.Helper()
	entries = slices.Clone(entries)
	slices.SortFunc(entries, func(a, b entry) int { return bytes.Compare(a.key, b.key) })

	builder, err := sst.NewBuilder(sst.BuilderOpts{
		Dir:       t.TempDir(),
		ID:        lsmTestTableID.Add(1),
		Level:     uint8(level),
		Keys:      uint64(len(entries)),
		SizeBytes: 64 << 10,
	})
	require.NoError(t, err)
	for _, e := range entries {
		require.NoError(t, builder.Add(sst.Record{OrderedKey: e.key, FilterKey: e.key.UserKey(), Value: []byte(e.value)}))
	}
	table, err := builder.Finalize()
	require.NoError(t, err)
	require.NoError(t, state.replaceSSTs(nil, []*sst.Metadata{table}))
	return table
}

func runCompaction(t *testing.T, state *lsmState, compactor *lsmCompactor, sourceLevel int, inputs ...*sst.Metadata) {
	t.Helper()
	require.NoError(t, compactor.execute(&compactionPlan{
		sourceLevel: sourceLevel,
		inputs:      inputs,
		view:        state.acquire(),
	}))
}

func compactionEntries(t *testing.T, state *lsmState, table *sst.Metadata) []string {
	t.Helper()
	view := state.acquire()
	defer view.release()
	return drain(t, view.tables.Table(table.ID).Iterator(nil, nil))
}

func TestEstimateOutputKeys(t *testing.T) {
	inputs := []*sst.Metadata{
		{Keys: 100, SizeBytes: 1000},
		{Keys: 100, SizeBytes: 1000},
		{Keys: 100, SizeBytes: 1000},
		{Keys: 100, SizeBytes: 1000},
	}

	// a large target can't hold more keys than the inputs have
	assert.Equal(t, uint64(400), estimateOutputKeys(inputs, 1<<20))
	// a small target holds what fits at the inputs' 10 bytes per key
	assert.Equal(t, uint64(100), estimateOutputKeys(inputs, 1000))
	// inputs without data fall back to their key count
	assert.Equal(t, uint64(2), estimateOutputKeys([]*sst.Metadata{{Keys: 2}}, 1000))
}
