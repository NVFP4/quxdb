package vset

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/yashgorana/quxdb/pkg/jsonl"
	"github.com/yashgorana/quxdb/pkg/sst"
)

func TestApplyDeleteByIDMatchesReplay(t *testing.T) {
	dir := t.TempDir()
	vs, err := New(dir)
	require.NoError(t, err)

	require.NoError(t, vs.Apply([]Change{{Op: OpAdd, Table: testTable(1, 0)}}))
	require.NoError(t, vs.Apply([]Change{{Op: OpDelete, Table: &sst.Metadata{ID: 1, Level: 1}}}))
	assertLevelIDs(t, vs.CurrentVersion().Level(0))
	assertLevelIDs(t, vs.CurrentVersion().Level(1))
	require.NoError(t, vs.Close())

	reopened, err := New(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assertLevelIDs(t, reopened.CurrentVersion().Level(0))
	assertLevelIDs(t, reopened.CurrentVersion().Level(1))
}

func TestApplyDuplicateAddByIDMatchesReplay(t *testing.T) {
	dir := t.TempDir()
	vs, err := New(dir)
	require.NoError(t, err)

	first := testTable(1, 0)
	second := testTable(1, 1)
	require.NoError(t, vs.Apply([]Change{{Op: OpAdd, Table: first}}))
	require.NoError(t, vs.Apply([]Change{{Op: OpAdd, Table: second}}))
	assertLevelIDs(t, vs.CurrentVersion().Level(0))
	assertLevelIDs(t, vs.CurrentVersion().Level(1), 1)
	assert.Equal(t, second.Path, vs.CurrentVersion().Level(1)[0].Path)
	require.NoError(t, vs.Close())

	reopened, err := New(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assertLevelIDs(t, reopened.CurrentVersion().Level(0))
	assertLevelIDs(t, reopened.CurrentVersion().Level(1), 1)
	assert.Equal(t, second.Path, reopened.CurrentVersion().Level(1)[0].Path)
}

func TestApplyOrdersNonL0TablesByKey(t *testing.T) {
	vs, err := New(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })

	for _, id := range []uint64{5, 1, 3} {
		require.NoError(t, vs.Apply([]Change{{Op: OpAdd, Table: testTable(id, 1)}}))
	}

	assertLevelIDs(t, vs.CurrentVersion().Level(1), 1, 3, 5)
}

func TestApplyAllowsOverlappingNonL0Tables(t *testing.T) {
	vs, err := New(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })

	first := testTable(1, 1)
	second := testTable(2, 1)
	second.MinKey = bytes.Clone(first.MaxKey)

	require.NoError(t, vs.Apply([]Change{{Op: OpAdd, Table: first}}))
	require.NoError(t, vs.Apply([]Change{{Op: OpAdd, Table: second}}))
	assertLevelIDs(t, vs.CurrentVersion().Level(1), 1, 2)
	assertLevelIDs(t, pointLookup(vs.CurrentVersion(), first.MaxKey), 1, 2)
}

func TestCandidatesScanL1PlusTables(t *testing.T) {
	vs, err := New(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })

	require.NoError(t, vs.Apply([]Change{{Op: OpAdd, Table: testTable(2, 0)}}))
	for _, id := range []uint64{5, 1, 3} {
		require.NoError(t, vs.Apply([]Change{{Op: OpAdd, Table: testTable(id, 1)}}))
	}

	assertLevelIDs(t, pointLookup(vs.CurrentVersion(), []byte{3, 4}), 3)
	assertLevelIDs(t, vs.CurrentVersion().RangeLookupCandidates([]byte{1, 4}, []byte{4, 5}), 2, 1, 3)
}

func TestApplyPublishesFreshVersion(t *testing.T) {
	vs, err := New(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })

	prev := vs.CurrentVersion()
	require.NoError(t, vs.Apply([]Change{{Op: OpAdd, Table: testTable(1, 0)}}))
	next := vs.CurrentVersion()

	assert.NotSame(t, prev, next)
	assertLevelIDs(t, prev.Level(0))
	assertLevelIDs(t, next.Level(0), 1)

	prev.levels[0] = []*sst.Metadata{testTable(99, 0)}
	assertLevelIDs(t, vs.CurrentVersion().Level(0), 1)
}

func TestCheckpointPersistsIndependentlyOfTables(t *testing.T) {
	dir := t.TempDir()
	vs, err := New(dir)
	require.NoError(t, err)

	checkpoint := Checkpoint{LastSeq: 17, LastLSN: 42}
	require.NoError(t, vs.Apply([]Change{
		{Op: OpAdd, Table: testTable(1, 0)},
		{Op: OpCheckpoint, Checkpoint: checkpoint},
	}))
	assert.Equal(t, checkpoint, vs.CurrentVersion().Checkpoint())

	require.NoError(t, vs.Apply([]Change{{Op: OpDelete, Table: &sst.Metadata{ID: 1}}}))
	assert.Equal(t, checkpoint, vs.CurrentVersion().Checkpoint())
	require.NoError(t, vs.Close())

	reopened, err := New(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.Empty(t, reopened.CurrentVersion().Level(0))
	assert.Equal(t, checkpoint, reopened.CurrentVersion().Checkpoint())
}

func TestTableIDsResumeAboveDeletedTablesAfterReopen(t *testing.T) {
	dir := t.TempDir()
	vs, err := New(dir)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), vs.NextTableID())

	require.NoError(t, vs.Apply([]Change{
		{Op: OpAdd, Table: testTable(2, 0)},
		{Op: OpAdd, Table: testTable(7, 0)},
	}))
	require.NoError(t, vs.Apply([]Change{{Op: OpDelete, Table: &sst.Metadata{ID: 7}}}))
	require.NoError(t, vs.Close())

	reopened, err := New(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	assert.Equal(t, uint64(8), reopened.NextTableID())
}

func TestApplyRejectsCheckpointRegression(t *testing.T) {
	dir := t.TempDir()
	vs, err := New(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })

	checkpoint := Checkpoint{LastSeq: 17, LastLSN: 42}
	require.NoError(t, vs.Apply([]Change{{Op: OpCheckpoint, Checkpoint: checkpoint}}))
	before, err := os.Stat(filepath.Join(dir, catFilename))
	require.NoError(t, err)

	err = vs.Apply([]Change{{
		Op:         OpCheckpoint,
		Checkpoint: Checkpoint{LastSeq: 16, LastLSN: 41},
	}})
	require.ErrorIs(t, err, ErrRecordCorrupt)
	assert.Equal(t, checkpoint, vs.CurrentVersion().Checkpoint())
	after, err := os.Stat(filepath.Join(dir, catFilename))
	require.NoError(t, err)
	assert.Equal(t, before.Size(), after.Size())
}

func TestApplyRequiresCheckpointLast(t *testing.T) {
	vs, err := New(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })

	err = vs.Apply([]Change{
		{Op: OpCheckpoint, Checkpoint: Checkpoint{LastSeq: 17, LastLSN: 42}},
		{Op: OpAdd, Table: testTable(1, 0)},
	})
	require.ErrorIs(t, err, ErrRecordCorrupt)
	assert.Zero(t, vs.CurrentVersion().Checkpoint())
	assert.Empty(t, vs.CurrentVersion().Level(0))
}

func TestNewDiscardsTornCheckpoint(t *testing.T) {
	dir := t.TempDir()
	add := mustEncodeRecord(t, addRecord(testTable(1, 0)))
	checkpoint := Checkpoint{LastSeq: 17, LastLSN: 42}
	torn := mustEncodeRecord(t, catalogRecord{Op: OpCheckpoint, Checkpoint: &checkpoint})
	writeCatalog(t, dir, add, torn[:len(torn)-1])

	vs, err := New(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })
	assertLevelIDs(t, vs.CurrentVersion().Level(0), 1)
	assert.Zero(t, vs.CurrentVersion().Checkpoint())

	st, err := os.Stat(filepath.Join(dir, catFilename))
	require.NoError(t, err)
	assert.Equal(t, int64(len(add)), st.Size())
}

func TestNewTruncatesTornCatalogTail(t *testing.T) {
	dir := t.TempDir()
	first := mustEncodeRecord(t, addRecord(testTable(1, 0)))
	torn := mustEncodeRecord(t, addRecord(testTable(2, 0)))
	writeCatalog(t, dir, first, torn[:len(torn)-1])

	vs, err := New(dir)
	require.NoError(t, err)
	assertLevelIDs(t, vs.CurrentVersion().Level(0), 1)
	require.NoError(t, vs.Close())

	st, err := os.Stat(filepath.Join(dir, catFilename))
	require.NoError(t, err)
	assert.Equal(t, int64(len(first)), st.Size())
}

func TestNewReturnsErrorForCorruptCatalogLine(t *testing.T) {
	dir := t.TempDir()
	first := mustEncodeRecord(t, addRecord(testTable(1, 0)))
	corrupt := []byte("{not-json}\n")
	third := mustEncodeRecord(t, addRecord(testTable(3, 0)))
	writeCatalog(t, dir, first, corrupt, third)

	vs, err := New(dir)
	require.Error(t, err)
	assert.Nil(t, vs)

	st, err := os.Stat(filepath.Join(dir, catFilename))
	require.NoError(t, err)
	assert.Equal(t, int64(len(first)+len(corrupt)+len(third)), st.Size())
}

func TestCatalogAppendAfterReplayUsesFileSize(t *testing.T) {
	dir := t.TempDir()
	firstRec := addRecord(testTable(1, 0))
	first := mustEncodeRecord(t, firstRec)
	writeCatalog(t, dir, first)

	vs, err := New(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })

	secondRec := addRecord(testTable(2, 0))
	secondRec.Table.Path = filepath.Join("sst", string(bytes.Repeat([]byte("x"), 257)))
	second := mustEncodeRecord(t, secondRec)
	require.NoError(t, vs.Apply([]Change{{Op: secondRec.Op, Table: secondRec.Table}}))

	st, err := os.Stat(filepath.Join(dir, catFilename))
	require.NoError(t, err)
	assert.Equal(t, int64(len(first)+len(second)), st.Size())
}

func TestAppendRollbackUsesFileSize(t *testing.T) {
	dir := t.TempDir()
	first := mustEncodeRecord(t, addRecord(testTable(1, 0)))
	writeCatalog(t, dir, first)

	cat, err := openCatalog(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.close() })
	require.NoError(t, cat.replay(func(catalogRecord) error { return nil }))

	fw := &failAfterWriter{w: cat.fd, remaining: 8}
	cat.bw = bufio.NewWriterSize(fw, catalogBufferSize)
	cat.jw = jsonl.NewWriter(cat.bw)

	err = cat.appendAll([]catalogRecord{addRecord(testTable(2, 0))})
	require.ErrorIs(t, err, errInjectedCatalogWrite)

	st, err := os.Stat(filepath.Join(dir, catFilename))
	require.NoError(t, err)
	assert.Equal(t, int64(len(first)), st.Size())
}

var errInjectedCatalogWrite = errors.New("injected catalog write failure")

type failAfterWriter struct {
	w         *os.File
	remaining int
}

func (w *failAfterWriter) Write(p []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, errInjectedCatalogWrite
	}
	if len(p) > w.remaining {
		n, err := w.w.Write(p[:w.remaining])
		w.remaining -= n
		if err != nil {
			return n, err
		}
		return n, errInjectedCatalogWrite
	}
	n, err := w.w.Write(p)
	w.remaining -= n
	return n, err
}

func testTable(id uint64, level uint8) *sst.Metadata {
	return &sst.Metadata{
		Version:   1,
		ID:        id,
		Level:     level,
		Path:      filepath.Join("sst", strconv.FormatUint(id, 10)),
		CreatedAt: time.Unix(123, int64(id)).UTC(),
		MinKey:    []byte{byte(id), 4},
		MaxKey:    []byte{byte(id), 5},
	}
}

func TestSnapshotPreservesLiveStateAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	vs, err := New(dir)
	require.NoError(t, err)

	checkpoint := Checkpoint{LastSeq: 17, LastLSN: 42}
	require.NoError(t, vs.Apply([]Change{
		{Op: OpAdd, Table: testTable(50, 0)},
		{Op: OpAdd, Table: testTable(40, 0)},
		{Op: OpAdd, Table: testTable(60, 0)},
		{Op: OpAdd, Table: testTable(3, 1)},
		{Op: OpAdd, Table: testTable(9, 2)},
		{Op: OpCheckpoint, Checkpoint: checkpoint},
	}))
	before := catalogLines(t, dir)
	for id := uint64(1000); id < 1000+maxStaleRecords/2; id++ {
		require.NoError(t, vs.Apply([]Change{{Op: OpAdd, Table: testTable(id, 0)}}))
		require.NoError(t, vs.Apply([]Change{{Op: OpDelete, Table: &sst.Metadata{ID: id}}}))
	}
	assert.Greater(t, before+maxStaleRecords, catalogLines(t, dir), "catalog was rewritten")

	require.NoError(t, vs.Apply([]Change{{Op: OpAdd, Table: testTable(70, 1)}}))
	require.NoError(t, vs.Close())

	reopened, err := New(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })
	version := reopened.CurrentVersion()
	assertLevelIDs(t, version.Level(0), 50, 40, 60)
	assertLevelIDs(t, version.Level(1), 3, 70)
	assertLevelIDs(t, version.Level(2), 9)
	assert.Equal(t, checkpoint, version.Checkpoint())
	assert.Greater(t, reopened.NextTableID(), uint64(1000+maxStaleRecords/2-1))
}

func TestStaleSnapshotTmpIsDiscardedOnOpen(t *testing.T) {
	dir := t.TempDir()
	writeCatalog(t, dir, mustEncodeRecord(t, addRecord(testTable(1, 0))))
	require.NoError(t, os.WriteFile(filepath.Join(dir, catFilename+".tmp"), []byte("{torn"), 0o644))

	vs, err := New(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })
	assertLevelIDs(t, vs.CurrentVersion().Level(0), 1)
	assert.NoFileExists(t, filepath.Join(dir, catFilename+".tmp"))
}

func TestOversizedCatalogIsSnapshottedOnOpen(t *testing.T) {
	dir := t.TempDir()
	records := [][]byte{mustEncodeRecord(t, addRecord(testTable(1, 0)))}
	for id := uint64(2); id < 2+maxStaleRecords; id++ {
		records = append(records,
			mustEncodeRecord(t, addRecord(testTable(id, 1))),
			mustEncodeRecord(t, catalogRecord{Op: OpDelete, Table: &sst.Metadata{ID: id}}),
		)
	}
	writeCatalog(t, dir, records...)

	vs, err := New(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })
	assert.Equal(t, 2, catalogLines(t, dir))
	assertLevelIDs(t, vs.CurrentVersion().Level(0), 1)
	assert.Equal(t, uint64(2+maxStaleRecords), vs.NextTableID())
}

func catalogLines(t *testing.T, dir string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, catFilename))
	require.NoError(t, err)
	return bytes.Count(data, []byte("\n"))
}

func pointLookup(v *Version, key []byte) []*sst.Metadata {
	var tables []*sst.Metadata
	v.PointLookup(key, func(tab *sst.Metadata) bool {
		tables = append(tables, tab)
		return true
	})
	return tables
}

func assertLevelIDs(t *testing.T, tables []*sst.Metadata, ids ...uint64) {
	t.Helper()
	if ids == nil {
		ids = []uint64{}
	}
	got := make([]uint64, len(tables))
	for i, t := range tables {
		got[i] = t.ID
	}
	assert.Equal(t, ids, got)
}

func addRecord(table *sst.Metadata) catalogRecord {
	return catalogRecord{Op: OpAdd, Table: table}
}

func mustEncodeRecord(t *testing.T, rec catalogRecord) []byte {
	t.Helper()
	var buf bytes.Buffer
	nn, err := jsonl.NewWriter(&buf).Write(rec)
	require.Greater(t, nn, 0)
	require.NoError(t, err)
	return buf.Bytes()
}

func writeCatalog(t *testing.T, dir string, records ...[]byte) {
	t.Helper()
	file, err := os.Create(filepath.Join(dir, catFilename))
	require.NoError(t, err)
	defer file.Close()

	for _, rec := range records {
		_, err := file.Write(rec)
		require.NoError(t, err)
	}
}
