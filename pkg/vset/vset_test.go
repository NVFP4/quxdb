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

	require.NoError(t, vs.Apply([]Changes{{Op: OpAdd, Meta: testTableMeta(1, 0)}}))
	require.NoError(t, vs.Apply([]Changes{{Op: OpDelete, Meta: TableMeta{ID: 1, Level: 1}}}))
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

	first := testTableMeta(1, 0)
	second := testTableMeta(1, 1)
	require.NoError(t, vs.Apply([]Changes{{Op: OpAdd, Meta: first}}))
	require.NoError(t, vs.Apply([]Changes{{Op: OpAdd, Meta: second}}))
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

func TestApplyPublishesFreshVersion(t *testing.T) {
	vs, err := New(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })

	prev := vs.CurrentVersion()
	require.NoError(t, vs.Apply([]Changes{{Op: OpAdd, Meta: testTableMeta(1, 0)}}))
	next := vs.CurrentVersion()

	assert.NotSame(t, prev, next)
	assertLevelIDs(t, prev.Level(0))
	assertLevelIDs(t, next.Level(0), 1)

	prev.levels[0] = []TableMeta{testTableMeta(99, 0)}
	assertLevelIDs(t, vs.CurrentVersion().Level(0), 1)
}

func TestNewTruncatesTornCatalogTail(t *testing.T) {
	dir := t.TempDir()
	first := mustEncodeRecord(t, catalogRecord{Op: OpAdd, Meta: testTableMeta(1, 0)})
	torn := mustEncodeRecord(t, catalogRecord{Op: OpAdd, Meta: testTableMeta(2, 0)})
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
	first := mustEncodeRecord(t, catalogRecord{Op: OpAdd, Meta: testTableMeta(1, 0)})
	corrupt := []byte("{not-json}\n")
	third := mustEncodeRecord(t, catalogRecord{Op: OpAdd, Meta: testTableMeta(3, 0)})
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
	firstRec := catalogRecord{Op: OpAdd, Meta: testTableMeta(1, 0)}
	first := mustEncodeRecord(t, firstRec)
	writeCatalog(t, dir, first)

	vs, err := New(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = vs.Close() })

	secondRec := catalogRecord{Op: OpAdd, Meta: testTableMeta(2, 0)}
	secondRec.Meta.Path = filepath.Join("sst", string(bytes.Repeat([]byte("x"), 257)))
	second := mustEncodeRecord(t, secondRec)
	require.NoError(t, vs.Apply([]Changes{Changes(secondRec)}))

	st, err := os.Stat(filepath.Join(dir, catFilename))
	require.NoError(t, err)
	assert.Equal(t, int64(len(first)+len(second)), st.Size())
}

func TestAppendRollbackUsesFileSize(t *testing.T) {
	dir := t.TempDir()
	first := mustEncodeRecord(t, catalogRecord{Op: OpAdd, Meta: testTableMeta(1, 0)})
	writeCatalog(t, dir, first)

	cat, err := openCatalog(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cat.close() })
	require.NoError(t, cat.replay(func(catalogRecord) error { return nil }))

	fw := &failAfterWriter{w: cat.fd, remaining: 8}
	cat.bw = bufio.NewWriterSize(fw, catalogBufferSize)
	cat.jw = jsonl.NewWriter(cat.bw)

	err = cat.appendAll([]catalogRecord{{Op: OpAdd, Meta: testTableMeta(2, 0)}})
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

func testTableMeta(id uint64, level uint8) TableMeta {
	return TableMeta{
		Version:   1,
		ID:        id,
		Level:     level,
		Path:      filepath.Join("sst", strconv.FormatUint(id, 10)),
		CreatedAt: time.Unix(123, int64(id)).UTC(),
		FileHashes: sst.FileHashes{
			Data:   "1",
			Index:  "2",
			Filter: "3",
		},
		MinKey: []byte{byte(id), 4},
		MaxKey: []byte{byte(id), 5},
	}
}

func assertLevelIDs(t *testing.T, metas []TableMeta, ids ...uint64) {
	t.Helper()
	if ids == nil {
		ids = []uint64{}
	}
	got := make([]uint64, len(metas))
	for i, meta := range metas {
		got[i] = meta.ID
	}
	assert.Equal(t, ids, got)
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
