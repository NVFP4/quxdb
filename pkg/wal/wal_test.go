package wal

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALReadUsesSegmentLookupByID(t *testing.T) {
	dir := t.TempDir()

	seg, err := newSegment(dir, 3)
	require.NoError(t, err)
	data := []byte("record")
	lsn, err := seg.append(data, walRecordFlags)
	require.NoError(t, err)
	require.NoError(t, seg.close())

	w := openTestWAL(t, dir)

	data, next, err := w.Read(lsn)
	require.NoError(t, err)
	assert.Equal(t, []byte("record"), data)
	assert.Equal(t, newLSN(3, lsnOffset(lsn)+int64(encodedRecordLen(data))), next)
}

func TestWALRolloverAndReplayAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	lsn1, _, err := appendAcrossRollover(t, w, []byte("one"), []byte("two"))
	require.NoError(t, err)
	assertInactiveSegmentUnmapped(t, w, 0)
	require.NoError(t, w.Close())

	reopened := openTestWAL(t, dir)
	assertInactiveSegmentUnmapped(t, reopened, 0)
	var got []string
	_, err = reopened.Replay(func(r Record) error {
		got = append(got, string(r.Data))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"one", "two"}, got)
	assertInactiveSegmentUnmapped(t, reopened, 0)

	data, _, err := reopened.Read(lsn1)
	require.NoError(t, err)
	assert.Equal(t, []byte("one"), data)
	assertInactiveSegmentMapped(t, reopened, 0)
}

func TestWALTruncateFromOlderSegmentDeletesTail(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	first := []byte("one")
	second := []byte("two")
	lsn1, lsn2, err := appendAcrossRollover(t, w, first, second)
	require.NoError(t, err)
	require.Equal(t, segID(1), lsnSegID(lsn2))

	cutLSN := newLSN(lsnSegID(lsn1), lsnOffset(lsn1)+int64(encodedRecordLen(first)))
	require.NoError(t, w.TruncateFrom(cutLSN))

	_, _, err = w.Read(lsn2)
	require.Error(t, err)
	_, err = os.Stat(filepath.Join(dir, segmentName(1)))
	require.ErrorIs(t, err, os.ErrNotExist)

	lsn3, err := w.Append([]byte("three"))
	require.NoError(t, err)
	assert.Equal(t, cutLSN, lsn3)

	data, _, err := w.Read(lsn3)
	require.NoError(t, err)
	assert.Equal(t, []byte("three"), data)
	assert.Nil(t, w.segments.active.mmap)
	assert.NotNil(t, w.segments.active.file)
}

func TestWALRejectsOversizedBatch(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	_, err := w.AppendBatch([][]byte{make([]byte, walSegmentMaxSize)})
	require.ErrorIs(t, err, ErrRecordTooLarge)
	assert.Len(t, w.segments.segments, 1)
}

func TestWALDetectsStoredLSNMismatch(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	seg := w.segments.active
	off := seg.cursor
	lsn := newLSN(seg.sid, off)
	data := []byte("bad-lsn")
	buf := make([]byte, encodedRecordLen(data))

	n := encodeRecord(buf, newLSN(seg.sid, off+8), walRecordFlags, data)
	written, err := seg.file.WriteAt(buf, off)
	require.NoError(t, err)
	require.Equal(t, n, written)
	seg.cursor += int64(n)

	_, _, err = w.Read(lsn)
	require.ErrorIs(t, err, ErrRecordInvalidLSN)
}

func TestWALReplayReportsCorruptRecordForTruncate(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	_, err := w.Append([]byte("good"))
	require.NoError(t, err)
	badLSN, err := w.Append([]byte("bad"))
	require.NoError(t, err)

	_, err = w.segments.active.file.WriteAt([]byte{0xff}, lsnOffset(badLSN)+walRecordHeaderLen)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	reopened := openTestWAL(t, dir)
	var got []string
	_, err = reopened.Replay(func(r Record) error {
		got = append(got, string(r.Data))
		return nil
	})
	require.ErrorIs(t, err, ErrRecordCorrupt)
	assert.Equal(t, []string{"good"}, got)

	require.NoError(t, reopened.TruncateFrom(badLSN))

	_, _, err = reopened.Read(badLSN)
	require.ErrorIs(t, err, io.EOF)
}

func TestReadRecordRejectsInvalidDataLengthBeforeAllocation(t *testing.T) {
	buf := make([]byte, alignUp8(walRecordHeaderLen))
	binary.BigEndian.PutUint32(buf[0:], walRecordMagic32)
	binary.LittleEndian.PutUint32(buf[12:], uint32(len(buf)))
	binary.LittleEndian.PutUint32(buf[28:], 1<<30)
	binary.LittleEndian.PutUint32(buf[8:], crc32.Checksum(buf[12:walRecordHeaderLen], walCRC32CTable))

	_, err := readRecord(bytes.NewReader(buf), 0, int64(len(buf)))
	require.ErrorIs(t, err, ErrRecordInvalidSize)
}

func TestReadRecordHeaderRejectsHeaderCRC(t *testing.T) {
	data := []byte("record")
	buf := make([]byte, encodedRecordLen(data))
	lsn := newLSN(0, walHeaderLen)
	encodeRecord(buf, lsn, walRecordFlags, data)
	buf[16]++ // corrupt rtype while leaving hcrc unchanged

	_, err := readRecordHeader(bytes.NewReader(buf), 0, int64(len(buf)))
	require.ErrorIs(t, err, ErrRecordCorrupt)
}

func openTestWAL(t *testing.T, dir string) *WAL {
	t.Helper()
	w, err := New(dir)
	require.NoError(t, err)
	require.NoError(t, w.Open())
	t.Cleanup(func() {
		_ = w.Close()
	})
	return w
}

func appendAndForceRollover(t *testing.T, w *WAL, first, second []byte) error {
	t.Helper()
	_, _, err := appendAcrossRollover(t, w, first, second)
	return err
}

func appendAcrossRollover(t *testing.T, w *WAL, first, second []byte) (LSN, LSN, error) {
	t.Helper()
	lsn1, err := w.Append(first)
	if err != nil {
		return 0, 0, err
	}
	w.segments.active.cursor = w.segments.active.maxSize - int64(encodedRecordLen(second))
	lsn2, err := w.Append(second)
	return lsn1, lsn2, err
}

func assertInactiveSegmentMapped(t *testing.T, w *WAL, sid segID) {
	t.Helper()
	seg := w.segments.byID[sid]
	require.NotNil(t, seg)
	require.NotSame(t, w.segments.active, seg)
	assert.Nil(t, seg.file)
	assert.NotNil(t, seg.mmap)
}

func assertInactiveSegmentUnmapped(t *testing.T, w *WAL, sid segID) {
	t.Helper()
	seg := w.segments.byID[sid]
	require.NotNil(t, seg)
	require.NotSame(t, w.segments.active, seg)
	assert.Nil(t, seg.file)
	assert.Nil(t, seg.mmap)
}
