package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type corruptRecordLengthCase struct {
	name    string
	recLen  uint32
	dataLen uint32
	want    error
}

const walTestSegmentSize = walSegmentMinSize

func testLSN(sid segID, offset uint64) LSN {
	return newLSN(sid, offset, walTestSegmentSize)
}

func testLSNOffset(lsn LSN) uint64 {
	return lsnOffset(lsn, walTestSegmentSize)
}

func testLSNSegID(lsn LSN) segID {
	return lsnSegID(lsn, walTestSegmentSize)
}

func corruptRecordLengthCases() []corruptRecordLengthCase {
	return []corruptRecordLengthCase{
		{
			name:    "too small",
			recLen:  walRecordHeaderLen + walRecordMetaLen - 8,
			dataLen: 0,
			want:    ErrRecordTorn,
		},
		{
			name:    "not aligned",
			recLen:  walRecordHeaderLen + walRecordMetaLen + 1,
			dataLen: 0,
			want:    ErrRecordTorn,
		},
		{
			name:    "data length exceeds record length",
			recLen:  uint32(encodedRecordSize(1)),
			dataLen: 9,
			want:    ErrRecordInvalidSize,
		},
		{
			name:    "data length exceeds maximum",
			recLen:  uint32(encodedRecordSize(walMaxDataSize)),
			dataLen: walMaxDataSize + 1,
			want:    ErrRecordInvalidSize,
		},
	}
}

func TestIsCorruption(t *testing.T) {
	for _, err := range []error{
		ErrRecordInvalidFormat,
		ErrRecordInvalidSize,
		ErrRecordInvalidLSN,
		ErrRecordTorn,
		ErrRecordChecksumMismatch,
	} {
		assert.True(t, IsCorruption(fmt.Errorf("wrapped: %w", err)))
	}
	assert.False(t, IsCorruption(io.ErrUnexpectedEOF))
	assert.False(t, IsCorruption(errors.New("callback failed")))
}

func TestWALReadBySegmentID(t *testing.T) {
	dir := t.TempDir()

	seg1, err := newSegment(dir, 1, walTestSegmentSize)
	require.NoError(t, err)
	data1 := []byte("record-1")
	lsn1, err := seg1.append(data1, walRecordFlags)
	require.NoError(t, err)
	require.NoError(t, seg1.close())

	seg3, err := newSegment(dir, 3, walTestSegmentSize)
	require.NoError(t, err)
	data3 := []byte("record-3")
	lsn3, err := seg3.append(data3, walRecordFlags)
	require.NoError(t, err)
	require.NoError(t, seg3.close())

	w := openTestWAL(t, dir)
	require.Equal(t, segID(3), w.segments.active.segId)
	assertInactiveSegmentReadyForMmap(t, w, 1)
	assertActiveSegmentReadWrite(t, w)

	data, next, err := w.Read(lsn1)
	require.NoError(t, err)
	assert.Equal(t, data1, data)
	assert.Equal(t, testLSN(1, testLSNOffset(lsn1)+uint64(encodedRecordLen(data1))), next)
	assertInactiveSegmentMapped(t, w, 1)

	data, next, err = w.Read(lsn3)
	require.NoError(t, err)
	assert.Equal(t, data3, data)
	assert.Equal(t, testLSN(3, testLSNOffset(lsn3)+uint64(encodedRecordLen(data3))), next)
	assertActiveSegmentReadWrite(t, w)
}

func TestSegmentNameOrdersByTimestampThenID(t *testing.T) {
	createdAt := time.Unix(1_700_000_000, 123_456_789).UTC()

	assert.Less(t, segmentName(createdAt, 0), segmentName(createdAt, ^segID(0)))
	assert.Less(t, segmentName(createdAt, ^segID(0)), segmentName(createdAt.Add(time.Millisecond), 0))
}

func TestWALReplayAcrossRollover(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	first := bytes.Repeat([]byte{'a'}, 4000)
	second := []byte("two")
	lsn1, lsn2, err := appendAcrossRollover(t, w, first, second)
	require.NoError(t, err)
	assertInactiveSegmentUnmapped(t, w, 0)
	assertSegmentCursorMatchesFileSize(t, w.segments.byID[0])
	assertSegmentCursorMatchesFileSize(t, w.segments.active)
	require.NoError(t, w.Close())

	reopened := openTestWAL(t, dir)
	assertInactiveSegmentReadyForMmap(t, reopened, 0)
	assertActiveSegmentReadWrite(t, reopened)
	for _, seg := range reopened.segments.segments {
		assertSegmentCursorMatchesFileSize(t, seg)
	}
	type replayedRecord struct {
		lsn  LSN
		data []byte
	}
	var got []replayedRecord
	end, err := reopened.Replay(func(r Record) error {
		got = append(got, replayedRecord{lsn: r.LSN, data: append([]byte(nil), r.Data...)})
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []replayedRecord{{lsn: lsn1, data: first}, {lsn: lsn2, data: second}}, got)
	assert.Equal(t, testLSN(testLSNSegID(lsn2), testLSNOffset(lsn2)+uint64(encodedRecordLen(second))), end)
	assertInactiveSegmentUnmapped(t, reopened, 0)

	data, _, err := reopened.Read(lsn1)
	require.NoError(t, err)
	assert.Equal(t, first, data)
	assertInactiveSegmentMapped(t, reopened, 0)
}

func TestWALReplayAfterLSN(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	_, err := w.Append([]byte("one"))
	require.NoError(t, err)
	secondLSN, err := w.Append([]byte("two"))
	require.NoError(t, err)
	thirdLSN, err := w.Append([]byte("three"))
	require.NoError(t, err)

	var got []string
	end, err := w.ReplayAfter(secondLSN, func(r Record) error {
		got = append(got, string(r.Data))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"three"}, got)
	assert.Equal(t, testLSN(testLSNSegID(thirdLSN), testLSNOffset(thirdLSN)+uint64(encodedRecordLen([]byte("three")))), end)
}

func TestWALReplayAfterLastRecord(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	lsn, err := w.Append([]byte("one"))
	require.NoError(t, err)

	called := false
	end, err := w.ReplayAfter(lsn, func(Record) error {
		called = true
		return nil
	})
	require.NoError(t, err)
	assert.False(t, called)
	assert.Equal(t, testLSN(testLSNSegID(lsn), testLSNOffset(lsn)+uint64(encodedRecordLen([]byte("one")))), end)
}

func TestWALReplayAfterLSNAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	first := bytes.Repeat([]byte{'a'}, 4000)
	firstLSN, secondLSN, err := appendAcrossRollover(t, w, first, []byte("two"))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	reopened := openTestWAL(t, dir)
	assertInactiveSegmentReadyForMmap(t, reopened, 0)

	var got []string
	end, err := reopened.ReplayAfter(firstLSN, func(r Record) error {
		got = append(got, string(r.Data))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"two"}, got)
	assert.Equal(t, testLSN(testLSNSegID(secondLSN), testLSNOffset(secondLSN)+uint64(encodedRecordLen([]byte("two")))), end)
	assertInactiveSegmentUnmapped(t, reopened, 0)
}

func TestWALReplayAfterRejectsInvalidLSN(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	_, err := w.Append([]byte("one"))
	require.NoError(t, err)
	invalid := testLSN(w.segments.active.segId, w.segments.active.cursor+1)

	_, err = w.ReplayAfter(invalid, func(Record) error { return nil })
	require.ErrorIs(t, err, io.EOF)
}

func TestWALNewSegmentKeepsLogicalEOFAtCursor(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	assert.Equal(t, uint64(walHeaderLenPadded), w.segments.active.cursor)
	assert.Equal(t, uint64(walHeaderLenPadded), testLSNOffset(w.segments.active.startLSN))
	assertSegmentCursorMatchesFileSize(t, w.segments.active)
	assertActiveSegmentReadWrite(t, w)

	_, err := w.Append([]byte("record"))
	require.NoError(t, err)
	assertSegmentCursorMatchesFileSize(t, w.segments.active)
}

func TestWALTruncateDeletesLaterSegments(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	first := bytes.Repeat([]byte{'a'}, 3900)
	second := bytes.Repeat([]byte{'b'}, 100)
	lsn1, lsn2, err := appendAcrossRollover(t, w, first, second)
	require.NoError(t, err)
	require.Equal(t, segID(1), testLSNSegID(lsn2))

	old := w.segments.byID[testLSNSegID(lsn1)]
	removed := w.segments.byID[testLSNSegID(lsn2)]
	require.NotNil(t, removed)
	removedPath := removed.path
	oldCursor := old.cursor
	cutLSN := lsn1
	require.Less(t, testLSNOffset(cutLSN), oldCursor)
	assertInactiveSegmentUnmapped(t, w, testLSNSegID(lsn1))
	assertSegmentCursorMatchesFileSize(t, old)
	require.NoError(t, w.TruncateFrom(cutLSN))
	require.Same(t, old, w.segments.active)
	assert.Equal(t, testLSNOffset(cutLSN), w.segments.active.cursor)
	assert.Equal(t, uint64(walHeaderLenPadded), testLSNOffset(w.segments.active.startLSN))
	assertSegmentCursorMatchesFileSize(t, w.segments.active)
	assertActiveSegmentReadWrite(t, w)

	_, _, err = w.Read(lsn2)
	require.ErrorContains(t, err, "unknown segment id=1")
	_, err = os.Stat(removedPath)
	require.ErrorIs(t, err, os.ErrNotExist)

	lsn3, err := w.Append([]byte("three"))
	require.NoError(t, err)
	assert.Equal(t, cutLSN, lsn3)
	assertSegmentCursorMatchesFileSize(t, w.segments.active)

	data, _, err := w.Read(lsn3)
	require.NoError(t, err)
	assert.Equal(t, []byte("three"), data)
	assert.Nil(t, w.segments.active.mmap)
	assert.NotNil(t, w.segments.active.file)
}

func TestWALPruneBefore(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	firstLSN, secondLSN, err := appendAcrossRollover(
		t,
		w,
		bytes.Repeat([]byte{'a'}, 4000),
		bytes.Repeat([]byte{'b'}, 4000),
	)
	require.NoError(t, err)
	thirdLSN, err := w.Append([]byte("three"))
	require.NoError(t, err)
	require.Equal(t, segID(0), testLSNSegID(firstLSN))
	require.Equal(t, segID(1), testLSNSegID(secondLSN))
	require.Equal(t, segID(2), testLSNSegID(thirdLSN))

	firstPath := w.segments.byID[0].path
	secondPath := w.segments.byID[1].path
	require.NoError(t, w.PruneBefore(secondLSN))

	assert.Nil(t, w.segments.byID[0])
	assert.NotNil(t, w.segments.byID[1])
	assert.Equal(t, []segID{1, 2}, []segID{w.segments.segments[0].segId, w.segments.segments[1].segId})
	_, err = os.Stat(firstPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(secondPath)
	require.NoError(t, err)

	data, _, err := w.Read(secondLSN)
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{'b'}, 4000), data)

	require.NoError(t, w.PruneBefore(thirdLSN))
	assert.Nil(t, w.segments.byID[1])
	assert.Same(t, w.segments.active, w.segments.segments[0])
	_, err = os.Stat(secondPath)
	require.ErrorIs(t, err, os.ErrNotExist)

	data, _, err = w.Read(thirdLSN)
	require.NoError(t, err)
	assert.Equal(t, []byte("three"), data)

	require.NoError(t, w.Close())
	reopened := openTestWAL(t, dir)
	require.Len(t, reopened.segments.segments, 1)
	assert.Equal(t, segID(2), reopened.segments.active.segId)
	data, _, err = reopened.Read(thirdLSN)
	require.NoError(t, err)
	assert.Equal(t, []byte("three"), data)
}

func TestWALPruneBeforeRejectsInvalidLSN(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	_, activeLSN, err := appendAcrossRollover(
		t,
		w,
		bytes.Repeat([]byte{'a'}, 4000),
		[]byte("active"),
	)
	require.NoError(t, err)
	sealed := w.segments.byID[0]

	require.NoError(t, w.PruneBefore(0))
	assert.Same(t, sealed, w.segments.byID[0])

	invalid := testLSN(testLSNSegID(activeLSN), walHeaderLenPadded-1)
	require.ErrorIs(t, w.PruneBefore(invalid), ErrSegmentLSNInvalid)
	assert.Same(t, sealed, w.segments.byID[0])
	_, err = os.Stat(sealed.path)
	require.NoError(t, err)

	require.ErrorContains(t, w.PruneBefore(testLSN(99, walHeaderLenPadded)), "unknown segment id=99")
	assert.Same(t, sealed, w.segments.byID[0])
}

func TestWALAppendBatchRejectsOversizedRecord(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	startCursor := w.segments.active.cursor

	results, err := w.AppendBatch([][]byte{make([]byte, walMaxDataSize+1)})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.ErrorIs(t, results[0].Err, ErrRecordInvalidSize)
	assert.Zero(t, results[0].LSN)
	assert.Len(t, w.segments.segments, 1)
	assert.Equal(t, startCursor, w.segments.active.cursor)
}

func TestWALAppendBatchSkipsInvalidRecords(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	startCursor := w.segments.active.cursor

	one := []byte("one")
	oversized := make([]byte, walMaxDataSize+1)
	three := []byte("three")
	results, err := w.AppendBatch([][]byte{one, oversized, three})
	require.NoError(t, err)
	require.Len(t, results, 3)
	require.NoError(t, results[0].Err)
	require.ErrorIs(t, results[1].Err, ErrRecordInvalidSize)
	require.NoError(t, results[2].Err)

	wantFirst := testLSN(0, startCursor)
	wantSecond := testLSN(0, startCursor+uint64(encodedRecordLen(one)))
	wantEnd := testLSN(0, testLSNOffset(wantSecond)+uint64(encodedRecordLen(three)))
	assert.Equal(t, wantFirst, results[0].LSN)
	assert.Zero(t, results[1].LSN)
	assert.Equal(t, wantSecond, results[2].LSN)
	assert.Equal(t, testLSNOffset(wantEnd), w.segments.active.cursor)

	data, next, err := w.Read(results[0].LSN)
	require.NoError(t, err)
	assert.Equal(t, one, data)
	assert.Equal(t, wantSecond, next)

	data, next, err = w.Read(results[2].LSN)
	require.NoError(t, err)
	assert.Equal(t, three, data)
	assert.Equal(t, wantEnd, next)

	_, _, err = w.Read(wantEnd)
	require.ErrorIs(t, err, io.EOF)
	assert.Equal(t, []string{"one", "three"}, replayStrings(t, w))
}

func TestWALAppendBatchRolloverIsAtomic(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	filler := bytes.Repeat([]byte{'f'}, 4000)
	fillerLSN, err := w.Append(filler)
	require.NoError(t, err)
	old := w.segments.active
	oldCursor := old.cursor
	batch := [][]byte{[]byte("one"), []byte("two")}
	require.Equal(t, errSegmentInsufficientSpace, old.checkRoom(encodedRecordLen(batch[0])+encodedRecordLen(batch[1])))

	results, err := w.AppendBatch(batch)
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.NoError(t, results[0].Err)
	require.NoError(t, results[1].Err)
	assert.Equal(t, segID(1), testLSNSegID(results[0].LSN))
	assert.Equal(t, uint64(walHeaderLenPadded), testLSNOffset(results[0].LSN))
	assert.Equal(t, testLSN(1, uint64(walHeaderLenPadded+encodedRecordLen(batch[0]))), results[1].LSN)
	assert.Equal(t, oldCursor, old.cursor)
	assertInactiveSegmentUnmapped(t, w, 0)
	assertSegmentCursorMatchesFileSize(t, old)
	assertSegmentCursorMatchesFileSize(t, w.segments.active)

	require.NoError(t, w.Close())
	reopened := openTestWAL(t, dir)
	data, _, err := reopened.Read(fillerLSN)
	require.NoError(t, err)
	assert.Equal(t, filler, data)
	data, _, err = reopened.Read(results[0].LSN)
	require.NoError(t, err)
	assert.Equal(t, []byte("one"), data)
	data, _, err = reopened.Read(results[1].LSN)
	require.NoError(t, err)
	assert.Equal(t, []byte("two"), data)

	_, _, err = reopened.Read(testLSN(0, oldCursor))
	require.ErrorIs(t, err, io.EOF)
}

func TestWALAppendRollsOverWhenFull(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	old := w.segments.active
	fill := bytes.Repeat([]byte{'f'}, 4000)
	fillLSN, err := w.Append(fill)
	require.NoError(t, err)
	oldCursor := old.cursor

	next := []byte("next")
	lsn, err := w.Append(next)
	require.NoError(t, err)
	assert.Equal(t, uint64(walHeaderLenPadded), testLSNOffset(fillLSN))
	assert.Equal(t, oldCursor, old.cursor)
	assert.Equal(t, segID(1), testLSNSegID(lsn))
	assert.Equal(t, uint64(walHeaderLenPadded), testLSNOffset(lsn))
	assertInactiveSegmentUnmapped(t, w, 0)
	assertSegmentCursorMatchesFileSize(t, old)
	assertSegmentCursorMatchesFileSize(t, w.segments.active)

	require.NoError(t, w.Close())
	reopened := openTestWAL(t, dir)
	data, _, err := reopened.Read(fillLSN)
	require.NoError(t, err)
	assert.Equal(t, fill, data)
	data, _, err = reopened.Read(lsn)
	require.NoError(t, err)
	assert.Equal(t, next, data)
}

func TestWALOpenUsesPhysicalEOFUntilRecoveryTruncatesTail(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	good := []byte("good")
	goodLSN, err := w.Append(good)
	require.NoError(t, err)
	goodNext := testLSN(testLSNSegID(goodLSN), testLSNOffset(goodLSN)+uint64(encodedRecordLen(good)))
	assertSegmentCursorMatchesFileSize(t, w.segments.active)

	badHeader := make([]byte, walRecordHeaderLen)
	copy(badHeader, []byte("NOPE"))
	_, err = w.segments.active.file.WriteAt(badHeader, int64(testLSNOffset(goodNext)))
	require.NoError(t, err)
	info, err := os.Stat(w.segments.active.path)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	reopened := openTestWAL(t, dir)
	assert.Equal(t, uint64(info.Size()), reopened.segments.active.cursor)
	assert.Equal(t, testLSNOffset(goodNext)+uint64(len(badHeader)), reopened.segments.active.cursor)

	end, err := reopened.Replay(func(Record) error { return nil })
	require.ErrorIs(t, err, ErrRecordInvalidFormat)
	assert.Equal(t, goodNext, end)

	require.NoError(t, reopened.TruncateFrom(end))
	assert.Equal(t, testLSNOffset(end), reopened.segments.active.cursor)
	assertSegmentCursorMatchesFileSize(t, reopened.segments.active)
	assertActiveSegmentReadWrite(t, reopened)

	afterLSN, err := reopened.Append([]byte("after"))
	require.NoError(t, err)
	assert.Equal(t, goodNext, afterLSN)
	assertSegmentCursorMatchesFileSize(t, reopened.segments.active)
	assert.Equal(t, []string{"good", "after"}, replayStrings(t, reopened))
}

func TestWALReplayStopsOnCallbackError(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	_, err := w.Append([]byte("one"))
	require.NoError(t, err)
	lsn2, err := w.Append([]byte("two"))
	require.NoError(t, err)

	stop := errors.New("stop replay")
	var got []string
	last, err := w.Replay(func(r Record) error {
		got = append(got, string(r.Data))
		if string(r.Data) == "two" {
			return stop
		}
		return nil
	})
	require.ErrorIs(t, err, stop)
	assert.Equal(t, []string{"one", "two"}, got)
	assert.Equal(t, lsn2, last)
}

func TestWALOpenRejectsBadSegmentHeaderCRC(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, walTestSegmentSize)
	require.NoError(t, err)
	segPath := seg.path
	require.NoError(t, seg.close())

	file, err := os.OpenFile(segPath, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte{0xff}, 8) // segId is covered by the header CRC.
	require.NoError(t, err)
	require.NoError(t, file.Close())

	w, err := New(dir, WithSegmentSize(walTestSegmentSize))
	require.NoError(t, err)
	require.ErrorIs(t, w.Open(), ErrHeaderChecksumMismatch)
}

func TestSegmentSetRolloverWrapsSegmentID(t *testing.T) {
	dir := t.TempDir()
	segments := newSegmentSet(dir, walTestSegmentSize)
	old, err := newSegment(dir, ^segID(0), walTestSegmentSize)
	require.NoError(t, err)
	segments.segments = append(segments.segments, old)
	segments.byID[old.segId] = old
	segments.active = old

	require.NoError(t, segments.rollover())
	assert.Equal(t, segID(0), segments.active.segId)
	assert.Equal(t, segmentModeReadOnly, old.mode)
	assert.Nil(t, old.file)
	assert.Equal(t, segmentModeReadWrite, segments.active.mode)
	assert.NotNil(t, segments.active.file)
	assert.Len(t, segments.segments, 2)
	require.NoError(t, segments.close())
}

func TestWALTruncateRejectsOutOfBounds(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	lsn, err := w.Append([]byte("one"))
	require.NoError(t, err)
	cursor := w.segments.active.cursor

	require.ErrorIs(t, w.TruncateFrom(testLSN(testLSNSegID(lsn), uint64(walHeaderLenPadded-1))), ErrTruncateOutOfBounds)
	assert.Equal(t, cursor, w.segments.active.cursor)

	require.ErrorIs(t, w.TruncateFrom(testLSN(testLSNSegID(lsn), cursor+1)), ErrTruncateOutOfBounds)
	assert.Equal(t, cursor, w.segments.active.cursor)

	data, _, err := w.Read(lsn)
	require.NoError(t, err)
	assert.Equal(t, []byte("one"), data)
}

func TestWALTruncateRejectsUnknownSegment(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	lsn, err := w.Append([]byte("one"))
	require.NoError(t, err)
	active := w.segments.active
	cursor := active.cursor
	segments := append([]*walSegment(nil), w.segments.segments...)

	require.ErrorContains(t, w.TruncateFrom(testLSN(99, walHeaderLenPadded)), "unknown segment id=99")
	assert.Same(t, active, w.segments.active)
	assert.Equal(t, cursor, w.segments.active.cursor)
	assert.Equal(t, segments, w.segments.segments)

	data, _, err := w.Read(lsn)
	require.NoError(t, err)
	assert.Equal(t, []byte("one"), data)
}

func TestWALEmptyRecordRoundTrip(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	lsn, err := w.Append(nil)
	require.NoError(t, err)

	data, next, err := w.Read(lsn)
	require.NoError(t, err)
	assert.Empty(t, data)
	assert.Equal(t, testLSN(testLSNSegID(lsn), testLSNOffset(lsn)+uint64(encodedRecordLen(nil))), next)

	var got [][]byte
	end, err := w.Replay(func(r Record) error {
		got = append(got, append([]byte(nil), r.Data...))
		return nil
	})
	require.NoError(t, err)
	require.Len(t, got, 1)
	assert.Empty(t, got[0])
	assert.Equal(t, next, end)
}

func TestWALLargeRecordRoundTrip(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	dataLen := int(w.segments.active.segMaxSize-w.segments.active.cursor) - walRecordHeaderLen - walRecordMetaLen - 8
	data := bytes.Repeat([]byte{0x7b}, dataLen)
	lsn, err := w.Append(data)
	require.NoError(t, err)

	got, next, err := w.Read(lsn)
	require.NoError(t, err)
	assert.Equal(t, data, got)
	assert.Equal(t, testLSN(testLSNSegID(lsn), testLSNOffset(lsn)+uint64(encodedRecordLen(data))), next)
}

func TestWALReadRejectsInvalidLSN(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	lsn, err := w.Append([]byte("one"))
	require.NoError(t, err)

	_, _, err = w.Read(testLSN(testLSNSegID(lsn), 0))
	require.ErrorIs(t, err, ErrSegmentLSNInvalid)

	_, _, err = w.Read(testLSN(testLSNSegID(lsn), w.segments.active.cursor))
	require.ErrorIs(t, err, io.EOF)

	_, _, err = w.Read(testLSN(42, walHeaderLenPadded))
	require.ErrorContains(t, err, "unknown segment id=42")
}

func TestWALAppendBatchRejectsTooLargeBatch(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	active := w.segments.active
	cursor := active.cursor

	maxRecord := make([]byte, walMaxDataSize)
	batch := make([][]byte, 16)
	for i := range batch {
		batch[i] = maxRecord
	}

	results, err := w.AppendBatch(batch)
	require.ErrorIs(t, err, ErrRecordTooLarge)
	assert.Nil(t, results)
	assert.Same(t, active, w.segments.active)
	assert.Equal(t, cursor, active.cursor)
	assert.Len(t, w.segments.segments, 1)
}

func TestWALReplayReportsTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	good := []byte("good")
	goodLSN, err := w.Append(good)
	require.NoError(t, err)
	goodNext := testLSN(testLSNSegID(goodLSN), testLSNOffset(goodLSN)+uint64(encodedRecordLen(good)))

	tailLSN, err := w.Append([]byte("tail"))
	require.NoError(t, err)
	require.Equal(t, goodNext, tailLSN)
	segPath := w.segments.active.path
	require.NoError(t, w.Close())

	// Leave a valid record header plus a byte of data, but remove the rest of the
	// record body/CRC. Replay should surface the torn tail at its LSN so recovery
	// can truncate precisely there.
	require.NoError(t, os.Truncate(segPath, int64(testLSNOffset(tailLSN)+walRecordHeaderLen+1)))

	reopened := openTestWAL(t, dir)
	var got []string
	end, err := reopened.Replay(func(r Record) error {
		got = append(got, string(r.Data))
		return nil
	})
	require.ErrorIs(t, err, ErrRecordTorn)
	assert.Equal(t, []string{"good"}, got)
	assert.Equal(t, tailLSN, end)

	_, _, err = reopened.Read(tailLSN)
	require.ErrorIs(t, err, ErrRecordTorn)

	require.NoError(t, reopened.TruncateFrom(tailLSN))
	afterLSN, err := reopened.Append([]byte("after"))
	require.NoError(t, err)
	assert.Equal(t, tailLSN, afterLSN)
	assert.Equal(t, []string{"good", "after"}, replayStrings(t, reopened))
}

func TestWALReplayReportsPreallocatedTornTail(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	_, err := w.Append([]byte("good"))
	require.NoError(t, err)
	tail := []byte("tail")
	tailLSN, err := w.Append(tail)
	require.NoError(t, err)
	zeroRecordBodyAndCRC(t, w, tailLSN, tail)
	require.NoError(t, w.Close())

	reopened := openTestWAL(t, dir)
	var got []string
	end, err := reopened.Replay(func(r Record) error {
		got = append(got, string(r.Data))
		return nil
	})
	require.ErrorIs(t, err, ErrRecordChecksumMismatch)
	assert.Equal(t, []string{"good"}, got)
	assert.Equal(t, tailLSN, end)

	require.NoError(t, reopened.TruncateFrom(tailLSN))
	afterLSN, err := reopened.Append([]byte("after"))
	require.NoError(t, err)
	assert.Equal(t, tailLSN, afterLSN)
	assert.Equal(t, []string{"good", "after"}, replayStrings(t, reopened))
}

func TestWALReplayStopsAtMiddleChecksumError(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	_, err := w.Append([]byte("good"))
	require.NoError(t, err)
	bad := []byte("bad")
	badLSN, err := w.Append(bad)
	require.NoError(t, err)
	thirdLSN, err := w.Append([]byte("third"))
	require.NoError(t, err)
	zeroRecordBodyAndCRC(t, w, badLSN, bad)
	require.NoError(t, w.Close())

	reopened := openTestWAL(t, dir)
	var got []string
	end, err := reopened.Replay(func(r Record) error {
		got = append(got, string(r.Data))
		return nil
	})
	require.ErrorIs(t, err, ErrRecordChecksumMismatch)
	assert.Equal(t, []string{"good"}, got)
	assert.Equal(t, badLSN, end)

	require.NoError(t, reopened.TruncateFrom(badLSN))
	_, _, err = reopened.Read(thirdLSN)
	require.ErrorIs(t, err, io.EOF)
	afterLSN, err := reopened.Append([]byte("after"))
	require.NoError(t, err)
	assert.Equal(t, badLSN, afterLSN)
	assert.Equal(t, []string{"good", "after"}, replayStrings(t, reopened))
}

func TestWALRecordPaddingZeroed(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	data := []byte("abc")
	lsn, err := w.Append(data)
	require.NoError(t, err)

	raw := make([]byte, encodedRecordLen(data))
	n, err := w.segments.active.file.ReadAt(raw, int64(testLSNOffset(lsn)))
	require.NoError(t, err)
	require.Equal(t, len(raw), n)

	pad := raw[walRecordHeaderLen+len(data)+walRecordMetaLen:]
	require.NotEmpty(t, pad)
	assert.Equal(t, make([]byte, len(pad)), pad)
}

func TestWALAppendCopiesCallerBuffer(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	data := []byte("owned")
	lsn, err := w.Append(data)
	require.NoError(t, err)
	copy(data, "mutated")

	got, _, err := w.Read(lsn)
	require.NoError(t, err)
	assert.Equal(t, []byte("owned"), got)
}

func TestWALSync(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	lsn, err := w.Append([]byte("synced"))
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	reopened := openTestWAL(t, dir)
	data, _, err := reopened.Read(lsn)
	require.NoError(t, err)
	assert.Equal(t, []byte("synced"), data)
}

func TestWALConcurrentAccess(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	type appendedRecord struct {
		lsn  LSN
		data []byte
	}
	const n = 200
	records := make(chan appendedRecord, n)
	errs := make(chan error, n)

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		defer close(records)
		for i := range n {
			data := []byte{byte(i), byte(i >> 8)}
			lsn, err := w.Append(data)
			if err != nil {
				errs <- err
				return
			}
			records <- appendedRecord{lsn: lsn, data: data}
		}
	}()

	go func() {
		defer wg.Done()
		for rec := range records {
			got, _, err := w.Read(rec.lsn)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, rec.data) {
				errs <- fmt.Errorf("read lsn=%d got=%x want=%x", rec.lsn, got, rec.data)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for range 50 {
			_, err := w.Replay(func(Record) error { return nil })
			if err != nil {
				errs <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func TestWALReplayRejectsBadRecordMagic(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	_, err := w.Append([]byte("good"))
	require.NoError(t, err)
	badLSN, err := w.Append([]byte("bad-magic"))
	require.NoError(t, err)

	_, err = w.segments.active.file.WriteAt([]byte("NOPE"), int64(testLSNOffset(badLSN)))
	require.NoError(t, err)

	_, _, err = w.Read(badLSN)
	require.ErrorIs(t, err, ErrRecordInvalidFormat)

	var got []string
	_, err = w.Replay(func(r Record) error {
		got = append(got, string(r.Data))
		return nil
	})
	require.ErrorIs(t, err, ErrRecordInvalidFormat)
	assert.Equal(t, []string{"good"}, got)
}

func TestWALReadRejectsBadRecordLengths(t *testing.T) {
	for _, tt := range corruptRecordLengthCases() {
		t.Run(tt.name, func(t *testing.T) {
			w := openTestWAL(t, t.TempDir())
			lsn, err := w.Append([]byte("payload"))
			require.NoError(t, err)

			var header [walRecordHeaderLen]byte
			h := walRecordHeader{
				lsn:      uint64(lsn),
				recLen:   tt.recLen,
				recType:  walRecTypeFull,
				recFlags: walRecordFlags,
				dataLen:  tt.dataLen,
			}
			_, err = encodeRecordHeader(header[:], &h)
			require.NoError(t, err)
			_, err = w.segments.active.file.WriteAt(header[:], int64(testLSNOffset(lsn)))
			require.NoError(t, err)

			_, _, err = w.Read(lsn)
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestWALReadRejectsSegmentCrossingRecord(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	seg := w.segments.active

	data := []byte("cross-boundary")
	buf := make([]byte, encodedRecordLen(data))
	off := seg.segMaxSize - uint64(len(buf)) + 8
	lsn := testLSN(seg.segId, off)
	rec := newRecord(lsn, walRecordFlags, data)
	n, err := encodeRecord(buf, &rec)
	require.NoError(t, err)
	written, err := seg.file.WriteAt(buf, int64(off))
	require.NoError(t, err)
	require.Equal(t, n, written)
	seg.cursor = off + uint64(n)

	_, _, err = w.Read(lsn)
	require.ErrorIs(t, err, ErrRecordTorn)
}

func TestWALReadRejectsStoredLSNMismatch(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	seg := w.segments.active
	off := seg.cursor
	lsn := testLSN(seg.segId, off)
	data := []byte("bad-lsn")
	buf := make([]byte, encodedRecordLen(data))

	rec := newRecord(testLSN(seg.segId, off+8), walRecordFlags, data)
	n, err := encodeRecord(buf, &rec)
	require.NoError(t, err)
	written, err := seg.file.WriteAt(buf, int64(off))
	require.NoError(t, err)
	require.Equal(t, n, written)
	seg.cursor += uint64(n)

	_, _, err = w.Read(lsn)
	require.ErrorIs(t, err, ErrRecordInvalidLSN)
}

func TestWALReplayChecksumErrorCanTruncate(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	_, err := w.Append([]byte("good"))
	require.NoError(t, err)
	badLSN, err := w.Append([]byte("bad"))
	require.NoError(t, err)

	_, err = w.segments.active.file.WriteAt([]byte{0xff}, int64(testLSNOffset(badLSN)+walRecordHeaderLen+4))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	reopened := openTestWAL(t, dir)
	var got []string
	_, err = reopened.Replay(func(r Record) error {
		got = append(got, string(r.Data))
		return nil
	})
	require.ErrorIs(t, err, ErrRecordChecksumMismatch)
	assert.Equal(t, []string{"good"}, got)

	require.NoError(t, reopened.TruncateFrom(badLSN))

	_, _, err = reopened.Read(badLSN)
	require.ErrorIs(t, err, io.EOF)
}

func TestReadRecordRejectsHugeDataLength(t *testing.T) {
	buf := make([]byte, encodedRecordLen(nil))
	h := walRecordHeader{
		lsn:      uint64(testLSN(0, walHeaderLenPadded)),
		recLen:   uint32(len(buf)),
		recType:  walRecTypeFull,
		recFlags: walRecordFlags,
		dataLen:  1 << 30,
	}
	_, err := encodeRecordHeader(buf, &h)
	require.NoError(t, err)

	_, _, err = readRecord(bytes.NewReader(buf), 0)
	require.ErrorIs(t, err, ErrRecordInvalidSize)
}

func TestReadRecordHeaderRejectsBadCRC(t *testing.T) {
	data := []byte("record")
	buf := make([]byte, encodedRecordLen(data))
	lsn := testLSN(0, walHeaderLenPadded)
	rec := newRecord(lsn, walRecordFlags, data)
	_, err := encodeRecord(buf, &rec)
	require.NoError(t, err)
	buf[16]++ // corrupt header while leaving hcrc unchanged

	_, _, err = readRecordHeader(bytes.NewReader(buf), 0)
	require.ErrorIs(t, err, ErrRecordChecksumMismatch)
}

func TestReadRecordHeaderRejectsBadMagic(t *testing.T) {
	data := []byte("record")
	buf := make([]byte, encodedRecordLen(data))
	lsn := testLSN(0, walHeaderLenPadded)
	rec := newRecord(lsn, walRecordFlags, data)
	_, err := encodeRecord(buf, &rec)
	require.NoError(t, err)
	copy(buf[:4], []byte("NOPE"))

	_, _, err = readRecordHeader(bytes.NewReader(buf), 0)
	require.ErrorIs(t, err, ErrRecordInvalidFormat)
}

func TestReadRecordHeaderRejectsBadLengths(t *testing.T) {
	for _, tt := range corruptRecordLengthCases() {
		t.Run(tt.name, func(t *testing.T) {
			buf := make([]byte, walRecordHeaderLen)
			h := walRecordHeader{
				lsn:      uint64(testLSN(0, walHeaderLenPadded)),
				recLen:   tt.recLen,
				recType:  walRecTypeFull,
				recFlags: walRecordFlags,
				dataLen:  tt.dataLen,
			}
			_, err := encodeRecordHeader(buf, &h)
			require.NoError(t, err)

			_, _, err = readRecordHeader(bytes.NewReader(buf), 0)
			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestReadRecordBytesRejectsTornInput(t *testing.T) {
	data := []byte("record")
	buf := make([]byte, encodedRecordLen(data))
	lsn := testLSN(0, walHeaderLenPadded)
	rec := newRecord(lsn, walRecordFlags, data)
	_, err := encodeRecord(buf, &rec)
	require.NoError(t, err)

	_, err = readRecordBytes(buf[:walRecordHeaderLen-1], 0)
	require.ErrorIs(t, err, ErrRecordTorn)

	_, err = readRecordBytes(buf[:walRecordHeaderLen+1], 0)
	require.ErrorIs(t, err, ErrRecordTorn)
}

func TestReadRecordRejectsTruncatedInput(t *testing.T) {
	data := []byte("record")
	buf := make([]byte, encodedRecordLen(data))
	lsn := testLSN(0, walHeaderLenPadded)
	rec := newRecord(lsn, walRecordFlags, data)
	_, err := encodeRecord(buf, &rec)
	require.NoError(t, err)

	_, _, err = readRecord(bytes.NewReader(buf[:walRecordHeaderLen+1]), 0)
	require.ErrorIs(t, err, ErrRecordTorn)
}

func zeroRecordBodyAndCRC(t *testing.T, w *WAL, lsn LSN, data []byte) {
	t.Helper()
	zeroes := make([]byte, encodedRecordLen(data)-walRecordHeaderLen)
	written, err := w.segments.active.file.WriteAt(zeroes, int64(testLSNOffset(lsn)+walRecordHeaderLen))
	require.NoError(t, err)
	require.Equal(t, len(zeroes), written)
}

func openTestWAL(t *testing.T, dir string, opts ...Option) *WAL {
	t.Helper()
	if len(opts) == 0 {
		opts = append(opts, WithSegmentSize(walTestSegmentSize))
	}
	w, err := New(dir, opts...)
	require.NoError(t, err)
	require.NoError(t, w.Open())
	t.Cleanup(func() {
		require.NoError(t, w.Close())
	})
	return w
}

func replayStrings(t *testing.T, w *WAL) []string {
	t.Helper()
	var got []string
	_, err := w.Replay(func(r Record) error {
		got = append(got, string(r.Data))
		return nil
	})
	require.NoError(t, err)
	return got
}

func appendAcrossRollover(t *testing.T, w *WAL, first, second []byte) (LSN, LSN, error) {
	t.Helper()
	lsn1, err := w.Append(first)
	if err != nil {
		return 0, 0, err
	}
	lsn2, err := w.Append(second)
	return lsn1, lsn2, err
}

func assertInactiveSegmentMapped(t *testing.T, w *WAL, sid segID) {
	t.Helper()
	seg := w.segments.byID[sid]
	require.NotNil(t, seg)
	require.NotSame(t, w.segments.active, seg)
	assert.Equal(t, segmentModeReadOnly, seg.mode)
	assert.Nil(t, seg.file)
	assert.NotNil(t, seg.mmap)
}

func assertInactiveSegmentReadyForMmap(t *testing.T, w *WAL, sid segID) {
	t.Helper()
	seg := w.segments.byID[sid]
	require.NotNil(t, seg)
	require.NotSame(t, w.segments.active, seg)
	assert.Equal(t, segmentModeReadOnly, seg.mode)
	assert.NotNil(t, seg.file)
	assert.Nil(t, seg.mmap)
}

func assertInactiveSegmentUnmapped(t *testing.T, w *WAL, sid segID) {
	t.Helper()
	seg := w.segments.byID[sid]
	require.NotNil(t, seg)
	require.NotSame(t, w.segments.active, seg)
	assert.Equal(t, segmentModeReadOnly, seg.mode)
	assert.Nil(t, seg.file)
	assert.Nil(t, seg.mmap)
}

func assertActiveSegmentReadWrite(t *testing.T, w *WAL) {
	t.Helper()
	require.NotNil(t, w.segments.active)
	assert.Equal(t, segmentModeReadWrite, w.segments.active.mode)
	assert.NotNil(t, w.segments.active.file)
	assert.Nil(t, w.segments.active.mmap)
}

func assertSegmentCursorMatchesFileSize(t *testing.T, seg *walSegment) {
	t.Helper()
	info, err := os.Stat(seg.path)
	require.NoError(t, err)
	assert.Equal(t, int64(seg.cursor), info.Size(), "segment id=%d", seg.segId)
}
