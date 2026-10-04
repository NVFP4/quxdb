package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
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
			name:    "huge data length",
			recLen:  uint32(encodedRecordSize(0)),
			dataLen: 1 << 30,
			want:    ErrRecordInvalidSize,
		},
	}
}

func TestIsCorruptionError(t *testing.T) {
	for _, err := range []error{
		ErrRecordInvalidFormat,
		ErrRecordInvalidSize,
		ErrRecordInvalidLSN,
		ErrRecordTorn,
		ErrRecordChecksumMismatch,
	} {
		assert.True(t, isCorruptionError(fmt.Errorf("wrapped: %w", err)))
	}
	assert.False(t, isCorruptionError(io.ErrUnexpectedEOF))
	assert.False(t, isCorruptionError(errors.New("callback failed")))
}

func TestWALAppendRequiresRecover(t *testing.T) {
	dir := t.TempDir()
	w, err := New(dir, WithSegmentSize(walTestSegmentSize))
	require.NoError(t, err)

	_, err = w.Append(parts("early"), DurabilitySynced)
	require.ErrorIs(t, err, ErrNotRecovered)

	require.NoError(t, w.Recover(0, func(Record) error { return nil }, StopOnCorruption))
	require.NoError(t, w.Close())
	_, err = w.Append(parts("late"), DurabilitySynced)
	require.ErrorIs(t, err, ErrNotRecovered)
}

func TestWALRecoverReadsSegmentsWithIDGaps(t *testing.T) {
	dir := t.TempDir()

	seg1, err := newSegment(dir, 1, walTestSegmentSize, "")
	require.NoError(t, err)
	lsn1 := writeRecordAt(t, seg1, []byte("record-1"))
	require.NoError(t, seg1.close())

	seg3, err := newSegment(dir, 3, walTestSegmentSize, "")
	require.NoError(t, err)
	lsn3 := writeRecordAt(t, seg3, []byte("record-3"))
	require.NoError(t, seg3.close())

	w, got := recoverTestWAL(t, dir, 0)
	require.Equal(t, segID(3), w.segments.active.segId)
	assert.Equal(t, []testRecord{{lsn1, "record-1"}, {lsn3, "record-3"}}, got)

	data, _, err := readAt(w, lsn1)
	require.NoError(t, err)
	assert.Equal(t, []byte("record-1"), data)
	assertInactiveSegmentMapped(t, w, 1)
}

func TestSegmentNameOrdersByTimestampThenID(t *testing.T) {
	createdAt := time.Unix(1_700_000_000, 123_456_789).UTC()

	assert.Less(t, segmentName(createdAt, 0), segmentName(createdAt, ^segID(0)))
	assert.Less(t, segmentName(createdAt, ^segID(0)), segmentName(createdAt.Add(time.Millisecond), 0))
}

func TestWALRecoverReplaysAcrossRollover(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	first := bytes.Repeat([]byte{'a'}, 4000)
	lsn1, lsn2 := appendTwo(t, w, first, []byte("two"))
	require.Equal(t, segID(1), testLSNSegID(lsn2))
	assertInactiveSegmentUnmapped(t, w, 0)
	require.NoError(t, w.Close())

	reopened, got := recoverTestWAL(t, dir, 0)
	assert.Equal(t, []testRecord{{lsn1, string(first)}, {lsn2, "two"}}, got)
	assertInactiveSegmentUnmapped(t, reopened, 0)
	assert.Equal(t, testLSNOffset(lsn2)+uint64(encodedRecordLen([]byte("two"))), reopened.segments.active.cursor)
}

func TestWALRecoverAfterLSN(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	_, second, third := appendThree(t, w, "one", "two", "three")
	require.NoError(t, w.Close())

	_, got := recoverTestWAL(t, dir, second)
	assert.Equal(t, []testRecord{{third, "three"}}, got)
}

func TestWALRecoverAfterLastRecordAppendsAfterIt(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	lsn := appendOne(t, w, "one")
	require.NoError(t, w.Close())

	reopened, got := recoverTestWAL(t, dir, lsn)
	assert.Empty(t, got)

	next := appendOne(t, reopened, "two")
	assert.Equal(t, testLSN(testLSNSegID(lsn), testLSNOffset(lsn)+uint64(encodedRecordLen([]byte("one")))), next)
}

func TestWALRecoverAfterLSNAcrossSegments(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	firstLSN, secondLSN := appendTwo(t, w, bytes.Repeat([]byte{'a'}, 4000), []byte("two"))
	require.NoError(t, w.Close())

	reopened, got := recoverTestWAL(t, dir, firstLSN)
	assert.Equal(t, []testRecord{{secondLSN, "two"}}, got)
	assertInactiveSegmentUnmapped(t, reopened, 0)
}

func TestWALRecoverRejectsInvalidAfter(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	appendOne(t, w, "one")
	invalid := testLSN(w.segments.active.segId, w.segments.active.cursor+1)
	require.NoError(t, w.Close())

	_, err := recoverWAL(t, dir, invalid, StopOnCorruption, func(Record) error { return nil })
	require.ErrorIs(t, err, io.EOF)
}

func TestWALRecoverStopsOnCallbackError(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	appendThree(t, w, "one", "two", "three")
	require.NoError(t, w.Close())

	stop := errors.New("stop replay")
	var got []string
	_, err := recoverWAL(t, dir, 0, StopOnCorruption, func(r Record) error {
		got = append(got, string(r.Data))
		if string(r.Data) == "two" {
			return stop
		}
		return nil
	})
	require.ErrorIs(t, err, stop)
	assert.Equal(t, []string{"one", "two"}, got)
}

func TestWALNewSegmentIsFullSizeWithCursorAfterHeader(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	assert.Equal(t, uint64(walHeaderLenPadded), w.segments.active.cursor)
	assert.Equal(t, uint64(walHeaderLenPadded), testLSNOffset(w.segments.active.startLSN))
	assertActiveSegmentReadWrite(t, w)
	info, err := os.Stat(w.segments.active.path)
	require.NoError(t, err)
	assert.Equal(t, int64(walTestSegmentSize), info.Size())
}

func TestWALOpenRemovesUnfinishedSegments(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	require.NoError(t, w.Close())

	stale := filepath.Join(dir, walDir, segmentName(time.Now(), 9)+walTmpSuffix)
	require.NoError(t, os.WriteFile(stale, []byte("partial"), 0o644))

	reopened := openTestWAL(t, dir)
	assert.NoFileExists(t, stale)
	assert.Len(t, reopened.segments.segments, 1)
}

func TestWALTruncateDeletesLaterSegments(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	w.segments.stopPreparer() // truncation only runs before the preparer starts

	lsn1, lsn2 := appendTwo(t, w, bytes.Repeat([]byte{'a'}, 4000), bytes.Repeat([]byte{'b'}, 100))
	require.Equal(t, segID(1), testLSNSegID(lsn2))

	old := w.segments.byID[0]
	removedPath := w.segments.byID[1].path
	require.NoError(t, w.segments.truncateTail(lsn1))
	require.Same(t, old, w.segments.active)
	assert.Equal(t, testLSNOffset(lsn1), w.segments.active.cursor)
	assertActiveSegmentReadWrite(t, w)

	_, _, err := readAt(w, lsn2)
	require.ErrorContains(t, err, "unknown segment id=1")
	assert.NoFileExists(t, removedPath)

	lsn3 := appendOne(t, w, "three")
	assert.Equal(t, lsn1, lsn3)
	data, _, err := readAt(w, lsn3)
	require.NoError(t, err)
	assert.Equal(t, []byte("three"), data)
}

func TestWALTruncatedRecordsStayGoneAfterReopen(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	w.segments.stopPreparer()
	one, two, _ := appendThree(t, w, "one", "two", "three")

	require.NoError(t, w.segments.truncateTail(two))
	require.NoError(t, w.Close())

	reopened, got := recoverTestWAL(t, dir, 0)
	assert.Equal(t, []testRecord{{one, "one"}}, got)
	assert.Equal(t, testLSNOffset(two), reopened.segments.active.cursor)
}

func TestWALTruncateRejectsOutOfBounds(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	w.segments.stopPreparer()

	lsn := appendOne(t, w, "one")
	cursor := w.segments.active.cursor

	require.ErrorIs(t, w.segments.truncateTail(testLSN(testLSNSegID(lsn), uint64(walHeaderLenPadded-1))), ErrTruncateOutOfBounds)
	require.ErrorIs(t, w.segments.truncateTail(testLSN(testLSNSegID(lsn), cursor+1)), ErrTruncateOutOfBounds)
	require.ErrorContains(t, w.segments.truncateTail(testLSN(99, walHeaderLenPadded)), "unknown segment id=99")
	assert.Equal(t, cursor, w.segments.active.cursor)
}

func TestWALPruneKeepsRetiredSegmentsForReuse(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	w.segments.stopPreparer() // the preparer would take spares as they appear

	a, b := appendTwo(t, w, bytes.Repeat([]byte{'a'}, 4000), bytes.Repeat([]byte{'b'}, 4000))
	c := appendOne(t, w, "three")
	require.Equal(t, []segID{0, 1, 2}, []segID{testLSNSegID(a), testLSNSegID(b), testLSNSegID(c)})
	seg0, seg1 := w.segments.byID[0].path, w.segments.byID[1].path

	require.NoError(t, w.Prune())
	assert.Len(t, w.segments.segments, 3, "no retain point prunes nothing")

	w.RetainFrom(b)
	require.NoError(t, w.Prune())
	assert.Equal(t, []segID{1, 2}, segmentIDs(w))
	assert.NoFileExists(t, seg0)
	assert.FileExists(t, seg0+walPreparedSuffix)

	w.RetainFrom(c)
	require.NoError(t, w.Prune())
	assert.Equal(t, []segID{2}, segmentIDs(w))
	assert.Equal(t, []string{seg0 + walPreparedSuffix, seg1 + walPreparedSuffix}, w.segments.spares)
	require.NoError(t, w.Close())

	reopened, got := recoverTestWAL(t, dir, c)
	assert.Empty(t, got)
	assert.Equal(t, []segID{2}, segmentIDs(reopened))
}

func TestWALPruneDeletesBeyondSpareLimit(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	w.segments.stopPreparer()

	var last LSN
	for range walMaxSpares + 2 {
		last = appendOne(t, w, strings.Repeat("x", 4000))
	}
	paths := segmentPaths(w)

	w.RetainFrom(last)
	require.NoError(t, w.Prune())
	assert.Len(t, w.segments.spares, walMaxSpares)
	for _, path := range paths[:walMaxSpares] {
		assert.FileExists(t, path+walPreparedSuffix)
	}
	for _, path := range paths[walMaxSpares : len(paths)-1] {
		assert.NoFileExists(t, path)
		assert.NoFileExists(t, path+walPreparedSuffix)
	}
}

func TestWALRolloverRecyclesSpareWithoutReplayingItsOldRecords(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	w.segments.stopPreparer() // the rollover below must recycle the spare inline

	// fill segment 0 with same-size records so their boundaries line up with new ones
	var retain LSN
	for {
		retain = appendOne(t, w, "old-record")
		if testLSNSegID(retain) != 0 {
			break
		}
	}
	w.RetainFrom(retain)
	require.NoError(t, w.Prune())
	require.Len(t, w.segments.spares, 1)
	spare := w.segments.spares[0]
	spareInfo, err := os.Stat(spare)
	require.NoError(t, err)

	// fill segment 1 exactly so the next record rolls over into segment 0's file
	appendOne(t, w, string(make([]byte, recordCapacity(w.segments.active.segMaxSize-w.segments.active.cursor))))
	newLSN := appendOne(t, w, "new-record")
	require.Equal(t, segID(2), testLSNSegID(newLSN))
	assert.Empty(t, w.segments.spares)
	assert.NoFileExists(t, spare)
	activeInfo, err := os.Stat(w.segments.active.path)
	require.NoError(t, err)
	assert.True(t, os.SameFile(spareInfo, activeInfo))

	// lose the end marker: the stale header beneath it belongs to segment 0
	end := testLSNOffset(newLSN) + uint64(encodedRecordLen([]byte("new-record")))
	stale := encodeTestRecordAt(testLSN(0, end), []byte("old-record"))[:walEndMarkerLen]
	_, err = w.segments.active.file.WriteAt(stale, int64(end))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	_, got := recoverTestWAL(t, dir, retain)
	require.NotEmpty(t, got)
	assert.Equal(t, testRecord{newLSN, "new-record"}, got[len(got)-1])
	for _, rec := range got {
		assert.NotEqual(t, "old-record", rec.data)
	}
}

func TestWALPreparerSuppliesNextSegment(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	next := waitPrepared(t, w)
	assert.Equal(t, w.segments.active.segId+1, next.segId)
	assert.True(t, strings.HasSuffix(next.path, walPreparedSuffix))
	assert.FileExists(t, next.path)

	appendOne(t, w, strings.Repeat("f", 4000))
	lsn := appendOne(t, w, "next")
	assert.Same(t, next, w.segments.active)
	assert.Equal(t, next.segId, testLSNSegID(lsn))
	assert.False(t, strings.HasSuffix(next.path, walPreparedSuffix))
	assert.FileExists(t, next.path)

	refilled := waitPrepared(t, w)
	assert.Equal(t, next.segId+1, refilled.segId)
}

func TestWALPreparerRecyclesPrunedSegments(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	w.segments.stopPreparer()

	appendOne(t, w, strings.Repeat("a", 4000))
	retain := appendOne(t, w, "b")
	require.Equal(t, segID(1), testLSNSegID(retain))
	w.RetainFrom(retain)
	require.NoError(t, w.Prune())
	require.Len(t, w.segments.spares, 1)
	spareInfo, err := os.Stat(w.segments.spares[0])
	require.NoError(t, err)

	w.segments.startPreparer()
	next := waitPrepared(t, w)
	nextInfo, err := os.Stat(next.path)
	require.NoError(t, err)
	assert.True(t, os.SameFile(spareInfo, nextInfo), "preparer built a new file instead of recycling")
	assert.Empty(t, w.segments.spares)
}

func TestWALPreparedSegmentLeftOnCloseIsReused(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	one := appendOne(t, w, "one")
	prepared := waitPrepared(t, w).path
	require.NoError(t, w.Close())

	preparedInfo, err := os.Stat(prepared)
	require.NoError(t, err)

	reopened, got := recoverTestWAL(t, dir, 0)
	assert.Equal(t, []testRecord{{one, "one"}}, got)
	for _, seg := range reopened.segments.segments {
		assert.False(t, strings.HasSuffix(seg.path, walPreparedSuffix), "prepared file opened as a segment")
	}

	// the reopened preparer recycles it as the next segment
	nextInfo, err := os.Stat(waitPrepared(t, reopened).path)
	require.NoError(t, err)
	assert.True(t, os.SameFile(preparedInfo, nextInfo))
}

func TestWALPruneRunsBesideAppends(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	const n = 400
	lsns := make([]LSN, n)
	data := func(i int) string { return fmt.Sprintf("record-%03d-%s", i, strings.Repeat("p", 100)) }
	var last atomic.Uint64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(done)
		for i := range n {
			lsn, err := w.Append(parts(data(i)), DurabilitySynced)
			if err != nil {
				t.Error(err)
				return
			}
			lsns[i] = lsn
			last.Store(uint64(lsn))
		}
	})
	var retain LSN
	wg.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if lsn := LSN(last.Load()); lsn != 0 {
				retain = lsn
				w.RetainFrom(lsn)
				if err := w.Prune(); err != nil {
					t.Error(err)
					return
				}
			}
		}
	})
	wg.Wait()
	require.NoError(t, w.Close())

	_, got := recoverTestWAL(t, dir, retain)
	var want []testRecord
	for i, lsn := range lsns {
		if lsn > retain {
			want = append(want, testRecord{lsn, data(i)})
		}
	}
	assert.Equal(t, want, got)
}

func TestWALAppendSplitsRecordAcrossSegments(t *testing.T) {
	for _, tt := range []struct {
		name     string
		size     int
		segments int
	}{
		{name: "two segments", size: 6000, segments: 2},
		{name: "three segments", size: 10000, segments: 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			w := openTestWAL(t, dir)

			data := patterned(tt.size)
			// empty parts, and parts that straddle record boundaries
			lsn, err := w.Append([][]byte{nil, data[:1], data[1:700], {}, data[700:]}, DurabilitySynced)
			require.NoError(t, err)
			afterLSN := appendOne(t, w, "after")
			assert.Equal(t, segID(0), testLSNSegID(lsn))
			assert.Equal(t, segID(tt.segments-1), testLSNSegID(afterLSN))

			got, next, err := readAt(w, lsn)
			require.NoError(t, err)
			assert.Equal(t, data, got)
			assert.Equal(t, afterLSN, next)
			require.NoError(t, w.Close())

			_, records := recoverTestWAL(t, dir, 0)
			assert.Equal(t, []testRecord{{lsn, string(data)}, {afterLSN, "after"}}, records)
		})
	}
}

func TestWALAppendWritesMorePartsThanIOVMax(t *testing.T) {
	w := openTestWAL(t, t.TempDir(), WithSegmentSize(16<<20))

	var ps [][]byte
	var want []byte
	for i := range 3000 {
		p := patterned(1 + i%300)
		ps = append(ps, p)
		want = append(want, p...)
	}

	lsn, err := w.Append(ps, DurabilitySynced)
	require.NoError(t, err)
	got, _, err := readAt(w, lsn)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestWALRecoverCutsTornSplitRecord(t *testing.T) {
	for _, tt := range []struct {
		name string
		at   uint64 // offset into the last partial walRecord that gets damaged
		with []byte
	}{
		{name: "last partial walRecord lost", at: 0, with: make([]byte, walRecordHeaderLen)},
		{name: "last partial walRecord header intact, data torn", at: walRecordHeaderLen + 5, with: []byte{0xff}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			w := openTestWAL(t, dir)

			good := appendOne(t, w, "good")
			split := appendOne(t, w, string(patterned(6000)))
			last := w.segments.active
			require.Equal(t, segID(1), last.segId)
			_, err := last.file.WriteAt(tt.with, int64(testLSNOffset(last.startLSN)+tt.at))
			require.NoError(t, err)
			require.NoError(t, w.Close())

			reopened, got := recoverTestWAL(t, dir, 0)
			assert.Equal(t, []testRecord{{good, "good"}}, got)
			assert.Equal(t, []segID{0}, segmentIDs(reopened))
			assert.Equal(t, split, appendOne(t, reopened, "after"))
		})
	}
}

func TestWALRecoverSkipsPrunedRecordTail(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)
	w.segments.stopPreparer() // the reads below race with a concurrent prune

	// partial walRecords in segments 0, 1 and 2
	appendOne(t, w, string(patterned(10000)))
	tailLSN := appendOne(t, w, "tail")
	require.Equal(t, segID(2), testLSNSegID(tailLSN))
	middle := w.segments.byID[1].startLSN

	w.RetainFrom(middle)
	require.NoError(t, w.Prune())
	require.Equal(t, []segID{1, 2}, segmentIDs(w))
	_, _, err := readAt(w, middle)
	require.ErrorIs(t, err, ErrRecordInvalidLSN)
	require.NoError(t, w.Close())

	_, got := recoverTestWAL(t, dir, 0)
	assert.Equal(t, []testRecord{{tailLSN, "tail"}}, got)
}

func TestWALAppendRollsOverWhenFull(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	old := w.segments.active
	fill := strings.Repeat("f", 4000)
	fillLSN := appendOne(t, w, fill)
	oldCursor := old.cursor

	lsn := appendOne(t, w, "next")
	assert.Equal(t, uint64(walHeaderLenPadded), testLSNOffset(fillLSN))
	assert.Equal(t, oldCursor, old.cursor)
	assert.Equal(t, testLSN(1, walHeaderLenPadded), lsn)
	assertInactiveSegmentUnmapped(t, w, 0)
	require.NoError(t, w.Close())

	_, got := recoverTestWAL(t, dir, 0)
	assert.Equal(t, []testRecord{{fillLSN, fill}, {lsn, "next"}}, got)
}

func TestWALRecoverCutsInvalidTail(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	good := appendOne(t, w, "good")
	goodNext := testLSN(0, testLSNOffset(good)+uint64(encodedRecordLen([]byte("good"))))

	badHeader := make([]byte, walRecordHeaderLen)
	copy(badHeader, "NOPE")
	_, err := w.segments.active.file.WriteAt(badHeader, int64(testLSNOffset(goodNext)))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	reopened, got := recoverTestWAL(t, dir, 0)
	assert.Equal(t, []testRecord{{good, "good"}}, got)
	assert.Equal(t, testLSNOffset(goodNext), reopened.segments.active.cursor)
	marker := make([]byte, walEndMarkerLen)
	_, err = reopened.segments.active.file.ReadAt(marker, int64(testLSNOffset(goodNext)))
	require.NoError(t, err)
	assert.Equal(t, endMarker[:], marker)

	afterLSN := appendOne(t, reopened, "after")
	assert.Equal(t, goodNext, afterLSN)
	require.NoError(t, reopened.Close())

	_, got = recoverTestWAL(t, dir, 0)
	assert.Equal(t, []testRecord{{good, "good"}, {afterLSN, "after"}}, got)
}

func TestWALRecoverCutsTornLastRecord(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	good := appendOne(t, w, "good")
	tailLSN := appendOne(t, w, "tail")
	zeroRecordBodyAndCRC(t, w, tailLSN, []byte("tail"))
	require.NoError(t, w.Close())

	reopened, got := recoverTestWAL(t, dir, 0)
	assert.Equal(t, []testRecord{{good, "good"}}, got)
	assert.Equal(t, tailLSN, appendOne(t, reopened, "after"))
}

func TestWALRecoverMidLogCorruption(t *testing.T) {
	setup := func(t *testing.T) (dir string, good, bad LSN) {
		dir = t.TempDir()
		w := openTestWAL(t, dir)
		good, bad, _ = appendThree(t, w, "good", "bad", "third")
		zeroRecordBodyAndCRC(t, w, bad, []byte("bad"))
		require.NoError(t, w.Close())
		return dir, good, bad
	}

	t.Run("stop leaves the log untouched", func(t *testing.T) {
		dir, _, bad := setup(t)
		for range 2 {
			_, err := recoverWAL(t, dir, 0, StopOnCorruption, func(Record) error { return nil })
			var corrupt *CorruptionError
			require.ErrorAs(t, err, &corrupt)
			assert.Equal(t, bad, corrupt.LSN)
			require.ErrorIs(t, err, ErrMidLogCorruption)
			require.ErrorIs(t, err, ErrRecordChecksumMismatch)
		}
	})

	t.Run("truncate keeps records before the damage", func(t *testing.T) {
		dir, good, bad := setup(t)
		w, err := recoverWAL(t, dir, 0, TruncateOnCorruption, func(Record) error { return nil })
		require.NoError(t, err)
		// "after" encodes to the size of "bad", so without its end marker "third" would read as valid again
		after := appendOne(t, w, "after")
		assert.Equal(t, bad, after)
		require.NoError(t, w.Close())

		_, got := recoverTestWAL(t, dir, 0)
		assert.Equal(t, []testRecord{{good, "good"}, {after, "after"}}, got)
	})
}

func TestWALRecoverStopsOnCorruptionBeforeLaterSegment(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	first, second := appendTwo(t, w, bytes.Repeat([]byte{'a'}, 4000), []byte("two"))
	require.Equal(t, segID(1), testLSNSegID(second))
	_, err := w.segments.byID[0].file.WriteAt([]byte{0xff}, int64(testLSNOffset(first)+walRecordHeaderLen+10))
	if err != nil {
		// sealed segments are read-only, write through the path instead
		file, openErr := os.OpenFile(w.segments.byID[0].path, os.O_RDWR, 0)
		require.NoError(t, openErr)
		_, err = file.WriteAt([]byte{0xff}, int64(testLSNOffset(first)+walRecordHeaderLen+10))
		require.NoError(t, errors.Join(err, file.Close()))
	}
	require.NoError(t, w.Close())

	_, err = recoverWAL(t, dir, 0, StopOnCorruption, func(Record) error { return nil })
	var corrupt *CorruptionError
	require.ErrorAs(t, err, &corrupt)
	assert.Equal(t, first, corrupt.LSN)
}

func TestWALRecoverStopsOnCorruptAfterRecord(t *testing.T) {
	for _, policy := range []CorruptionPolicy{StopOnCorruption, TruncateOnCorruption} {
		dir := t.TempDir()
		w := openTestWAL(t, dir)
		one, _, _ := appendThree(t, w, "one", "two", "three")
		zeroRecordBodyAndCRC(t, w, one, []byte("one"))
		require.NoError(t, w.Close())

		_, err := recoverWAL(t, dir, one, policy, func(Record) error { return nil })
		var corrupt *CorruptionError
		require.ErrorAs(t, err, &corrupt, "policy=%d", policy)
		assert.Equal(t, one, corrupt.LSN)
	}
}

func TestWALFailureIsSticky(t *testing.T) {
	w, err := New(t.TempDir(), WithSegmentSize(walTestSegmentSize))
	require.NoError(t, err)
	require.NoError(t, w.Recover(0, func(Record) error { return nil }, StopOnCorruption))
	t.Cleanup(func() { _ = w.Close() }) // the active file is closed below
	appendOne(t, w, "ok")

	require.NoError(t, w.segments.active.file.Close()) // every write now fails
	_, err1 := w.Append(parts("x"), DurabilitySynced)
	_, err2 := w.Append(parts("y"), DurabilityWritten)
	require.ErrorIs(t, err1, ErrWALFailed)
	assert.Equal(t, err1, err2)
	assert.Contains(t, err1.Error(), w.segments.active.path)
	assert.NotContains(t, err1.Error(), walPreparedSuffix)
}

func TestWALOpenRejectsBadSegmentHeaderCRC(t *testing.T) {
	dir := t.TempDir()
	seg, err := newSegment(dir, 0, walTestSegmentSize, "")
	require.NoError(t, err)
	segPath := seg.path
	require.NoError(t, seg.close())

	file, err := os.OpenFile(segPath, os.O_RDWR, 0)
	require.NoError(t, err)
	_, err = file.WriteAt([]byte{0xff}, 8) // segId is covered by the header CRC.
	require.NoError(t, err)
	require.NoError(t, file.Close())

	_, err = recoverWAL(t, dir, 0, StopOnCorruption, func(Record) error { return nil })
	require.ErrorIs(t, err, ErrHeaderChecksumMismatch)
}

func TestSegmentSetRolloverWrapsSegmentID(t *testing.T) {
	dir := t.TempDir()
	segments := newSegmentSet(dir, walTestSegmentSize)
	old, err := newSegment(dir, ^segID(0), walTestSegmentSize, "")
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

func TestWALEmptyRecordRoundTrip(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	lsn, err := w.Append(nil, DurabilitySynced)
	require.NoError(t, err)

	data, next, err := readAt(w, lsn)
	require.NoError(t, err)
	assert.Empty(t, data)
	assert.Equal(t, testLSN(testLSNSegID(lsn), testLSNOffset(lsn)+uint64(encodedRecordLen(nil))), next)
	require.NoError(t, w.Close())

	_, got := recoverTestWAL(t, dir, 0)
	assert.Equal(t, []testRecord{{lsn, ""}}, got)
}

func TestWALLargestFullRecordRoundTrip(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	dataLen := recordCapacity(w.segments.active.segMaxSize - w.segments.active.cursor)
	data := bytes.Repeat([]byte{0x7b}, dataLen)
	lsn, err := w.Append([][]byte{data}, DurabilitySynced)
	require.NoError(t, err)
	assert.Len(t, w.segments.segments, 1)

	got, next, err := readAt(w, lsn)
	require.NoError(t, err)
	assert.Equal(t, data, got)
	assert.Equal(t, testLSN(testLSNSegID(lsn), testLSNOffset(lsn)+uint64(encodedRecordLen(data))), next)
}

func TestWALReadRejectsInvalidLSN(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	lsn := appendOne(t, w, "one")

	_, _, err := readAt(w, testLSN(testLSNSegID(lsn), 0))
	require.ErrorIs(t, err, ErrSegmentLSNInvalid)

	_, _, err = readAt(w, testLSN(testLSNSegID(lsn), w.segments.active.cursor))
	require.ErrorIs(t, err, io.EOF)

	_, _, err = readAt(w, testLSN(42, walHeaderLenPadded))
	require.ErrorContains(t, err, "unknown segment id=42")
}

func TestWALRecordPaddingZeroed(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	data := []byte("abc")
	lsn := appendOne(t, w, string(data))

	buf := make([]byte, encodedRecordLen(data))
	n, err := w.segments.active.file.ReadAt(buf, int64(testLSNOffset(lsn)))
	require.NoError(t, err)
	require.Equal(t, len(buf), n)

	pad := buf[walRecordHeaderLen+len(data)+walRecordMetaLen:]
	require.NotEmpty(t, pad)
	assert.Equal(t, make([]byte, len(pad)), pad)
}

func TestWALAppendDoesNotRetainCallerBuffer(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	data := bytes.Repeat([]byte("owned"), 1000)
	want := bytes.Clone(data)
	lsn, err := w.Append([][]byte{data}, DurabilityWritten)
	require.NoError(t, err)
	clear(data)

	got, _, err := readAt(w, lsn)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestWALWrittenAndSyncedRecordsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	w := openTestWAL(t, dir)

	written, err := w.Append(parts("written"), DurabilityWritten)
	require.NoError(t, err)
	synced, err := w.Append(parts("synced"), DurabilitySynced)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	_, got := recoverTestWAL(t, dir, 0)
	assert.Equal(t, []testRecord{{written, "written"}, {synced, "synced"}}, got)
}

func TestWALReadRejectsBadRecordMagic(t *testing.T) {
	w := openTestWAL(t, t.TempDir())

	appendOne(t, w, "good")
	badLSN := appendOne(t, w, "bad-magic")

	_, err := w.segments.active.file.WriteAt([]byte("NOPE"), int64(testLSNOffset(badLSN)))
	require.NoError(t, err)

	_, _, err = readAt(w, badLSN)
	require.ErrorIs(t, err, ErrRecordInvalidFormat)
}

func TestWALReadRejectsBadRecordLengths(t *testing.T) {
	for _, tt := range corruptRecordLengthCases() {
		t.Run(tt.name, func(t *testing.T) {
			w := openTestWAL(t, t.TempDir())
			lsn := appendOne(t, w, "payload")

			var header [walRecordHeaderLen]byte
			h := walRecordHeader{
				lsn:     uint64(lsn),
				recLen:  tt.recLen,
				dataLen: tt.dataLen,
			}
			_, err := encodeRecordHeader(header[:], &h)
			require.NoError(t, err)
			_, err = w.segments.active.file.WriteAt(header[:], int64(testLSNOffset(lsn)))
			require.NoError(t, err)

			_, _, err = readAt(w, lsn)
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
	rec := newRecord(lsn, data)
	n := encodeRecord(buf, &rec)
	written, err := seg.file.WriteAt(buf[:len(buf)-8], int64(off))
	require.NoError(t, err)
	require.Equal(t, n-8, written)
	seg.cursor = off + uint64(n)

	_, _, err = readAt(w, lsn)
	require.ErrorIs(t, err, ErrRecordTorn)
}

func TestWALReadRejectsStoredLSNMismatch(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	seg := w.segments.active
	off := seg.cursor
	lsn := testLSN(seg.segId, off)
	data := []byte("bad-lsn")
	buf := make([]byte, encodedRecordLen(data))

	rec := newRecord(testLSN(seg.segId, off+8), data)
	n := encodeRecord(buf, &rec)
	written, err := seg.file.WriteAt(buf, int64(off))
	require.NoError(t, err)
	require.Equal(t, n, written)
	seg.cursor += uint64(n)

	_, _, err = readAt(w, lsn)
	require.ErrorIs(t, err, ErrRecordInvalidLSN)
}

func TestWALReadRejectsBadHeaderCRC(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	lsn := appendOne(t, w, "record")

	buf := encodeTestRecordAt(lsn, []byte("record"))
	buf[16]++ // corrupt header while leaving hcrc unchanged
	_, err := w.segments.active.file.WriteAt(buf, int64(testLSNOffset(lsn)))
	require.NoError(t, err)

	_, _, err = readAt(w, lsn)
	require.ErrorIs(t, err, ErrRecordChecksumMismatch)
}

func TestWALReadRejectsRecordPastCursor(t *testing.T) {
	for _, tt := range []struct {
		name string
		keep uint64
	}{
		{name: "torn header", keep: walRecordHeaderLen - 1},
		{name: "short record", keep: uint64(encodedRecordLen([]byte("payload"))) - 8},
	} {
		t.Run(tt.name, func(t *testing.T) {
			w := openTestWAL(t, t.TempDir())
			lsn := appendOne(t, w, "payload")
			w.segments.active.cursor = testLSNOffset(lsn) + tt.keep

			_, _, err := readAt(w, lsn)
			require.ErrorIs(t, err, ErrRecordTorn)
		})
	}
}

func TestWALReplayReadsActiveSegmentAcrossWindows(t *testing.T) {
	dir := t.TempDir()
	size := WithSegmentSize(1 << 20)
	w := openTestWAL(t, dir, size)

	var want []testRecord
	for i := range 2000 {
		data := fmt.Sprintf("record-%04d-%s", i, strings.Repeat("x", i%97))
		lsn, err := w.Append(parts(data), DurabilityWritten)
		require.NoError(t, err)
		want = append(want, testRecord{lsn, data})
	}
	big := string(patterned(3 * walReadAhead)) // larger than one read-ahead
	want = append(want, testRecord{appendOne(t, w, big), big})
	require.NoError(t, w.Close())

	reopened, got := recoverTestWAL(t, dir, 0, size)
	require.Len(t, reopened.segments.segments, 1)
	assert.Equal(t, want, got)
	assert.Nil(t, reopened.segments.active.rbuf)
}

func TestWALReadAfterTruncateSeesRewrittenTail(t *testing.T) {
	w := openTestWAL(t, t.TempDir())
	one, two := appendTwo(t, w, []byte("one"), []byte("two"))
	_, _, err := readAt(w, one) // reads ahead over two
	require.NoError(t, err)

	require.NoError(t, w.segments.truncateTail(two))
	require.Equal(t, two, appendOne(t, w, "six"))

	data, _, err := readAt(w, two)
	require.NoError(t, err)
	assert.Equal(t, []byte("six"), data)
}

func TestReadRecordBytesRejectsTornInput(t *testing.T) {
	buf := encodeTestRecordAt(testLSN(0, walHeaderLenPadded), []byte("record"))

	_, err := readRecordBytes(buf[:walRecordHeaderLen-1], 0)
	require.ErrorIs(t, err, ErrRecordTorn)

	_, err = readRecordBytes(buf[:walRecordHeaderLen+1], 0)
	require.ErrorIs(t, err, ErrRecordTorn)
}

func BenchmarkWALAppend(b *testing.B) {
	w, err := New(b.TempDir())
	require.NoError(b, err)
	require.NoError(b, w.Recover(0, func(Record) error { return nil }, StopOnCorruption))
	b.Cleanup(func() { require.NoError(b, w.Close()) })

	var ps [][]byte
	for i := range 128 {
		size := 64
		if i%4 == 0 {
			size = 8192
		}
		ps = append(ps, patterned(size))
	}
	for _, d := range []Durability{DurabilityWritten, DurabilitySynced} {
		b.Run(map[Durability]string{DurabilityWritten: "written", DurabilitySynced: "synced"}[d], func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := w.Append(ps, d); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type testRecord struct {
	lsn  LSN
	data string
}

func parts(s string) [][]byte {
	return [][]byte{[]byte(s)}
}

func encodedRecordLen(d []byte) int {
	return encodedRecordSize(len(d))
}

func patterned(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte(i % 251)
	}
	return data
}

func newRecord(lsn LSN, data []byte) walRecord {
	return walRecord{
		walRecordHeader: walRecordHeader{
			lsn:     uint64(lsn),
			recLen:  uint32(encodedRecordLen(data)),
			dataLen: uint32(len(data)),
		},
		data: data,
	}
}

func encodeRecord(dst []byte, rec *walRecord) int {
	off, _ := encodeRecordHeader(dst, &rec.walRecordHeader)
	off += copy(dst[off:], rec.data)
	binary.LittleEndian.PutUint32(dst[off:], crc32.Checksum(dst[:off], crc32Table))
	off += walRecordMetaLen
	clear(dst[off:rec.recLen])
	return int(rec.recLen)
}

func encodeTestRecordAt(lsn LSN, data []byte) []byte {
	buf := make([]byte, encodedRecordLen(data))
	rec := newRecord(lsn, data)
	encodeRecord(buf, &rec)
	return buf
}

// writeRecordAt appends a whole walRecord to seg bypassing the writer.
func writeRecordAt(t *testing.T, seg *walSegment, data []byte) LSN {
	t.Helper()
	lsn := newLSN(seg.segId, seg.cursor, seg.segMaxSize)
	buf := encodeTestRecordAt(lsn, data)
	_, err := seg.file.WriteAt(buf, int64(seg.cursor))
	require.NoError(t, err)
	seg.cursor += uint64(len(buf))
	return lsn
}

func zeroRecordBodyAndCRC(t *testing.T, w *WAL, lsn LSN, data []byte) {
	t.Helper()
	zeroes := make([]byte, encodedRecordLen(data)-walRecordHeaderLen)
	written, err := w.segments.byID[testLSNSegID(lsn)].file.WriteAt(zeroes, int64(testLSNOffset(lsn)+walRecordHeaderLen))
	require.NoError(t, err)
	require.Equal(t, len(zeroes), written)
}

// recoverWAL opens dir and recovers it, the WAL is closed at test end even when Recover fails.
func recoverWAL(t *testing.T, dir string, after LSN, policy CorruptionPolicy, fn func(Record) error, opts ...Option) (*WAL, error) {
	t.Helper()
	if len(opts) == 0 {
		opts = append(opts, WithSegmentSize(walTestSegmentSize))
	}
	w, err := New(dir, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	return w, w.Recover(after, fn, policy)
}

func recoverTestWAL(t *testing.T, dir string, after LSN, opts ...Option) (*WAL, []testRecord) {
	t.Helper()
	var got []testRecord
	w, err := recoverWAL(t, dir, after, StopOnCorruption, func(r Record) error {
		got = append(got, testRecord{r.LSN, string(r.Data)})
		return nil
	}, opts...)
	require.NoError(t, err)
	return w, got
}

func openTestWAL(t *testing.T, dir string, opts ...Option) *WAL {
	t.Helper()
	w, _ := recoverTestWAL(t, dir, 0, opts...)
	return w
}

func readAt(w *WAL, lsn LSN) ([]byte, LSN, error) {
	data, _, next, err := w.reader.read(lsn, nil)
	return data, next, err
}

func appendOne(t *testing.T, w *WAL, data string) LSN {
	t.Helper()
	lsn, err := w.Append(parts(data), DurabilitySynced)
	require.NoError(t, err)
	return lsn
}

func appendTwo(t *testing.T, w *WAL, first, second []byte) (LSN, LSN) {
	t.Helper()
	return appendOne(t, w, string(first)), appendOne(t, w, string(second))
}

func appendThree(t *testing.T, w *WAL, a, b, c string) (LSN, LSN, LSN) {
	t.Helper()
	return appendOne(t, w, a), appendOne(t, w, b), appendOne(t, w, c)
}

// waitPrepared waits for the preparer to fill the next segment slot.
func waitPrepared(t *testing.T, w *WAL) *walSegment {
	t.Helper()
	var next *walSegment
	require.Eventually(t, func() bool {
		w.segments.mu.Lock()
		defer w.segments.mu.Unlock()
		next = w.segments.next
		return next != nil
	}, 5*time.Second, time.Millisecond)
	return next
}

func segmentIDs(w *WAL) []segID {
	ids := make([]segID, len(w.segments.segments))
	for i, seg := range w.segments.segments {
		ids[i] = seg.segId
	}
	return ids
}

func segmentPaths(w *WAL) []string {
	paths := make([]string, len(w.segments.segments))
	for i, seg := range w.segments.segments {
		paths[i] = seg.path
	}
	return paths
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
