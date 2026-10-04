package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/yashgorana/quxdb/pkg/fs"
	"github.com/yashgorana/quxdb/pkg/metrics"
)

// Durability is how far a Record must get before Append returns.
type Durability uint8

const (
	// DurabilityWritten returns once the OS has the Record, a crash can still lose it.
	DurabilityWritten Durability = iota
	// DurabilitySynced returns once the Record survives a crash.
	DurabilitySynced
)

// walWriter frames and writes Records on the appending goroutine, one walRecord per pwritev.
type walWriter struct {
	segments *segmentSet

	recovered bool
	err       error // sticky, set by the first io error

	hdr     [walRecordHeaderLen]byte
	trailer [walRecordMetaLen + 7]byte // crc and padding
	iovecs  [][]byte                   // one record in write order
}

func newWalWriter(segments *segmentSet) *walWriter {
	return &walWriter{segments: segments}
}

func (w *walWriter) start() {
	w.recovered = true
}

func (w *walWriter) stop() {
	w.recovered = false
}

func (w *walWriter) append(parts [][]byte, d Durability) (LSN, error) {
	if !w.recovered {
		return 0, ErrNotRecovered
	}
	if w.err != nil {
		return 0, w.err
	}

	lsn, err := w.writeRecord(parts, d)
	clear(w.iovecs)
	if err != nil {
		w.err = fmt.Errorf("%w: %w", ErrWALFailed, err)
		return 0, w.err
	}
	return lsn, nil
}

// writeRecord writes parts as walRecords from the active segment's cursor, rolling over at segment ends.
func (w *walWriter) writeRecord(parts [][]byte, d Durability) (LSN, error) {
	left := 0
	for _, part := range parts {
		left += len(part)
	}

	var lsn LSN
	part, partOff := 0, 0
	for first := true; first || left > 0; first = false {
		seg := w.segments.active
		capacity := recordCapacity(seg.segMaxSize - seg.cursor)
		if capacity < 0 || (capacity == 0 && left > 0) {
			if err := w.segments.rollover(); err != nil {
				return 0, err
			}
			seg = w.segments.active
			capacity = recordCapacity(seg.segMaxSize - seg.cursor)
		}

		n := min(left, capacity)
		recLSN := newLSN(seg.segId, seg.cursor, seg.segMaxSize)
		if first {
			lsn = recLSN
		}
		flags := walRecFull
		switch {
		case first && n < left:
			flags = walRecPartialStart
		case !first && n < left:
			flags = walRecPartialMiddle
		case !first:
			flags = walRecPartialEnd
		}
		h := walRecordHeader{
			lsn:      uint64(recLSN),
			recLen:   uint32(encodedRecordSize(n)),
			recFlags: flags,
			dataLen:  uint32(n),
		}
		_, _ = encodeRecordHeader(w.hdr[:], &h)
		crc := crc32.Update(0, crc32Table, w.hdr[:])
		w.iovecs = append(w.iovecs[:0], w.hdr[:])

		for need := n; need > 0; {
			chunk := parts[part][partOff:min(len(parts[part]), partOff+need)]
			if len(chunk) > 0 {
				crc = crc32.Update(crc, crc32Table, chunk)
				w.iovecs = append(w.iovecs, chunk)
			}
			need -= len(chunk)
			if partOff += len(chunk); partOff == len(parts[part]) {
				part, partOff = part+1, 0
			}
		}

		trailer := w.trailer[:int(h.recLen)-walRecordHeaderLen-n]
		binary.LittleEndian.PutUint32(trailer, crc)
		clear(trailer[walRecordMetaLen:])
		w.iovecs = append(w.iovecs, trailer)

		end := seg.cursor + uint64(h.recLen)
		if recordCapacity(seg.segMaxSize-end) >= 0 {
			// overwritten by the next record
			w.iovecs = append(w.iovecs, endMarker[:])
		}
		start := time.Now()
		written, err := fs.Pwritev(seg.file, w.iovecs, int64(seg.cursor))
		metrics.WalWriteDuration.Observe(time.Since(start).Seconds())
		metrics.WalBytesWritten.Add(float64(written))
		if err != nil {
			return 0, err
		}
		seg.cursor = end
		left -= n
	}

	if d == DurabilitySynced {
		start := time.Now()
		err := w.segments.active.sync()
		metrics.WalSyncDuration.Observe(time.Since(start).Seconds())
		return lsn, err
	}
	return lsn, nil
}
