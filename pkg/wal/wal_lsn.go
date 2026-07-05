package wal

type LSN uint64
type segID uint32

func newLSN(sid segID, offset int64) LSN {
	if offset < 0 || offset >= walSegmentMaxSize {
		panic("wal: offset exceeds segment size")
	}

	return LSN(uint64(sid)*walSegmentMaxSize + uint64(offset))
}

func lsnOffset(lsn LSN) int64 {
	return int64(uint64(lsn) % walSegmentMaxSize)
}

func lsnSegID(lsn LSN) segID {
	return segID(uint64(lsn) / walSegmentMaxSize)
}
