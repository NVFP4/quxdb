package wal

// LSN is a Record's position in the log.
type LSN uint64
type segID uint32

func newLSN(sid segID, offset uint64, segSize uint64) LSN {
	if offset >= segSize {
		panic("wal: offset exceeds segment size")
	}

	return LSN(uint64(sid)*segSize + offset)
}

func lsnOffset(lsn LSN, segSize uint64) uint64 {
	return uint64(lsn) % segSize
}

func lsnSegID(lsn LSN, segSize uint64) segID {
	return segID(uint64(lsn) / segSize)
}
