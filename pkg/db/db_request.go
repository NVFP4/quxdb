package db

import "time"

type writeReq struct {
	qkey       quxKey
	val        []byte
	res        writeResult
	enqueuedAt time.Time
	done       chan struct{}
}

func newWriteReq() *writeReq {
	return &writeReq{
		done: make(chan struct{}, 1),
	}
}

type writeResult struct {
	lsn uint64
	err error

	queueWait time.Duration
	commitDur time.Duration
	totalDur  time.Duration
}
