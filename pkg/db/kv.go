package db

import (
	"encoding/binary"
	"time"
)

const (
	opSet = 1 << iota
	opDelete
)

type kvItem struct {
	key []byte
	val []byte
	op  uint8
}

func (r *kvItem) EncodedLen() int {
	// 1 for op, 4 for lenKey, 4 for lenVal
	return 1 + 8 + len(r.key) + len(r.val)
}

func (r *kvItem) Encode(buf []byte) int {
	bufOff := 0
	lenKey := len(r.key)
	lenVal := len(r.val)

	buf[bufOff] = byte(r.op)
	bufOff += 1

	binary.LittleEndian.PutUint32(buf[bufOff:], uint32(lenKey))
	bufOff += 4

	copy(buf[bufOff:], r.key)
	bufOff += lenKey

	binary.LittleEndian.PutUint32(buf[bufOff:], uint32(lenVal))
	bufOff += 4

	copy(buf[bufOff:], r.val)
	bufOff += lenVal

	return bufOff
}

func (r *kvItem) Decode(buf []byte) int {
	bufOff := 0

	r.op = uint8(buf[bufOff])
	bufOff += 1

	lenKey := binary.LittleEndian.Uint32(buf[bufOff:])
	bufOff += 4

	r.key = buf[bufOff : bufOff+int(lenKey)]
	bufOff += int(lenKey)

	lenVal := binary.LittleEndian.Uint32(buf[bufOff:])
	bufOff += 4

	r.val = buf[bufOff : bufOff+int(lenVal)]
	bufOff += int(lenVal)

	return bufOff
}

type writeReq struct {
	kvItem

	res  writeResult
	done chan struct{}

	enqueuedAt time.Time
}

type writeResult struct {
	lsn uint64
	err error

	queueWait time.Duration
	commitDur time.Duration
	totalDur  time.Duration
}
