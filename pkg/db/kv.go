package db

import (
	"bytes"
	"encoding/binary"
	"time"
)

type dbOP uint8

type quxKey []byte

const (
	opSet dbOP = 1 << iota
	opDelete
	opMax dbOP = 0xff
)

const quxKeyTrailerLen = 8 + 1

type kvPair struct {
	qkey quxKey
	val  []byte
}

func (r *kvPair) EncodedLen() int {
	// 4 for lenKey, 4 for lenVal
	return 8 + len(r.qkey) + len(r.val)
}

func (r *kvPair) Encode(buf []byte) int {
	bufOff := 0
	lenKey := len(r.qkey)
	lenVal := len(r.val)

	binary.LittleEndian.PutUint32(buf[bufOff:], uint32(lenKey))
	bufOff += 4

	copy(buf[bufOff:], r.qkey)
	bufOff += lenKey

	binary.LittleEndian.PutUint32(buf[bufOff:], uint32(lenVal))
	bufOff += 4

	copy(buf[bufOff:], r.val)
	bufOff += lenVal

	return bufOff
}

func (r *kvPair) Decode(buf []byte) int {
	bufOff := 0

	lenKey := binary.LittleEndian.Uint32(buf[bufOff:])
	bufOff += 4

	r.qkey = buf[bufOff : bufOff+int(lenKey)]
	bufOff += int(lenKey)

	lenVal := binary.LittleEndian.Uint32(buf[bufOff:])
	bufOff += 4

	r.val = buf[bufOff : bufOff+int(lenVal)]
	bufOff += int(lenVal)

	return bufOff
}

func newQuxKey(userKey []byte, seq uint64, op dbOP) quxKey {
	n := len(userKey)
	internalKey := make([]byte, n+quxKeyTrailerLen)
	copy(internalKey, userKey)
	binary.LittleEndian.PutUint64(internalKey[n:], seq)
	internalKey[n+8] = byte(op)
	return internalKey
}

func userKeyFromQuxKey(qkey quxKey) []byte {
	return qkey[:len(qkey)-quxKeyTrailerLen]
}

func seqFromQuxKey(qkey quxKey) uint64 {
	keylen := len(qkey)
	return binary.LittleEndian.Uint64(qkey[keylen-quxKeyTrailerLen : keylen-1])
}

func opFromQuxKey(qkey quxKey) dbOP {
	return dbOP(qkey[len(qkey)-1])
}

func decodeQuxKey(qkey quxKey) ([]byte, uint64, dbOP) {
	n := len(qkey) - quxKeyTrailerLen
	userKey := qkey[:n]
	seq := binary.LittleEndian.Uint64(qkey[n:])
	op := dbOP(qkey[n+8])
	return userKey, seq, op
}

func compareInternalKey(a, b []byte) int {
	aUserLen := len(a) - quxKeyTrailerLen
	bUserLen := len(b) - quxKeyTrailerLen

	if c := bytes.Compare(a[:aUserLen], b[:bUserLen]); c != 0 {
		return c
	}

	aSeq := binary.LittleEndian.Uint64(a[aUserLen : aUserLen+8])
	bSeq := binary.LittleEndian.Uint64(b[bUserLen : bUserLen+8])
	if aSeq > bSeq {
		return -1
	}
	if aSeq < bSeq {
		return 1
	}

	aOp := dbOP(a[aUserLen+8])
	bOp := dbOP(b[bUserLen+8])
	if aOp > bOp {
		return -1
	}
	if aOp < bOp {
		return 1
	}
	return 0
}

type writeReq struct {
	kvPair

	res  writeResult
	done chan struct{}

	enqueuedAt time.Time
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
