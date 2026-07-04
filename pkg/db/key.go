package db

import (
	"encoding/binary"
)

type (
	quxKey []byte
	quxOp  uint8
	quxSeq = uint64
)

const (
	quxOpSet quxOp = 1 << iota
	quxOpDelete
)

const (
	quxKeyAlign      = 8
	quxKeyMarker     = 1
	quxKeySeqLen     = 8
	quxKeyOpLen      = 1
	quxKeyTrailerLen = quxKeyMarker + quxKeySeqLen + quxKeyOpLen
)

// byte sortable internal key - userKey ASC, seq DESC, op DESC
func newQuxKey(userKey []byte, seq quxSeq, op quxOp) quxKey {
	n := len(userKey)

	// If n%quxKeyAlign == 0, add a whole empty terminating group.
	padLen := quxKeyAlign - (n % quxKeyAlign)
	paddedKeyLen := n + padLen

	qkey := make([]byte, paddedKeyLen+quxKeyTrailerLen)
	off := 0

	// key + delimiter
	copy(qkey[0:n], userKey)
	qkey[paddedKeyLen] = byte(n % quxKeyAlign)
	off += paddedKeyLen + quxKeyMarker

	binary.BigEndian.PutUint64(qkey[off:off+quxKeySeqLen], ^seq)
	off += quxKeySeqLen

	qkey[off] = ^byte(op)

	return quxKey(qkey)
}

func (k quxKey) UserKey() []byte {
	marker := len(k) - quxKeyTrailerLen
	last := int(k[marker])
	return k[:marker-quxKeyAlign+last]
}

func (k quxKey) Seq() quxSeq {
	seqOff := len(k) - quxKeySeqLen - quxKeyOpLen
	return quxSeq(^binary.BigEndian.Uint64(k[seqOff : seqOff+quxKeySeqLen]))
}

func (k quxKey) SetSeq(seq quxSeq) {
	seqOff := len(k) - quxKeySeqLen - quxKeyOpLen
	binary.BigEndian.PutUint64(k[seqOff:seqOff+quxKeySeqLen], ^seq)
}

func (k quxKey) Op() quxOp {
	return quxOp(^k[len(k)-1])
}

func (k quxKey) Decode() ([]byte, quxSeq, quxOp) {
	return k.UserKey(), k.Seq(), k.Op()
}

// seek start (inclusive)
// returns the newest version of `userKey` that is `<=seq`
func newSeekStart(userKey []byte, seq quxSeq) quxKey {
	return newQuxKey(userKey, seq, quxOp(0xFF))
}

// seek end (inclusive)
// returns last possible `quxKey` for this `userKey`
func newSeekEnd(userKey []byte) quxKey {
	return newQuxKey(userKey, 0, quxOp(0x00))
}
