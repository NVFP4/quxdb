package db

import (
	"bytes"
	"encoding/binary"
)

type (
	quxKey      []byte
	quxOp       uint8
	quxSeq      = uint64
	quxKeyMatch int
)

const (
	quxOpSet quxOp = 1 << iota
	quxOpDelete
)

const (
	keyMatchDeleted quxKeyMatch = -1 // exact match, but tombstoned
	keyMatchExact   quxKeyMatch = 0  // exact match
	keyMatchNone    quxKeyMatch = 1  // different key
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

func (k quxKey) Compare(userKey []byte) int {
	return bytes.Compare(k.UserKey(), userKey)
}

func (k quxKey) Match(userKey []byte) quxKeyMatch {
	if k.Compare(userKey) != 0 {
		return keyMatchNone
	}
	if k.Op() == quxOpDelete {
		return keyMatchDeleted
	}
	return keyMatchExact
}

// seek start (inclusive)
// returns the newest version of `userKey` that is `<=seq`
func newSeekStart(userKey []byte, seq quxSeq) quxKey {
	return newQuxKey(userKey, seq, quxOp(0xFF))
}

// seek end (inclusive)
// returns last possible `quxKey` for this `userKey`
func newSeekEndInclusive(userKey []byte) quxKey {
	return newQuxKey(userKey, quxSeq(0), quxOp(0))
}

func isValidQuxKey(key quxKey) bool {
	if len(key) < quxKeyAlign+quxKeyTrailerLen || (len(key)-quxKeyTrailerLen)%quxKeyAlign != 0 {
		return false
	}
	marker := len(key) - quxKeyTrailerLen
	return key[marker] < quxKeyAlign && (key.Op() == quxOpSet || key.Op() == quxOpDelete)
}
