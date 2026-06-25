package db

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuxKey(t *testing.T) {
	k1 := newQuxKey([]byte("abcdefgh"), quxSeq(11), quxOpSet)
	k1User, k1Seq, k1Op := k1.Decode()
	assert.Equal(t, []byte("abcdefgh"), k1User)
	assert.Equal(t, quxSeq(11), k1Seq)
	assert.Equal(t, quxOpSet, k1Op)

	k2 := newQuxKey([]byte("abcdefgh"), quxSeq(14), quxOpSet)
	k2User, k2Seq, k2Op := k2.Decode()
	assert.Equal(t, []byte("abcdefgh"), k2User)
	assert.Equal(t, quxSeq(14), k2Seq)
	assert.Equal(t, quxOpSet, k2Op)

	assert.True(t, bytes.Compare(k1, k2) > 0, fmt.Sprintf("%s(%d) should sort after %s(%d)", k1User, k1Seq, k2User, k2Seq))

	k1 = newQuxKey([]byte("a\x00"), 31234, quxOpSet)
	k1User, k1Seq, k1Op = k1.Decode()
	assert.Equal(t, []byte("a\x00"), k1User)
	assert.Equal(t, quxSeq(31234), k1Seq)
	assert.Equal(t, quxOpSet, k1Op)

	k2 = newQuxKey([]byte("a"), 31234, quxOpSet)
	assert.True(t, bytes.Compare(k1, k2) > 0, fmt.Sprintf("'%s' should sort before '%s'", k1, k2))
}

func TestQuxKeySetSeq(t *testing.T) {
	key := newQuxKey([]byte("key"), 0, quxOpDelete)

	key.SetSeq(42)

	userKey, seq, op := key.Decode()
	assert.Equal(t, []byte("key"), userKey)
	assert.Equal(t, quxSeq(42), seq)
	assert.Equal(t, quxOpDelete, op)
}
