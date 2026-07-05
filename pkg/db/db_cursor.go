package db

import (
	"bytes"

	"github.com/yashgorana/quxdb/pkg/memtable"
)

// mvccCursor emits at most one visible version per user key from a single memtable.
type mvccCursor struct {
	cur     memtable.Cursor
	readSeq quxSeq

	skipUserKey []byte
}

func (c *mvccCursor) Next() (key, value []byte, ok bool) {
	for {
		key, value, ok = c.cur.Next()
		if !ok {
			return nil, nil, false
		}

		qkey := quxKey(key)
		userKey := qkey.UserKey()

		if c.skipUserKey != nil {
			if bytes.Equal(userKey, c.skipUserKey) {
				continue
			}
			c.skipUserKey = nil
		}

		if qkey.Seq() <= c.readSeq {
			c.skipUserKey = userKey
			return key, value, true
		}
	}
}
