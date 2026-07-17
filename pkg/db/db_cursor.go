package db

import (
	"bytes"

	"github.com/yashgorana/quxdb/pkg/core"
)

// mvccCursor emits the newest visible version per user key from one sorted source.
type mvccCursor struct {
	cur     core.Cursor
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

func (c *mvccCursor) Err() error {
	return c.cur.Err()
}
