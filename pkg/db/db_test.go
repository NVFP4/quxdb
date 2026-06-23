package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuxDB(t *testing.T) {
	db, err := New(t.TempDir())
	assert.NoError(t, err)

	err = db.Start(t.Context())
	assert.NoError(t, err)

	key := []byte("test")

	err = db.Set(key, []byte("val"))
	assert.NoError(t, err)

	err = db.Set(key, []byte("val2"))
	assert.NoError(t, err)

	err = db.Set(key, []byte("val3"))
	assert.NoError(t, err)

	val, found := db.Get(key)
	assert.True(t, found)
	assert.Equal(t, []byte("val3"), val)

	err = db.Delete(key)
	assert.NoError(t, err)

	val, found = db.Get(key)
	assert.False(t, found)
	assert.Nil(t, val)
}
