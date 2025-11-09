package db

import (
	"context"
	"fmt"
	"iter"

	"github.com/yashgorana/quxdb/pkg/memtable"
)

const (
	kMemTableType = memtable.Map
)

type QuxDB struct {
	dataDir string

	mt memtable.MemTable // active mem table
}

func New(dataDir string) *QuxDB {
	db := &QuxDB{
		dataDir: dataDir,
		mt:      memtable.New(kMemTableType),
	}
	return db
}

func (db *QuxDB) Start(ctx context.Context) error {
	fmt.Printf("db started with memtable type=%s\n", kMemTableType)
	return nil
}

func (db *QuxDB) Set(key []byte, value []byte) error {
	return db.mt.Set(key, value)
}

func (db *QuxDB) Get(key []byte) ([]byte, bool) {
	return db.mt.Get(key)
}

func (db *QuxDB) Delete(key []byte) error {
	return db.mt.Delete(key)
}

func (db *QuxDB) All() iter.Seq2[[]byte, []byte] {
	return db.mt.All()
}

func (db *QuxDB) Range(start, end []byte) iter.Seq2[[]byte, []byte] {
	return db.mt.Range(start, end)
}
