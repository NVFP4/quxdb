package vset

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/yashgorana/quxdb/pkg/sst"
)

const MaxLevels = 5

type Checkpoint struct {
	LastSeq uint64 `json:"seq"`
	LastLSN uint64 `json:"lsn"`
}

type Change struct {
	Op         Op
	Table      *sst.Metadata
	Checkpoint Checkpoint
}

type VersionSet struct {
	catalog     *catalog
	latest      *Version
	lastTableID atomic.Uint64
	mu          sync.RWMutex
}

func New(dir string) (*VersionSet, error) {
	catalog, err := openCatalog(dir)
	if err != nil {
		return nil, err
	}

	ver := &Version{
		levels: newLevelMap(MaxLevels),
	}

	// deleted tables count too, so their ids are never reused.
	var lastTableID uint64
	err = catalog.replay(func(rec catalogRecord) error {
		if err := applyRecordLocked(ver, rec); err != nil {
			return err
		}
		if rec.Op == OpAdd {
			lastTableID = max(lastTableID, rec.Table.ID)
		}
		return nil
	})
	if err != nil {
		catalog.close()
		return nil, err
	}

	vs := &VersionSet{
		catalog: catalog,
		latest:  ver,
	}
	vs.lastTableID.Store(lastTableID)
	return vs, nil
}

func (vs *VersionSet) NextTableID() uint64 {
	return vs.lastTableID.Add(1)
}

func (vs *VersionSet) CurrentVersion() *Version {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.latest
}

func (vs *VersionSet) Apply(changes []Change) error {
	if len(changes) == 0 {
		return nil
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.latest == nil {
		return ErrClosed
	}

	newRecords := make([]catalogRecord, 0, len(changes))
	for i, ch := range changes {
		switch ch.Op {
		case OpAdd, OpDelete:
			newRecords = append(newRecords, catalogRecord{
				Op:    ch.Op,
				Table: ch.Table,
			})
		case OpCheckpoint:
			if i != len(changes)-1 {
				return ErrRecordCorrupt
			}
			checkpoint := ch.Checkpoint
			newRecords = append(newRecords, catalogRecord{
				Op:         ch.Op,
				Checkpoint: &checkpoint,
			})
		default:
			return ErrRecordCorrupt
		}
	}

	next, err := buildNextVersion(vs.latest, newRecords)
	if err != nil {
		return err
	}

	if err := vs.catalog.appendAll(newRecords); err != nil {
		return err
	}

	vs.latest = next
	return nil
}

func buildNextVersion(current *Version, newRecords []catalogRecord) (*Version, error) {
	next := current.Clone()
	for _, rec := range newRecords {
		if err := applyRecordLocked(next, rec); err != nil {
			return nil, err
		}
	}
	return next, nil
}

func applyRecordLocked(ver *Version, rec catalogRecord) error {
	switch rec.Op {
	case OpAdd:
		if rec.Table == nil || rec.Checkpoint != nil {
			return ErrRecordCorrupt
		}
		deleteTableLocked(ver.levels, rec.Table.ID)
		if rec.Table.Level >= MaxLevels {
			return fmt.Errorf("level out of bounds '%d'", rec.Table.Level)
		}
		if err := addTableLocked(ver.levels, rec.Table); err != nil {
			return err
		}
	case OpDelete:
		if rec.Table == nil || rec.Checkpoint != nil {
			return ErrRecordCorrupt
		}
		deleteTableLocked(ver.levels, rec.Table.ID)
	case OpCheckpoint:
		if rec.Table != nil || rec.Checkpoint == nil {
			return ErrRecordCorrupt
		}
		checkpoint := *rec.Checkpoint
		if (checkpoint.LastSeq == 0) != (checkpoint.LastLSN == 0) ||
			checkpoint.LastSeq < ver.checkpoint.LastSeq ||
			(checkpoint.LastSeq == ver.checkpoint.LastSeq && checkpoint.LastLSN != ver.checkpoint.LastLSN) {
			return ErrRecordCorrupt
		}
		ver.checkpoint = checkpoint
	default:
		return ErrRecordCorrupt
	}
	return nil
}

func addTableLocked(levels LevelMap, t *sst.Metadata) error {
	lvl := int(t.Level)

	if lvl == 0 {
		levels[lvl] = append(levels[lvl], t)
		return nil
	}

	tables := levels[lvl]
	idx, _ := slices.BinarySearchFunc(tables, t.MinKey, func(t *sst.Metadata, key []byte) int {
		return bytes.Compare(t.MinKey, key)
	})

	levels[lvl] = slices.Insert(tables, idx, t)
	return nil
}

func deleteTableLocked(levels LevelMap, id uint64) {
	for lvl, metas := range levels {
		metas = slices.DeleteFunc(metas, func(t *sst.Metadata) bool {
			return t.ID == id
		})
		if len(metas) == 0 {
			delete(levels, lvl)
			continue
		}
		levels[lvl] = metas
	}
}

func (vs *VersionSet) Close() error {
	vs.mu.Lock()
	vs.latest = nil
	vs.mu.Unlock()
	return vs.catalog.close()
}
