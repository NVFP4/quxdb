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

// Checkpoint marks the wal position covered by tables.
type Checkpoint struct {
	LastSeq uint64 `json:"seq"`
	LastLSN uint64 `json:"lsn"`
	// set only by catalog snapshots
	LastTableID uint64 `json:"tid,omitempty"`
}

// Change is one catalog edit.
type Change struct {
	Op         Op
	Table      *sst.Metadata
	Checkpoint Checkpoint
}

// VersionSet tracks table versions in an append-only catalog.
type VersionSet struct {
	catalog     *catalog
	latest      *Version
	lastTableID atomic.Uint64
	mu          sync.RWMutex
}

// New replays the catalog in dir.
func New(dir string) (*VersionSet, error) {
	catalog, err := openCatalog(dir)
	if err != nil {
		return nil, err
	}

	ver := &Version{
		levels: newLevelMap(MaxLevels),
	}

	// include deleted tables so ids are never reused
	var lastTableID uint64
	err = catalog.replay(func(rec catalogRecord) error {
		if err := applyRecordLocked(ver, rec); err != nil {
			return err
		}
		switch rec.Op {
		case OpAdd:
			lastTableID = max(lastTableID, rec.Table.ID)
		case OpCheckpoint:
			lastTableID = max(lastTableID, rec.Checkpoint.LastTableID)
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
	vs.snapshotLocked()
	return vs, nil
}

// NextTableID returns an unused table id.
func (vs *VersionSet) NextTableID() uint64 {
	return vs.lastTableID.Add(1)
}

// keeps the counter at or above every added id.
func (vs *VersionSet) raiseTableID(id uint64) {
	for last := vs.lastTableID.Load(); last < id; last = vs.lastTableID.Load() {
		if vs.lastTableID.CompareAndSwap(last, id) {
			return
		}
	}
}

// CurrentVersion returns the latest version.
func (vs *VersionSet) CurrentVersion() *Version {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.latest
}

// Apply persists changes and publishes the next version.
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
	for _, rec := range newRecords {
		if rec.Op == OpAdd {
			vs.raiseTableID(rec.Table.ID)
		}
	}
	vs.snapshotLocked()
	return nil
}

// snapshots the catalog once dead records reach the threshold.
func (vs *VersionSet) snapshotLocked() {
	live := vs.latest.Len() + 1
	if vs.catalog.records-live < snapshotThreshold {
		return
	}

	records := make([]catalogRecord, 0, live)
	for level := range MaxLevels {
		// keep l0 order, it encodes recency
		for _, table := range vs.latest.Level(level) {
			records = append(records, catalogRecord{Op: OpAdd, Table: table})
		}
	}
	checkpoint := vs.latest.Checkpoint()
	checkpoint.LastTableID = vs.lastTableID.Load()
	records = append(records, catalogRecord{Op: OpCheckpoint, Checkpoint: &checkpoint})

	if err := vs.catalog.rewrite(records); err != nil {
		fmt.Printf("vset: catalog snapshot error %v\n", err)
	}
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
		ver.checkpoint = Checkpoint{LastSeq: checkpoint.LastSeq, LastLSN: checkpoint.LastLSN}
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

// Close closes the catalog.
func (vs *VersionSet) Close() error {
	vs.mu.Lock()
	vs.latest = nil
	vs.mu.Unlock()
	return vs.catalog.close()
}
