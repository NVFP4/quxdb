package vset

import (
	"fmt"
	"slices"
	"sync"

	"github.com/yashgorana/quxdb/pkg/sst"
)

const maxLevels = 8

type TableMeta = sst.Metadata
type Changes catalogRecord

type VersionSet struct {
	catalog *catalog
	latest  *Version
	mu      sync.RWMutex
}

func New(dir string) (*VersionSet, error) {
	catalog, err := openCatalog(dir)
	if err != nil {
		return nil, err
	}

	ver := &Version{
		levels: newLevelMap(maxLevels),
	}

	err = catalog.replay(func(rec catalogRecord) error {
		return applyRecord(ver, rec)
	})
	if err != nil {
		catalog.close()
		return nil, err
	}

	return &VersionSet{
		catalog: catalog,
		latest:  ver,
	}, nil
}

func (vs *VersionSet) CurrentVersion() *Version {
	vs.mu.RLock()
	defer vs.mu.RUnlock()
	return vs.latest
}

func (vs *VersionSet) Apply(changes []Changes) error {
	if len(changes) == 0 {
		return nil
	}

	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.latest == nil {
		return ErrClosed
	}

	newRecords := make([]catalogRecord, 0, len(changes))
	for _, ch := range changes {
		switch ch.Op {
		case OpAdd, OpDelete:
			newRecords = append(newRecords, catalogRecord(ch))
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
		if err := applyRecord(next, rec); err != nil {
			return nil, err
		}
	}
	return next, nil
}

func applyRecord(ver *Version, rec catalogRecord) error {
	switch rec.Op {
	case OpAdd:
		deleteTable(ver.levels, rec.Meta.ID)
		if rec.Meta.Level >= maxLevels {
			return fmt.Errorf("level out of bounds '%d'", rec.Meta.Level)
		}
		lvl := int(rec.Meta.Level)
		ver.levels[lvl] = append(ver.levels[lvl], rec.Meta)
	case OpDelete:
		deleteTable(ver.levels, rec.Meta.ID)
	default:
		return ErrRecordCorrupt
	}
	return nil
}

func deleteTable(levels LevelMap, id uint64) {
	for lvl, metas := range levels {
		metas = slices.DeleteFunc(metas, func(meta TableMeta) bool {
			return meta.ID == id
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
