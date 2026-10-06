package vset

import (
	"bytes"
	"iter"
	"slices"

	"github.com/yashgorana/quxdb/pkg/sst"
)

// LevelMap maps a level to its tables.
type LevelMap map[int][]*sst.Metadata

func newLevelMap(depth int) LevelMap {
	return make(LevelMap, depth)
}

// Version is an immutable set of tables per level.
type Version struct {
	levels     LevelMap
	checkpoint Checkpoint
}

// Len returns the table count.
func (v *Version) Len() int {
	count := 0
	for _, tab := range v.levels {
		count += len(tab)
	}
	return count
}

// Checkpoint returns the version's checkpoint.
func (v *Version) Checkpoint() Checkpoint {
	return v.checkpoint
}

// Level returns tables at level n, clone before mutating.
func (v *Version) Level(n int) []*sst.Metadata {
	if v == nil {
		return nil
	}
	return v.levels[n]
}

// Levels returns all levels, clone before mutating.
func (v *Version) Levels() LevelMap {
	if v == nil {
		return nil
	}
	return v.levels
}

// Clone returns a mutable copy.
func (v *Version) Clone() *Version {
	clone := &Version{
		levels:     newLevelMap(len(v.levels)),
		checkpoint: v.checkpoint,
	}
	for lvl, metas := range v.levels {
		clone.levels[lvl] = slices.Clone(metas)
	}
	return clone
}

// All returns every table in unspecified level order, l0 newest first.
func (v *Version) All() []*sst.Metadata {
	var meta []*sst.Metadata
	for _, tables := range v.levels {
		for i := len(tables) - 1; i >= 0; i-- {
			meta = append(meta, tables[i])
		}
	}
	return meta
}

// PointLookup yields tables that may hold key, l0 newest first.
func (v *Version) PointLookup(key []byte) iter.Seq[*sst.Metadata] {
	return func(yield func(*sst.Metadata) bool) {
		if len(key) == 0 {
			return
		}

		// L0 overlap, so sort latest first
		tables := v.levels[0]
		for i := len(tables) - 1; i >= 0; i-- {
			tab := tables[i]
			if bytes.Compare(key, tab.MinKey) >= 0 && bytes.Compare(key, tab.MaxKey) <= 0 && !yield(tab) {
				return
			}
		}

		// scan every table instead of binary search, support both tiered/hybrid compaction
		for lvl := 1; lvl < MaxLevels; lvl++ {
			for _, tab := range v.levels[lvl] {
				if bytes.Compare(key, tab.MinKey) >= 0 && bytes.Compare(key, tab.MaxKey) <= 0 && !yield(tab) {
					return
				}
			}
		}
	}
}

// RangeLookup yields tables overlapping [start, end), l0 newest first, empty bounds are open.
func (v *Version) RangeLookup(start, end []byte) iter.Seq[*sst.Metadata] {
	return func(yield func(*sst.Metadata) bool) {
		tables := v.levels[0]
		for i := len(tables) - 1; i >= 0; i-- {
			if overlaps(tables[i], start, end) && !yield(tables[i]) {
				return
			}
		}

		for lvl := 1; lvl < MaxLevels; lvl++ {
			for _, tab := range v.levels[lvl] {
				if overlaps(tab, start, end) && !yield(tab) {
					return
				}
			}
		}
	}
}

func overlaps(tab *sst.Metadata, start, end []byte) bool {
	return (len(start) == 0 || bytes.Compare(start, tab.MaxKey) <= 0) &&
		(len(end) == 0 || bytes.Compare(end, tab.MinKey) > 0)
}
