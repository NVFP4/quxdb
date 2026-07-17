package vset

import (
	"bytes"
	"slices"
)

type LevelMap map[int][]Table

func newLevelMap(depth int) LevelMap {
	return make(LevelMap, depth)
}

type Version struct {
	levels     LevelMap
	checkpoint Checkpoint
}

func (v *Version) Len() int {
	count := 0
	for _, tab := range v.levels {
		count += len(tab)
	}
	return count
}

func (v *Version) Checkpoint() Checkpoint {
	return v.checkpoint
}

// caller must clone to mutate
func (v *Version) Level(n int) []Table {
	if v == nil {
		return nil
	}
	return v.levels[n]
}

// caller must clone to mutate
func (v *Version) Levels() LevelMap {
	if v == nil {
		return nil
	}
	return v.levels
}

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

// returns all ssts, sorted by L0, L1, ..., Ln and the order in which they were added
func (v *Version) All() []Table {
	var meta []Table
	for _, tables := range v.levels {
		for i := len(tables) - 1; i >= 0; i-- {
			meta = append(meta, tables[i])
		}
	}
	return meta
}

// point lookup candidate ssts
func (v *Version) PointLookupCandidates(key []byte) []Table {
	var meta []Table
	if len(key) == 0 {
		return meta
	}

	// L0 overlap, so sort latest first
	tables := v.levels[0]
	for i := len(tables) - 1; i >= 0; i-- {
		tab := tables[i]
		if bytes.Compare(key, tab.MinKey) >= 0 && bytes.Compare(key, tab.MaxKey) <= 0 {
			meta = append(meta, tab)
		}
	}

	// compaction strat agnostic selection
	for lvl := 1; lvl < MaxLevels; lvl++ {
		for _, tab := range v.levels[lvl] {
			if bytes.Compare(key, tab.MinKey) >= 0 && bytes.Compare(key, tab.MaxKey) <= 0 {
				meta = append(meta, tab)
			}
		}
	}
	return meta
}

// range lookup candidate ssts, `start` must be less than `end`
func (v *Version) RangeLookupCandidates(start, end []byte) []Table {
	var meta []Table

	if len(start) > 0 && len(end) > 0 && bytes.Compare(start, end) > 0 {
		return meta
	}

	// L0 tables may overlap, so test every run newest-first.
	tables := v.levels[0]
	for i := len(tables) - 1; i >= 0; i-- {
		tab := tables[i]
		hasStart := true
		hasEnd := true
		if len(start) > 0 {
			hasStart = bytes.Compare(start, tab.MaxKey) <= 0
		}
		if len(end) > 0 {
			hasEnd = bytes.Compare(end, tab.MinKey) >= 0
		}
		if hasStart && hasEnd {
			meta = append(meta, tab)
		}
	}

	// L1+ tables are ordered by MinKey, but may overlap under hybrid
	// compaction. Test every table rather than assuming a contiguous range.
	for lvl := 1; lvl < MaxLevels; lvl++ {
		for _, tab := range v.levels[lvl] {
			hasStart := true
			hasEnd := true
			if len(start) > 0 {
				hasStart = bytes.Compare(start, tab.MaxKey) <= 0
			}
			if len(end) > 0 {
				hasEnd = bytes.Compare(end, tab.MinKey) >= 0
			}
			if hasStart && hasEnd {
				meta = append(meta, tab)
			}
		}
	}

	return meta
}
