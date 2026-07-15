package vset

import "slices"

type LevelMap map[int][]TableMeta

func newLevelMap(depth int) LevelMap {
	return make(LevelMap, depth)
}

type Version struct {
	levels LevelMap
}

// caller must clone to mutate
func (v *Version) Level(n int) []TableMeta {
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
		levels: newLevelMap(len(v.levels)),
	}
	for lvl, metas := range v.levels {
		clone.levels[lvl] = slices.Clone(metas)
	}
	return clone
}
