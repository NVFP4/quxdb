package db

import (
	"iter"

	"github.com/yashgorana/quxdb/pkg/sst"
	"github.com/yashgorana/quxdb/pkg/vset"
)

// lsmView is immutable, rollover views share its tables and ref
type lsmView struct {
	version   *vset.Version
	tables    *sst.View
	memtables []*quxMemtable // newest first, [0] is the active memtable
}

func (v *lsmView) release() {
	v.tables.Release()
}

func (v *lsmView) tableCandidates(key []byte) iter.Seq[*sst.SST] {
	return func(yield func(*sst.SST) bool) {
		for _, table := range v.version.PointLookupCandidates(key) {
			if !yield(v.tables.Table(table.ID)) {
				return
			}
		}
	}
}

func (v *lsmView) tableRangeCandidates(start, end []byte) []*sst.SST {
	candidates := v.version.RangeLookupCandidates(start, end)
	tables := make([]*sst.SST, len(candidates))
	for i, table := range candidates {
		tables[i] = v.tables.Table(table.ID)
	}
	return tables
}
