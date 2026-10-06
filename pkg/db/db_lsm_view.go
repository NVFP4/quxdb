package db

import (
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
