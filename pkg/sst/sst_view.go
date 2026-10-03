package sst

import "sync/atomic"

// View is a read snapshot of sstables; its tables are not deleted until its last Release.
type View struct {
	refs     atomic.Int64
	registry *Registry
	handles  map[uint64]*tableHandle
}

// valid until the view's last Release.
func (v *View) Table(id uint64) *SST {
	return v.handles[id].sst
}

// fails after the last Release; callers reload the current view.
func (v *View) TryRetain() bool {
	for refs := v.refs.Load(); refs != 0; refs = v.refs.Load() {
		if v.refs.CompareAndSwap(refs, refs+1) {
			return true
		}
	}
	return false
}

// retired tables are deleted in the background to keep removal off the read path.
func (v *View) Release() {
	if v.refs.Add(-1) != 0 {
		return
	}

	r := v.registry
	var obsolete []*tableHandle
	r.mu.Lock()
	for id, h := range v.handles {
		h.refs--
		if _, retired := r.retired[id]; retired && h.refs == 0 {
			obsolete = append(obsolete, h)
		}
	}
	r.mu.Unlock()

	if len(obsolete) > 0 {
		r.cleanup.Go(func() { r.removeTables(obsolete) })
	}
}
