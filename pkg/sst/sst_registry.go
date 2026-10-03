package sst

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

type tableHandle struct {
	meta *Metadata
	sst  *SST
	refs int
}

// Registry manages the sstable lifecycle: open, cache, and delete once retired and unpinned.
type Registry struct {
	cache   map[uint64]*tableHandle
	retired map[uint64]*tableHandle
	cleanup sync.WaitGroup
	mu      sync.Mutex
}

func NewRegistry() *Registry {
	return &Registry{
		cache:   make(map[uint64]*tableHandle),
		retired: make(map[uint64]*tableHandle),
	}
}

// on error, callers must Retire the whole batch.
func (r *Registry) Open(tables []*Metadata) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, meta := range tables {
		table, err := openSST(meta)
		if err != nil {
			return err
		}
		r.cache[meta.ID] = &tableHandle{meta: meta, sst: table}
	}
	return nil
}

// tables must already be open.
func (r *Registry) View(tables []*Metadata) *View {
	r.mu.Lock()
	defer r.mu.Unlock()

	view := &View{registry: r, handles: make(map[uint64]*tableHandle, len(tables))}
	view.refs.Store(1)
	for _, meta := range tables {
		h := r.cache[meta.ID]
		h.refs++
		view.handles[meta.ID] = h
	}
	return view
}

// unpinned tables are deleted synchronously; pinned ones after their last view release.
func (r *Registry) Retire(tables []*Metadata) {
	var obsolete []*tableHandle
	r.mu.Lock()
	for _, meta := range tables {
		h, cached := r.cache[meta.ID]
		if cached {
			delete(r.cache, meta.ID)
		} else {
			h = &tableHandle{meta: meta}
		}
		r.retired[meta.ID] = h
		if h.refs == 0 {
			obsolete = append(obsolete, h)
		}
	}
	r.mu.Unlock()

	r.removeTables(obsolete)
}

// failed removals are logged and stay retired so Close retries them.
func (r *Registry) removeTables(handles []*tableHandle) {
	for _, h := range handles {
		if err := removeTable(h); err != nil {
			fmt.Printf("sst: remove table %d error %v\n", h.meta.ID, err)
			continue
		}
		r.mu.Lock()
		delete(r.retired, h.meta.ID)
		r.mu.Unlock()
	}
}

func removeTable(h *tableHandle) error {
	var closeErr error
	if h.sst != nil {
		closeErr = h.sst.close()
		if closeErr == nil {
			h.sst = nil
		}
	}
	return errors.Join(closeErr, os.RemoveAll(h.meta.Path))
}

// must run after every view is released.
func (r *Registry) Close() error {
	r.cleanup.Wait()

	r.mu.Lock()
	defer r.mu.Unlock()

	var errs []error
	for _, h := range r.cache {
		errs = append(errs, h.sst.close())
	}
	for _, h := range r.retired {
		errs = append(errs, removeTable(h))
	}
	clear(r.cache)
	clear(r.retired)
	return errors.Join(errs...)
}
