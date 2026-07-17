package sst

import (
	"errors"
	"fmt"
	"os"
	"sync"
)

type tableCache map[uint64]*tableHandle

type tableHandle struct {
	meta Metadata
	sst  *SST
	refs int
}

// TableView pins a version's tables without opening them.
type TableView struct {
	store   *Store
	handles map[uint64]*tableHandle
}

type Store struct {
	cache   tableCache
	retired tableCache
	mu      sync.Mutex
}

func NewStore() *Store {
	return &Store{
		cache:   make(tableCache),
		retired: make(tableCache),
	}
}

// View pins active tables without opening them.
func (s *Store) View(tables []Metadata) (*TableView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	view := &TableView{
		store:   s,
		handles: make(map[uint64]*tableHandle, len(tables)),
	}
	for _, meta := range tables {
		if _, exists := view.handles[meta.ID]; exists {
			continue
		}
		if _, retired := s.retired[meta.ID]; retired {
			return nil, fmt.Errorf("%w: %d", ErrTableRetired, meta.ID)
		}
		view.handles[meta.ID] = nil
	}
	for _, meta := range tables {
		if view.handles[meta.ID] != nil {
			continue
		}
		h, exists := s.cache[meta.ID]
		if !exists {
			h = &tableHandle{meta: meta}
			s.cache[meta.ID] = h
		}
		h.refs++
		view.handles[meta.ID] = h
	}
	return view, nil
}

func (s *Store) Open(meta Metadata) (Reader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, exists := s.cache[meta.ID]
	if !exists {
		if _, retired := s.retired[meta.ID]; retired {
			return Reader{}, fmt.Errorf("%w: %d", ErrTableRetired, meta.ID)
		}
		h = &tableHandle{meta: meta}
		s.cache[meta.ID] = h
	}
	return s.openLocked(h)
}

func (v *TableView) Open(meta Metadata) (Reader, error) {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()

	h, exists := v.handles[meta.ID]
	if !exists {
		return Reader{}, fmt.Errorf("table %d is not part of the view", meta.ID)
	}
	return v.store.openLocked(h)
}

func (s *Store) openLocked(h *tableHandle) (Reader, error) {
	if h.sst == nil {
		table, err := openSST(h.meta)
		if err != nil {
			return Reader{}, err
		}
		h.sst = table
	}
	h.refs++
	return Reader{store: s, table: h}, nil
}

func (v *TableView) Release() error {
	v.store.mu.Lock()
	defer v.store.mu.Unlock()

	var errs []error
	for _, h := range v.handles {
		errs = append(errs, v.store.releaseLocked(h))
	}
	v.handles = nil
	return errors.Join(errs...)
}

func (s *Store) release(h *tableHandle) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releaseLocked(h)
}

func (s *Store) RetireTable(meta Metadata) error {
	return s.RetireTables([]Metadata{meta})
}

func (s *Store) RetireTables(tables []Metadata) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	for _, meta := range tables {
		h, exists := s.cache[meta.ID]
		if exists {
			delete(s.cache, meta.ID)
			s.retired[meta.ID] = h
		} else {
			h, exists = s.retired[meta.ID]
			if !exists {
				h = &tableHandle{meta: meta}
				s.retired[meta.ID] = h
			}
		}
		errs = append(errs, s.removeRetiredLocked(h))
	}
	return errors.Join(errs...)
}

func (s *Store) releaseLocked(h *tableHandle) error {
	h.refs--
	return s.removeRetiredLocked(h)
}

func (s *Store) removeRetiredLocked(h *tableHandle) error {
	if h.refs != 0 || s.retired[h.meta.ID] != h {
		return nil
	}

	err := removeTable(h)
	if err == nil {
		delete(s.retired, h.meta.ID)
	}
	return err
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

func (s *Store) Close() error {
	var active, retired []*tableHandle

	s.mu.Lock()
	for _, h := range s.cache {
		active = append(active, h)
	}
	for _, h := range s.retired {
		retired = append(retired, h)
	}
	clear(s.cache)
	clear(s.retired)
	s.mu.Unlock()

	var errs []error
	for _, h := range active {
		if h.sst != nil {
			errs = append(errs, h.sst.close())
		}
	}
	for _, h := range retired {
		errs = append(errs, removeTable(h))
	}
	return errors.Join(errs...)
}
