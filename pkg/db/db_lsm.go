package db

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/yashgorana/quxdb/pkg/memtable"
	"github.com/yashgorana/quxdb/pkg/sst"
	"github.com/yashgorana/quxdb/pkg/vset"
	"github.com/yashgorana/quxdb/pkg/wal"
)

// lsmState publishes coherent memtable and table-version snapshots.
type lsmState struct {
	versions *vset.VersionSet
	store    *sst.Store

	active     atomic.Pointer[quxMemtable]
	immutables []*quxMemtable

	current atomic.Pointer[quxReadSnapshot]

	mu sync.Mutex
}

func newLsmState(dataDir string, maxImmutables int) (*lsmState, error) {
	versions, err := vset.New(dataDir)
	if err != nil {
		return nil, err
	}

	version := versions.CurrentVersion()
	store := sst.NewStore()
	tables, err := store.View(version.All())
	if err != nil {
		return nil, errors.Join(err, store.Close(), versions.Close())
	}

	active := newQuxMemtable()
	tableVersion := newQuxTableVersion(version, tables)
	state := &lsmState{
		versions:   versions,
		store:      store,
		immutables: make([]*quxMemtable, 0, maxImmutables),
	}
	state.active.Store(active)
	state.current.Store(newQuxReadSnapshot(tableVersion, []*quxMemtable{active}))
	return state, nil
}

func (s *lsmState) currentVersion() *vset.Version {
	return s.versions.CurrentVersion()
}

func (s *lsmState) activeMemtable() *quxMemtable {
	return s.active.Load()
}

func (s *lsmState) flushableMemtables(retain int) []*quxMemtable {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := len(s.immutables) - retain
	if count <= 0 {
		return nil
	}
	return slices.Clone(s.immutables[:count])
}

func (s *lsmState) immutableMemtableCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.immutables)
}

func (s *lsmState) rolloverMemtable(lastSeq uint64, lastLSN wal.LSN) *quxMemtable {
	s.mu.Lock()

	previous := s.active.Load()
	previous.lastSeq = lastSeq
	previous.lastLSN = lastLSN

	next := newQuxMemtable()
	s.immutables = append(s.immutables, previous)
	s.active.Store(next)
	tableVersion := s.current.Load().tableVersion
	tableVersion.retain()
	retiredSnapshot := s.publishLocked(tableVersion, s.snapshotMemtablesLocked(next))

	s.mu.Unlock()
	_ = retiredSnapshot.release()
	return previous
}

func (s *lsmState) replaceMemtablesWithSSTs(toRemove []*quxMemtable, toAdd []*sst.Metadata) error {
	last := toRemove[len(toRemove)-1]
	changes := make([]vset.Change, 0, len(toAdd)+1)
	for _, table := range toAdd {
		changes = append(changes, vset.Change{
			Op:    vset.OpAdd,
			Table: vset.Table(*table),
		})
	}
	changes = append(changes, vset.Change{
		Op: vset.OpCheckpoint,
		Checkpoint: vset.Checkpoint{
			LastSeq: last.lastSeq,
			LastLSN: uint64(last.lastLSN),
		},
	})

	s.mu.Lock()
	tableVersion, err := s.applyVersionEditLocked(changes)
	if err != nil {
		s.mu.Unlock()
		return err
	}
	s.immutables = s.immutables[len(toRemove):]
	retiredSnapshot := s.publishLocked(tableVersion, s.snapshotMemtablesLocked(s.active.Load()))
	s.mu.Unlock()

	s.reportTableCleanupError(retiredSnapshot.release())
	return nil
}

func (s *lsmState) replaceSSTs(toRemove []vset.Table, toAdd []*sst.Metadata) error {
	s.mu.Lock()

	changes := make([]vset.Change, 0, len(toAdd)+len(toRemove))
	for _, table := range toAdd {
		changes = append(changes, vset.Change{
			Op:    vset.OpAdd,
			Table: vset.Table(*table),
		})
	}
	for _, table := range toRemove {
		changes = append(changes, vset.Change{
			Op:    vset.OpDelete,
			Table: table,
		})
	}
	tableVersion, err := s.applyVersionEditLocked(changes)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	retiredSnapshot := s.publishLocked(tableVersion, s.current.Load().memtables)
	s.mu.Unlock()

	retiredTables := make([]sst.Metadata, len(toRemove))
	for i, table := range toRemove {
		retiredTables[i] = sst.Metadata(table)
	}
	s.reportTableCleanupError(errors.Join(
		s.store.RetireTables(retiredTables),
		retiredSnapshot.release(),
	))
	return nil
}

func (s *lsmState) discardSSTs(tables []*sst.Metadata) error {
	toRetire := make([]sst.Metadata, len(tables))
	for i, table := range tables {
		toRetire[i] = *table
	}
	return s.store.RetireTables(toRetire)
}

func (s *lsmState) acquireReadSnapshot() *quxReadSnapshot {
	for {
		snapshot := s.current.Load()
		if snapshot.tryRetain() {
			return snapshot
		}
	}
}

func (s *lsmState) acquireTableVersion() *quxTableVersion {
	s.mu.Lock()
	tableVersion := s.current.Load().tableVersion
	tableVersion.retain()
	s.mu.Unlock()
	return tableVersion
}

func (s *lsmState) publishLocked(
	tableVersion *quxTableVersion,
	memtables []*quxMemtable,
) *quxReadSnapshot {
	return s.current.Swap(newQuxReadSnapshot(tableVersion, memtables))
}

func (s *lsmState) snapshotMemtablesLocked(active *quxMemtable) []*quxMemtable {
	memtables := make([]*quxMemtable, 0, len(s.immutables)+1)
	memtables = append(memtables, active)
	return append(memtables, s.immutables...)
}

func (s *lsmState) applyVersionEditLocked(changes []vset.Change) (*quxTableVersion, error) {
	current := s.currentVersion()
	deleted := make(map[uint64]struct{})
	added := make([]sst.Metadata, 0, len(changes))
	for _, change := range changes {
		switch change.Op {
		case vset.OpAdd:
			added = append(added, sst.Metadata(change.Table))
		case vset.OpDelete:
			deleted[change.Table.ID] = struct{}{}
		}
	}

	tables := make([]sst.Metadata, 0, current.Len()+len(added)-len(deleted))
	for _, table := range current.All() {
		if _, removed := deleted[table.ID]; !removed {
			tables = append(tables, sst.Metadata(table))
		}
	}
	for _, table := range added {
		tables = append(tables, table)
	}

	tableView, err := s.store.View(tables)
	if err != nil {
		if len(added) > 0 {
			err = errors.Join(err, s.store.RetireTables(added))
		}
		return nil, err
	}
	if err := s.versions.Apply(changes); err != nil {
		err = errors.Join(err, tableView.Release())
		if len(added) > 0 {
			err = errors.Join(err, s.store.RetireTables(added))
		}
		return nil, err
	}
	return newQuxTableVersion(s.currentVersion(), tableView), nil
}

func (s *lsmState) close() error {
	current := s.current.Swap(nil)
	return errors.Join(
		current.release(),
		s.store.Close(),
		s.versions.Close(),
	)
}

func (s *lsmState) reportTableCleanupError(err error) {
	if err != nil {
		fmt.Printf("db: table cleanup error %v\n", err)
	}
}

// ---- memtable ----

type quxMemtable struct {
	memtable.Memtable
	lastLSN wal.LSN
	lastSeq uint64
}

func newQuxMemtable() *quxMemtable {
	return &quxMemtable{Memtable: memtable.New(memTableType)}
}

// ---- table version ----

type quxTableVersion struct {
	// refs counts read snapshots and compaction plans that retain this version.
	refs    atomic.Int64
	version *vset.Version
	view    *sst.TableView
}

func newQuxTableVersion(version *vset.Version, view *sst.TableView) *quxTableVersion {
	tableVersion := &quxTableVersion{
		version: version,
		view:    view,
	}
	tableVersion.refs.Store(1)
	return tableVersion
}

func (v *quxTableVersion) retain() {
	v.refs.Add(1)
}

func (v *quxTableVersion) release() error {
	if v.refs.Add(-1) != 0 {
		return nil
	}
	return v.view.Release()
}

func (v *quxTableVersion) openTable(table sst.Metadata) (sst.Reader, error) {
	return v.view.Open(table)
}

// ---- read snapshot ----

type quxReadSnapshot struct {
	// refs includes the current-pointer owner and acquired readers.
	refs         atomic.Int64
	tableVersion *quxTableVersion
	memtables    []*quxMemtable
}

func newQuxReadSnapshot(
	tableVersion *quxTableVersion,
	memtables []*quxMemtable,
) *quxReadSnapshot {
	snapshot := &quxReadSnapshot{
		tableVersion: tableVersion,
		memtables:    memtables,
	}
	snapshot.refs.Store(1)
	return snapshot
}

func (s *quxReadSnapshot) tryRetain() bool {
	for refs := s.refs.Load(); refs != 0; refs = s.refs.Load() {
		if s.refs.CompareAndSwap(refs, refs+1) {
			return true
		}
	}
	return false
}

func (s *quxReadSnapshot) release() error {
	if s.refs.Add(-1) != 0 {
		return nil
	}
	return s.tableVersion.release()
}

func (s *quxReadSnapshot) openTable(table sst.Metadata) (sst.Reader, error) {
	return s.tableVersion.openTable(table)
}
