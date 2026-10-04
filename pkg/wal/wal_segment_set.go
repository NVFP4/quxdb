package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yashgorana/quxdb/pkg/metrics"
)

const (
	segmentOpenMaxWorkers = 8
	walMaxSpares          = 2 // pruned segments kept for reuse

	walDir       = "wal"
	walTmpSuffix = ".tmp"
	// a full-size file outside the log, free to reuse as a segment
	walPreparedSuffix = ".prepared"
)

// segmentSet owns the segment files, mu guards membership.
type segmentSet struct {
	dir     string
	segSize uint64

	mu         sync.Mutex
	segments   []*walSegment
	byID       map[segID]*walSegment
	active     *walSegment
	spares     []string
	retainFrom atomic.Uint64 // lowest lsn recovery still needs

	next      *walSegment // segment active+1, built ahead of the rollover that needs it
	preparing bool        // the preparer is building next
	prepared  sync.Cond   // broadcast when preparing ends
	wake      chan struct{}
	done      chan struct{}
}

func newSegmentSet(dir string, segSize uint64) *segmentSet {
	s := &segmentSet{
		dir:      dir,
		segments: make([]*walSegment, 0),
		byID:     make(map[segID]*walSegment),
		segSize:  segSize,
	}
	s.prepared.L = &s.mu
	return s
}

func (s *segmentSet) open() error {
	if s.active != nil {
		return nil
	}

	// Segment names are ordered by (createdAt ASC, segID ASC).
	segments, spares, err := scanSegments(s.dir, s.segSize)
	if err != nil {
		return fmt.Errorf("wal open: %w", err)
	}
	s.spares = spares

	if len(segments) == 0 {
		return s.rollover()
	}

	for _, seg := range segments {
		if _, exists := s.byID[seg.segId]; exists {
			return errors.Join(
				fmt.Errorf("wal: duplicate segment id=%d", seg.segId),
				closeSegments(segments),
			)
		}
		s.byID[seg.segId] = seg
	}
	s.segments = append(s.segments, segments...)
	s.active = s.segments[len(s.segments)-1]
	s.publishMetricsLocked()

	return nil
}

func (s *segmentSet) publishMetricsLocked() {
	metrics.WalSegments.Set(float64(len(s.segments)))
	metrics.WalRetainedBytes.Set(float64(uint64(len(s.segments)) * s.segSize))
}

func (s *segmentSet) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	for _, seg := range s.segments {
		errs = append(errs, seg.close())
	}
	// an unused prepared segment stays on disk as an empty segment
	if s.next != nil {
		errs = append(errs, s.next.close())
		s.next = nil
	}
	clear(s.segments)
	s.segments = s.segments[:0]
	clear(s.byID)
	s.active = nil
	return errors.Join(errs...)
}

func (s *segmentSet) segmentForLSN(lsn LSN) (*walSegment, error) {
	sid := lsnSegID(lsn, s.segSize)
	seg := s.byID[sid]
	if seg == nil {
		return nil, fmt.Errorf("wal: unknown segment id=%d", sid)
	}
	return seg, nil
}

// rollover seals the active segment and switches to the next one, prepared or built inline.
func (s *segmentSet) rollover() error {
	start := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()

	// the preparer is building the segment this rollover needs
	observe := metrics.WalRolloverPrepared
	for s.preparing {
		observe = metrics.WalRolloverStalled
		s.prepared.Wait()
	}

	old := s.active
	nextID := segID(0)
	if old != nil {
		nextID = old.segId + 1
		if err := old.seal(); err != nil {
			return err
		}
	}

	seg := s.next
	s.next = nil
	if seg != nil {
		if err := seg.activate(); err != nil {
			// the .prepared file is reused as a spare after a restart
			return errors.Join(err, seg.close(), old.openReadWrite())
		}
	} else {
		observe = metrics.WalRolloverUnprepared
		var err error
		if seg, err = s.buildSegment(s.takeSpareLocked(), nextID, ""); err != nil {
			if old == nil {
				return err
			}
			return errors.Join(err, old.openReadWrite())
		}
	}

	fmt.Printf("wal: segment active sid=%d\n", seg.segId)
	s.segments = append(s.segments, seg)
	s.byID[seg.segId] = seg
	s.active = seg
	s.publishMetricsLocked()
	s.notifyPreparer()
	// the first segment of an empty log is not a switch
	if old != nil {
		observe.Observe(time.Since(start).Seconds())
	}

	return nil
}

// startPreparer begins building each next segment in the background.
func (s *segmentSet) startPreparer() {
	s.wake = make(chan struct{}, 1)
	s.done = make(chan struct{})
	go s.prepareLoop()
	s.notifyPreparer()
}

func (s *segmentSet) stopPreparer() {
	if s.wake == nil {
		return
	}
	close(s.wake)
	<-s.done
	s.wake = nil
}

func (s *segmentSet) notifyPreparer() {
	if s.wake == nil {
		return
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *segmentSet) prepareLoop() {
	defer close(s.done)
	for range s.wake {
		s.prepareNext()
	}
}

// prepareNext builds segment active+1 unless it is ready, preferring a spare over a new file.
func (s *segmentSet) prepareNext() {
	if err := s.prune(); err != nil {
		fmt.Printf("wal: prune before prepare: %v\n", err)
	}

	s.mu.Lock()
	if s.next != nil || s.active == nil {
		s.mu.Unlock()
		return
	}
	id := s.active.segId + 1
	spare := s.takeSpareLocked()
	s.preparing = true
	s.mu.Unlock()

	seg, err := s.buildSegment(spare, id, walPreparedSuffix)

	s.mu.Lock()
	s.preparing = false
	if err != nil {
		fmt.Printf("wal: prepare segment id=%d: %v\n", id, err)
	} else {
		s.next = seg
	}
	s.prepared.Broadcast()
	s.mu.Unlock()
}

func (s *segmentSet) takeSpareLocked() string {
	n := len(s.spares)
	if n == 0 {
		return ""
	}
	spare := s.spares[n-1]
	s.spares = s.spares[:n-1]
	return spare
}

// buildSegment recycles spare into segment id named with suffix, or creates a new one without a spare.
func (s *segmentSet) buildSegment(spare string, id segID, suffix string) (*walSegment, error) {
	if spare != "" {
		seg, err := recycleSegment(spare, s.dir, id, s.segSize, suffix)
		if err == nil {
			return seg, nil
		}
		fmt.Printf("wal: dropping spare: %v\n", errors.Join(err, os.Remove(spare)))
	}
	return newSegment(s.dir, id, s.segSize, suffix)
}

// prune retires segments before the retain point's segment, their file io runs outside mu.
func (s *segmentSet) prune() error {
	s.mu.Lock()
	retired := s.detachRetiredLocked()
	s.mu.Unlock()
	return s.retire(retired)
}

// detachRetiredLocked removes segments before the retain point's segment from the set.
func (s *segmentSet) detachRetiredLocked() []*walSegment {
	from := LSN(s.retainFrom.Load())
	if from == 0 {
		return nil
	}
	keep := s.byID[lsnSegID(from, s.segSize)]
	if keep == nil {
		return nil
	}

	n := slices.Index(s.segments, keep)
	retired := slices.Clone(s.segments[:n])
	for _, seg := range retired {
		delete(s.byID, seg.segId)
	}
	clear(s.segments[:n])
	s.segments = s.segments[n:]
	s.publishMetricsLocked()
	return retired
}

// retire keeps detached segments for reuse up to walMaxSpares and deletes the rest.
func (s *segmentSet) retire(segs []*walSegment) error {
	var errs []error
	for _, seg := range segs {
		if err := seg.close(); err != nil {
			errs = append(errs, fmt.Errorf("wal: close segment id=%d before prune: %w", seg.segId, err))
			continue
		}
		spare := seg.path + walPreparedSuffix
		if err := os.Rename(seg.path, spare); err != nil {
			errs = append(errs, fmt.Errorf("wal: prune segment id=%d: %w", seg.segId, err))
			continue
		}

		s.mu.Lock()
		kept := len(s.spares) < walMaxSpares
		if kept {
			s.spares = append(s.spares, spare)
		}
		s.mu.Unlock()

		if !kept {
			if err := os.Remove(spare); err != nil {
				errs = append(errs, fmt.Errorf("wal: prune segment id=%d: %w", seg.segId, err))
			}
		}
	}
	return errors.Join(errs...)
}

// truncateTail ends the log at lsn and deletes later segments.
func (s *segmentSet) truncateTail(lsn LSN) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	targetSID := lsnSegID(lsn, s.segSize)
	target := s.byID[targetSID]
	if target == nil {
		return fmt.Errorf("wal: unknown segment id=%d", targetSID)
	}

	if err := target.truncate(lsn); err != nil {
		return err
	}
	s.active = target

	targetIdx := slices.Index(s.segments, target)
	if targetIdx == -1 {
		return fmt.Errorf("wal: segment id=%d not found", targetSID)
	}
	cut := targetIdx + 1
	var errs []error
	for _, seg := range s.segments[cut:] {
		errs = append(errs, seg.close())
		if err := os.Remove(seg.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
		delete(s.byID, seg.segId)
	}
	// Truncation is irreversible, so retain state changes even when cleanup fails.
	clear(s.segments[cut:])
	s.segments = s.segments[:cut]
	s.publishMetricsLocked()

	return errors.Join(errs...)
}

// tornTail reports whether no Record starts after the one at lsn.
func (s *segmentSet) tornTail(lsn LSN) (bool, error) {
	seg, err := s.segmentForLSN(lsn)
	if err != nil {
		return false, err
	}
	from := lsnOffset(lsn, s.segSize) + 8
	for _, next := range s.segments[slices.Index(s.segments, seg):] {
		found, err := next.hasRecordStartFrom(from)
		if err != nil || found {
			return false, err
		}
		from = lsnOffset(next.startLSN, s.segSize)
	}
	return true, nil
}

func scanSegments(dir string, segmentSize uint64) ([]*walSegment, []string, error) {
	files, err := filepath.Glob(filepath.Join(dir, walDir, "*.quxwal"))
	if err != nil {
		return nil, nil, fmt.Errorf("wal: discover segments %w", err)
	}
	spares, _ := filepath.Glob(filepath.Join(dir, walDir, "*.quxwal"+walPreparedSuffix))
	// segments that never finished zero-filling
	stale, _ := filepath.Glob(filepath.Join(dir, walDir, "*.quxwal"+walTmpSuffix))
	for _, path := range stale {
		if err := os.Remove(path); err != nil {
			return nil, nil, fmt.Errorf("wal: remove stale segment %w", err)
		}
	}

	if len(files) == 0 {
		return make([]*walSegment, 0), spares, nil
	}
	type task struct {
		index int
		path  string
		mode  segmentMode
	}
	type result struct {
		index int
		seg   *walSegment
		err   error
	}

	workerCount := min(runtime.GOMAXPROCS(0), segmentOpenMaxWorkers, len(files))
	tasks := make(chan task)
	results := make(chan result, len(files))
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for task := range tasks {
				seg, err := openSegment(task.path, segmentSize, task.mode)
				results <- result{index: task.index, seg: seg, err: err}
			}
		}()
	}

	for i := len(files) - 1; i >= 0; i-- {
		mode := segmentModeReadOnly
		if i == len(files)-1 {
			mode = segmentModeReadWrite
		}
		tasks <- task{index: i, path: files[i], mode: mode}
	}

	close(tasks)
	workers.Wait()
	close(results)

	segments := make([]*walSegment, len(files))
	errs := make([]error, len(files))
	for result := range results {
		segments[result.index] = result.seg
		errs[result.index] = result.err
	}

	for _, err := range errs {
		if err != nil {
			return nil, nil, errors.Join(err, closeSegments(segments))
		}
	}

	return segments, spares, nil
}

func closeSegments(segments []*walSegment) error {
	var errs []error
	for _, seg := range segments {
		if seg != nil {
			errs = append(errs, seg.close())
		}
	}
	return errors.Join(errs...)
}
