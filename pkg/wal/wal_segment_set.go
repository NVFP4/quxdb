package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
)

const (
	segmentOpenMaxWorkers = 8

	walDir = "wal"
)

type segmentSet struct {
	dir      string
	segments []*walSegment
	byID     map[segID]*walSegment
	active   *walSegment

	segSize uint64
}

func newSegmentSet(dir string, segSize uint64) *segmentSet {
	return &segmentSet{
		dir:      dir,
		segments: make([]*walSegment, 0),
		byID:     make(map[segID]*walSegment),
		segSize:  segSize,
	}
}

func (s *segmentSet) open() error {
	if s.active != nil {
		return nil
	}

	// Segment names are ordered by (createdAt ASC, segID ASC).
	segments, err := scanSegments(s.dir, s.segSize)
	if err != nil {
		return fmt.Errorf("wal open: %w", err)
	}

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

	return nil
}

func (s *segmentSet) close() error {
	var errs []error
	for _, seg := range s.segments {
		errs = append(errs, seg.close())
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

func (s *segmentSet) rollover() error {
	old := s.active
	nextID := segID(0)
	if old != nil {
		nextID = old.segId + 1
	}

	if old != nil {
		if err := old.seal(); err != nil {
			return err
		}
	}

	seg, err := newSegment(s.dir, nextID, s.segSize)
	if err != nil {
		if old == nil {
			return err
		}
		return errors.Join(err, old.openReadWrite())
	}

	fmt.Printf("wal: segment create sid=%d\n", seg.segId)
	s.segments = append(s.segments, seg)
	s.byID[seg.segId] = seg
	s.active = seg

	return nil
}

func (s *segmentSet) pruneBefore(lsn LSN) error {
	if lsn == 0 {
		return nil
	}

	target, err := s.segmentForLSN(lsn)
	if err != nil {
		return err
	}
	offset := lsnOffset(lsn, target.segMaxSize)
	if offset < lsnOffset(target.startLSN, target.segMaxSize) || offset > target.cursor {
		return fmt.Errorf("%w: lsn=%d", ErrSegmentLSNInvalid, lsn)
	}

	targetIdx := slices.Index(s.segments, target)
	if targetIdx == -1 {
		return fmt.Errorf("wal: segment id=%d not found", target.segId)
	}

	for range targetIdx {
		seg := s.segments[0]
		if err := seg.close(); err != nil {
			return fmt.Errorf("wal: close segment id=%d before prune: %w", seg.segId, err)
		}
		if err := os.Remove(seg.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("wal: prune segment id=%d: %w", seg.segId, err)
		}

		delete(s.byID, seg.segId)
		s.segments[0] = nil
		s.segments = s.segments[1:]
	}

	return nil
}

func (s *segmentSet) truncateTail(lsn LSN) error {
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

	return errors.Join(errs...)
}

func scanSegments(dir string, segmentSize uint64) ([]*walSegment, error) {
	files, err := filepath.Glob(filepath.Join(dir, walDir, "*.quxwal"))
	if err != nil {
		return nil, fmt.Errorf("wal: discover segments %w", err)
	}

	if len(files) == 0 {
		return make([]*walSegment, 0), nil
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
			return nil, errors.Join(err, closeSegments(segments))
		}
	}

	return segments, nil
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
