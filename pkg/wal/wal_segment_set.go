package wal

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
)

var ErrSegmentIDExhausted = errors.New("wal: segment id exhausted")

type segmentSet struct {
	dir      string
	segments []*walSegment
	byID     map[segID]*walSegment
	active   *walSegment
}

func newSegmentSet(dir string) *segmentSet {
	return &segmentSet{
		dir:      dir,
		segments: make([]*walSegment, 0),
		byID:     make(map[segID]*walSegment),
	}
}

func (s *segmentSet) open() error {
	if s.active != nil {
		return nil
	}

	segments, err := scanSegments(s.dir)
	if err != nil {
		return fmt.Errorf("wal open: %w", err)
	}
	if len(segments) == 0 {
		return s.rollover()
	}

	for _, seg := range segments {
		if _, exists := s.byID[seg.sid]; exists {
			return fmt.Errorf("wal: duplicate segment id=%d", seg.sid)
		}
		s.byID[seg.sid] = seg
	}
	s.segments = append(s.segments, segments...)
	s.active = s.segments[len(s.segments)-1]

	return s.freezeInactive()
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
	sid := lsnSegID(lsn)
	seg := s.byID[sid]
	if seg == nil {
		return nil, fmt.Errorf("wal: unknown segment id=%d", sid)
	}
	return seg, nil
}

func (s *segmentSet) rollover() error {
	nextID := segID(0)
	old := s.active
	if s.active != nil {
		if s.active.sid == math.MaxUint32 {
			return ErrSegmentIDExhausted
		}
		nextID = s.active.sid + 1
	}

	seg, err := newSegment(s.dir, nextID)
	if err != nil {
		return err
	}
	if old != nil {
		if err := old.closeFile(); err != nil {
			_ = seg.close()
			_ = os.Remove(seg.path)
			return err
		}
	}

	fmt.Printf("wal: segment create sid=%d\n", seg.sid)
	s.segments = append(s.segments, seg)
	s.byID[seg.sid] = seg
	s.active = seg

	return nil
}

func (s *segmentSet) freezeInactive() error {
	for _, seg := range s.segments {
		if seg == s.active {
			continue
		}
		if err := seg.closeFile(); err != nil {
			return err
		}
	}
	return nil
}

func (s *segmentSet) truncateTail(lsn LSN) error {
	targetSID := lsnSegID(lsn)
	target := s.byID[targetSID]
	if target == nil {
		return fmt.Errorf("wal: unknown segment id=%d", targetSID)
	}

	cut := len(s.segments)
	for i, seg := range s.segments {
		if seg.sid > targetSID {
			cut = i
			break
		}
	}

	var errs []error
	for _, seg := range s.segments[cut:] {
		errs = append(errs, seg.close())
		if err := os.Remove(seg.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
		delete(s.byID, seg.sid)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	s.segments = s.segments[:cut]

	if err := target.truncate(lsn); err != nil {
		return err
	}
	s.active = target
	return nil
}

func scanSegments(dir string) ([]*walSegment, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.quxwal"))
	if err != nil {
		return nil, fmt.Errorf("wal: discover segments %w", err)
	}
	segments := make([]*walSegment, 0, len(files))

	for _, file := range files {
		seg, err := openSegment(file)
		if err != nil {
			return nil, err
		}
		segments = append(segments, seg)
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].sid < segments[j].sid
	})

	return segments, nil
}
