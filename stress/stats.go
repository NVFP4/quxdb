package main

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxStatsShards             = 32
	latencyBucketRelativeWidth = 0.01
	maxHistogramLatency        = time.Duration(1<<63 - 1)
	metricOperationCount       = 2
	invalidWindowGeneration    = ^uint64(0)
)

const (
	metricPUT = iota
	metricGET
)

var (
	latencyBucketLogWidth = math.Log1p(latencyBucketRelativeWidth)
	latencyBucketCount    = int(math.Ceil(math.Log(float64(maxHistogramLatency))/latencyBucketLogWidth)) + 2
)

type statsCollector struct {
	shards                []statsShard
	snapshotMu            sync.Mutex
	measurementStart      time.Time
	lastWindowCut         time.Time
	windowDispatchScratch *latencyHistogram
	windowGeneration      atomic.Uint64

	scheduled        atomic.Int64
	started          atomic.Uint64
	finished         atomic.Uint64
	inFlight         atomic.Int64
	maxFlight        atomic.Int64
	maxBacklog       atomic.Uint64
	lastStartOffset  atomic.Int64
	enqueueWaitNanos atomic.Int64
	maxEnqueueWait   atomic.Int64
	httpBusyNanos    atomic.Int64
}

type statsShard struct {
	mu            sync.Mutex
	ops           [metricOperationCount]operationStats
	totalDispatch *latencyHistogram
	windows       [2]windowStats
}

type operationStats struct {
	outcomes groupedResults
	dispatch *latencyHistogram
	service  *latencyHistogram
	endToEnd *latencyHistogram
}

type windowStats struct {
	generation uint64
	ops        [metricOperationCount]operationStats
	dispatch   *latencyHistogram
}

type groupedResults map[responseOutcome]uint64

type latencyHistogram struct {
	counts []uint64
	total  uint64
}

type latencySnapshot struct {
	count uint64
	p50   time.Duration
	p95   time.Duration
	p99   time.Duration
	p999  time.Duration
}

type operationSnapshot struct {
	outcomes []responseGroupSnapshot
	dispatch latencySnapshot
	service  latencySnapshot
	endToEnd latencySnapshot
}

type responseGroupSnapshot struct {
	response responseOutcome
	count    uint64
}

type statsSnapshot struct {
	put      operationSnapshot
	get      operationSnapshot
	dispatch latencySnapshot
	elapsed  time.Duration

	scheduled      uint64
	started        uint64
	finished       uint64
	inFlight       int64
	maxInFlight    int64
	maxBacklog     uint64
	enqueueWait    time.Duration
	maxEnqueueWait time.Duration
	httpBusyTime   time.Duration
}

func newStatsCollector(workers int) *statsCollector {
	shardCount := workers
	if shardCount <= 0 {
		shardCount = 1
	}
	if shardCount > maxStatsShards {
		shardCount = maxStatsShards
	}
	collector := &statsCollector{
		shards:                make([]statsShard, shardCount),
		windowDispatchScratch: newLatencyHistogram(),
	}
	for i := range collector.shards {
		shard := &collector.shards[i]
		shard.totalDispatch = newLatencyHistogram()
		for bank := range shard.windows {
			window := &shard.windows[bank]
			window.generation = invalidWindowGeneration
			window.dispatch = newLatencyHistogram()
		}
		shard.windows[0].generation = 0
	}
	return collector
}

func metricOperation(op operationKind) int {
	if op == operationPUT {
		return metricPUT
	}
	return metricGET
}

func (s *statsCollector) ObserveScheduled() {
	s.scheduled.Add(1)
}

func (s *statsCollector) ForgetScheduled() {
	s.scheduled.Add(-1)
}

func (s *statsCollector) StartMeasurement(start time.Time) {
	s.measurementStart = start
	s.lastWindowCut = start
}

func (s *statsCollector) BeginRequest(start time.Time) {
	s.started.Add(1)
	inFlight := s.inFlight.Add(1)
	updateAtomicMaxInt64(&s.maxFlight, inFlight)
	updateAtomicMaxInt64(&s.lastStartOffset, start.Sub(s.measurementStart).Nanoseconds())
}

func (s *statsCollector) LastRequestStart() (time.Time, bool) {
	if s.started.Load() == 0 {
		return time.Time{}, false
	}
	return s.measurementStart.Add(time.Duration(s.lastStartOffset.Load())), true
}

func (s *statsCollector) ObserveResult(workerID int, op operationKind, response responseOutcome, started bool, dispatchDelay, latency time.Duration) {
	var dispatchBucket, serviceBucket, endToEndBucket int
	if started {
		dispatchBucket = latencyBucketIndex(dispatchDelay)
		serviceBucket = latencyBucketIndex(latency)
		endToEndBucket = latencyBucketIndex(requestEndToEndLatency(dispatchDelay, latency))
	}
	shard := &s.shards[workerID%len(s.shards)]
	index := metricOperation(op)
	shard.mu.Lock()
	generation := s.windowGeneration.Load()
	window := &shard.windows[generation%uint64(len(shard.windows))]
	if window.generation != generation {
		if window.generation != invalidWindowGeneration {
			window.Reset()
		}
		window.generation = generation
	}
	if started {
		shard.totalDispatch.RecordBucket(dispatchBucket)
		window.dispatch.RecordBucket(dispatchBucket)
		shard.ops[index].RecordTiming(dispatchBucket, serviceBucket, endToEndBucket)
		window.ops[index].RecordTiming(dispatchBucket, serviceBucket, endToEndBucket)
	}
	shard.ops[index].outcomes.Record(response)
	window.ops[index].outcomes.Record(response)
	shard.mu.Unlock()

	s.finished.Add(1)
	if started {
		if latency > 0 {
			s.httpBusyNanos.Add(latency.Nanoseconds())
		}
		s.inFlight.Add(-1)
	}
}

func (s *statsCollector) ObserveBacklog(backlog uint64) {
	updateAtomicMaxUint64(&s.maxBacklog, backlog)
}

func (s *statsCollector) ObserveEnqueueWait(wait time.Duration) {
	if wait <= 0 {
		return
	}
	s.enqueueWaitNanos.Add(wait.Nanoseconds())
	updateAtomicMaxInt64(&s.maxEnqueueWait, wait.Nanoseconds())
}

func (s *statsCollector) Started() uint64 {
	return s.started.Load()
}

func (s *statsCollector) State() statsSnapshot {
	return statsSnapshot{
		scheduled:      uint64(max(s.scheduled.Load(), 0)),
		started:        s.started.Load(),
		finished:       s.finished.Load(),
		inFlight:       s.inFlight.Load(),
		maxInFlight:    s.maxFlight.Load(),
		maxBacklog:     s.maxBacklog.Load(),
		enqueueWait:    time.Duration(s.enqueueWaitNanos.Load()),
		maxEnqueueWait: time.Duration(s.maxEnqueueWait.Load()),
		httpBusyTime:   time.Duration(s.httpBusyNanos.Load()),
	}
}

func requestEndToEndLatency(dispatchDelay, httpLatency time.Duration) time.Duration {
	if httpLatency > maxHistogramLatency-dispatchDelay {
		return maxHistogramLatency
	}
	return dispatchDelay + httpLatency
}

func (s *operationStats) ensureTiming() {
	if s.dispatch == nil {
		s.dispatch = newLatencyHistogram()
		s.service = newLatencyHistogram()
		s.endToEnd = newLatencyHistogram()
	}
}

func (s *operationStats) RecordTiming(dispatchBucket, serviceBucket, endToEndBucket int) {
	s.ensureTiming()
	s.dispatch.RecordBucket(dispatchBucket)
	s.service.RecordBucket(serviceBucket)
	s.endToEnd.RecordBucket(endToEndBucket)
}

func (s *operationStats) Merge(other operationStats) {
	s.outcomes.Merge(other.outcomes)
	if other.dispatch == nil {
		return
	}
	s.ensureTiming()
	s.dispatch.Merge(*other.dispatch)
	s.service.Merge(*other.service)
	s.endToEnd.Merge(*other.endToEnd)
}

func (s *operationStats) Reset() {
	clear(s.outcomes)
	if s.dispatch != nil {
		s.dispatch.Reset()
		s.service.Reset()
		s.endToEnd.Reset()
	}
}

func (g *groupedResults) Record(response responseOutcome) {
	if *g == nil {
		*g = make(groupedResults)
	}
	(*g)[response]++
}

func (g *groupedResults) Merge(other groupedResults) {
	if len(other) == 0 {
		return
	}
	if *g == nil {
		*g = make(groupedResults, len(other))
	}
	for response, count := range other {
		(*g)[response] += count
	}
}

func (g groupedResults) Snapshot() []responseGroupSnapshot {
	snapshot := make([]responseGroupSnapshot, 0, len(g))
	for response, count := range g {
		if count > 0 {
			snapshot = append(snapshot, responseGroupSnapshot{response: response, count: count})
		}
	}
	sort.Slice(snapshot, func(i, j int) bool {
		return responseOutcomeLess(snapshot[i].response, snapshot[j].response)
	})
	return snapshot
}

func responseOutcomeLess(left, right responseOutcome) bool {
	leftRank := responseClassSortRank(left.class)
	rightRank := responseClassSortRank(right.class)
	if leftRank != rightRank {
		return leftRank < rightRank
	}
	if left.statusCode != right.statusCode {
		return left.statusCode < right.statusCode
	}
	return resultCauseSortRank(left.cause) < resultCauseSortRank(right.cause)
}

func responseClassSortRank(class responseClass) int {
	switch class {
	case responseHTTP:
		return 0
	case responseNoResponse:
		return 1
	default:
		return 2
	}
}

func resultCauseSortRank(cause resultCause) int {
	switch cause {
	case causeSuccess, causeHTTPStatus:
		return 0
	case causeVerification:
		return 1
	case causeBodyRead:
		return 2
	case causeClient:
		return 3
	case causeRequestBuild:
		return 4
	case causeCanceled:
		return 5
	default:
		return 6
	}
}

func newLatencyHistogram() *latencyHistogram {
	return &latencyHistogram{counts: make([]uint64, latencyBucketCount)}
}

func latencyBucketIndex(latency time.Duration) int {
	if latency <= 0 {
		return 0
	}
	index := int(math.Ceil(math.Log(float64(latency))/latencyBucketLogWidth)) + 1
	if index >= latencyBucketCount {
		return latencyBucketCount - 1
	}
	return index
}

func latencyBucketUpperBound(index int) time.Duration {
	if index <= 0 {
		return 0
	}
	upper := math.Exp(float64(index-1) * latencyBucketLogWidth)
	if upper >= float64(maxHistogramLatency) {
		return maxHistogramLatency
	}
	return time.Duration(math.Ceil(upper))
}

func (h *latencyHistogram) RecordBucket(index int) {
	h.counts[index]++
	h.total++
}

func (h *latencyHistogram) Merge(other latencyHistogram) {
	for i, count := range other.counts {
		h.counts[i] += count
	}
	h.total += other.total
}

func (h *latencyHistogram) Reset() {
	clear(h.counts)
	h.total = 0
}

func (w *windowStats) Reset() {
	for op := range metricOperationCount {
		w.ops[op].Reset()
	}
	w.dispatch.Reset()
}

func (h latencyHistogram) Snapshot() latencySnapshot {
	return latencySnapshot{
		count: h.total,
		p50:   h.Percentile(50),
		p95:   h.Percentile(95),
		p99:   h.Percentile(99),
		p999:  h.Percentile(99.9),
	}
}

func (h latencyHistogram) Percentile(percentile float64) time.Duration {
	if h.total == 0 {
		return 0
	}
	rank := uint64(math.Ceil((percentile / 100) * float64(h.total)))
	if rank == 0 {
		rank = 1
	}
	var cumulative uint64
	for index, count := range h.counts {
		cumulative += count
		if cumulative >= rank {
			return latencyBucketUpperBound(index)
		}
	}
	return maxHistogramLatency
}

func (s *statsCollector) WindowSnapshot() statsSnapshot {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()

	cut := time.Now()
	closedGeneration := s.windowGeneration.Add(1) - 1
	elapsed := cut.Sub(s.lastWindowCut)
	s.lastWindowCut = cut
	var aggregate [metricOperationCount]operationStats
	dispatch := s.windowDispatchScratch
	dispatch.Reset()
	for i := range s.shards {
		shard := &s.shards[i]
		bankIndex := closedGeneration % uint64(len(shard.windows))
		shard.mu.Lock()
		window := &shard.windows[bankIndex]
		if window.generation != closedGeneration {
			shard.mu.Unlock()
			continue
		}
		detached := *window
		shard.mu.Unlock()

		for op := range metricOperationCount {
			aggregate[op].Merge(detached.ops[op])
		}
		dispatch.Merge(*detached.dispatch)

		window.Reset()
		window.generation = invalidWindowGeneration
	}
	return statsSnapshot{
		put:      operationStatsSnapshot(aggregate[metricPUT]),
		get:      operationStatsSnapshot(aggregate[metricGET]),
		dispatch: dispatch.Snapshot(),
		elapsed:  elapsed,
	}
}

func (s *statsCollector) FinalSnapshot(elapsed time.Duration) statsSnapshot {
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()

	var aggregate [metricOperationCount]operationStats
	dispatch := newLatencyHistogram()
	for i := range s.shards {
		shard := &s.shards[i]
		shard.mu.Lock()
		for op := range metricOperationCount {
			aggregate[op].Merge(shard.ops[op])
		}
		dispatch.Merge(*shard.totalDispatch)
		shard.mu.Unlock()
	}
	state := s.State()
	state.put = operationStatsSnapshot(aggregate[metricPUT])
	state.get = operationStatsSnapshot(aggregate[metricGET])
	state.dispatch = dispatch.Snapshot()
	state.elapsed = elapsed
	return state
}

func operationStatsSnapshot(stats operationStats) operationSnapshot {
	snapshot := operationSnapshot{outcomes: stats.outcomes.Snapshot()}
	if stats.dispatch != nil {
		snapshot.dispatch = stats.dispatch.Snapshot()
		snapshot.service = stats.service.Snapshot()
		snapshot.endToEnd = stats.endToEnd.Snapshot()
	}
	return snapshot
}

func (s statsSnapshot) HTTPWorkerUtilization(workers int) float64 {
	if workers <= 0 || s.elapsed <= 0 {
		return 0
	}
	utilization := float64(s.httpBusyTime) / (float64(s.elapsed) * float64(workers))
	return min(max(utilization, 0), 1)
}

func (s operationSnapshot) totalCount() uint64 {
	var total uint64
	for _, outcome := range s.outcomes {
		total += outcome.count
	}
	return total
}

func (s operationSnapshot) successCount() uint64 {
	var total uint64
	for _, outcome := range s.outcomes {
		if outcome.response.cause == causeSuccess {
			total += outcome.count
		}
	}
	return total
}

func (s operationSnapshot) canceledCount() uint64 {
	var total uint64
	for _, outcome := range s.outcomes {
		if outcome.response.cause == causeCanceled {
			total += outcome.count
		}
	}
	return total
}

func (s operationSnapshot) failureCount() uint64 {
	return s.totalCount() - s.successCount() - s.canceledCount()
}

func updateAtomicMaxUint64(value *atomic.Uint64, candidate uint64) {
	for {
		maximum := value.Load()
		if candidate <= maximum || value.CompareAndSwap(maximum, candidate) {
			return
		}
	}
}

func updateAtomicMaxInt64(value *atomic.Int64, candidate int64) {
	for {
		maximum := value.Load()
		if candidate <= maximum || value.CompareAndSwap(maximum, candidate) {
			return
		}
	}
}

type failureStats struct {
	consecutive atomic.Int64
	threshold   int64
	cancel      func()
	triggered   atomic.Bool
	once        sync.Once
}

func newFailureStats(threshold int, cancel func()) *failureStats {
	return &failureStats{threshold: int64(threshold), cancel: cancel}
}

func (f *failureStats) Observe(success bool) {
	if f.threshold <= 0 {
		return
	}
	if success {
		f.consecutive.Store(0)
		return
	}
	streak := f.consecutive.Add(1)
	if streak < f.threshold {
		return
	}
	f.once.Do(func() {
		f.triggered.Store(true)
		f.cancel()
	})
}

func (f *failureStats) Triggered() bool {
	return f.triggered.Load()
}

func (f *failureStats) Reset() {
	f.consecutive.Store(0)
}
