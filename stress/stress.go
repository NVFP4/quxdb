package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ReadPercent      = 10
	StopThreshold    = 10
	KeyStoreCapacity = 100_000
	PayloadPoolSize  = 10_000
	MetricsInterval  = 5 * time.Second

	keyPath    = "/k/"
	healthPath = "/health"
)

var (
	errFailureThreshold = errors.New("failure threshold reached")
	errScheduleRange    = errors.New("request schedule exceeds the supported time range")
)

type runConfig struct {
	duration       time.Duration
	requests       uint64
	rate           float64
	workers        int
	queue          int
	warmup         time.Duration
	url            string
	scan           bool
	keyMode        keyMode
	keyPrefix      bool
	sequentialMax  uint64
	timestampValue bool
	noGET          bool
}

type runner struct {
	cfg         runConfig
	client      *http.Client
	urlPrefix   string
	keys        *keyStore
	keyGen      *keyGenerator
	valGen      *valGenerator
	stats       *statsCollector
	verifyValue bool
	// loggedFailures marks failure causes already printed; repeats appear only in the report counts.
	loggedFailures [causeRequestBuild + 1]atomic.Bool
}

type operationKind uint8

const (
	operationPUT operationKind = iota
	operationGET
	operationScan
)

type responseClass uint8

const (
	responseNotStarted responseClass = iota
	responseNoResponse
	responseHTTP
)

type resultCause uint8

const (
	causeSuccess resultCause = iota
	causeHTTPStatus
	causeClient
	causeBodyRead
	causeVerification
	causeCanceled
	causeRequestBuild
)

type responseOutcome struct {
	class      responseClass
	statusCode int
	cause      resultCause
}

type scheduledJob struct {
	due      time.Time
	op       operationKind
	measured bool
	phase    *phaseTracker
}

type operationResult struct {
	op       operationKind
	started  bool
	latency  time.Duration
	response responseOutcome
}

func newOperationResult(op operationKind) operationResult {
	return operationResult{
		op: op,
		response: responseOutcome{
			class: responseNotStarted,
			cause: causeRequestBuild,
		},
	}
}

func canceledOperationResult(op operationKind) operationResult {
	result := newOperationResult(op)
	result.response.cause = causeCanceled
	return result
}

type requestStartFunc func(time.Time)

type phaseResult struct {
	tracker    *phaseTracker
	scheduled  uint64
	nominalEnd time.Time
}

type runTiming struct {
	warmup          time.Duration
	active          time.Duration
	arrivalOverrun  time.Duration
	completionDrain time.Duration
	wall            time.Duration
	total           time.Duration
}

type phaseTracker struct {
	pending        atomic.Int64
	schedulingDone atomic.Bool
	closeOnce      sync.Once
	done           chan struct{}
}

type deadlineWaiter struct {
	timer *time.Timer
}

func newRunner(ctx context.Context, cfg *runConfig) (*runner, error) {
	baseURL := strings.TrimRight(cfg.url, "/")
	r := &runner{
		cfg:         *cfg,
		client:      newHTTPClient(),
		urlPrefix:   baseURL + keyPath,
		verifyValue: verifyGETValues(cfg),
		stats:       newStatsCollector(cfg.workers),
	}
	if err := ensureServerReachable(ctx, r.client, baseURL+healthPath); err != nil {
		return nil, fmt.Errorf("cannot reach server at %s: %w", cfg.url, err)
	}
	if !cfg.scan {
		r.keys = newKeyStore(KeyStoreCapacity, cfg.workers)
		var payloads *payloadPool
		if !cfg.timestampValue {
			payloads = newPayloadPool(payloadPoolSize(cfg))
		}
		r.keyGen = newKeyGenerator(cfg.keyMode, cfg.sequentialMax, cfg.keyPrefix)
		r.valGen = newValGenerator(payloads, cfg.timestampValue)
	}
	return r, nil
}

func (r *runner) start(parent context.Context) error {
	runCtx, cancelRun := context.WithCancelCause(parent)
	defer cancelRun(nil)
	failures := newFailureStats(StopThreshold, func() {
		cancelRun(errFailureThreshold)
	})

	logRunConfig(r.cfg, r.verifyValue)
	jobs := make(chan scheduledJob, r.cfg.queue)
	var workers sync.WaitGroup
	var workerReady sync.WaitGroup
	workerReady.Add(r.cfg.workers)
	for workerID := range r.cfg.workers {
		workers.Go(func() {
			workerReady.Done()
			r.worker(runCtx, workerID, jobs, failures)
		})
	}
	workerReady.Wait()

	totalStart := time.Now()
	if r.cfg.warmup > 0 {
		warmupStart := time.Now()
		warmupPhase, err := r.scheduleDuration(runCtx, jobs, warmupStart, r.cfg.warmup, false)
		if err == nil {
			warmupPhase.tracker.Wait(runCtx)
		}
		warmupEnd := time.Now()
		if err != nil || runCtx.Err() != nil {
			close(jobs)
			workers.Wait()
			return r.runError(runCtx, failures, err)
		}
		warmupOverrun := max(warmupEnd.Sub(warmupPhase.nominalEnd), 0)
		if warmupOverrun > max(100*time.Millisecond, r.cfg.warmup/100) {
			logWarning("Warm-up requests needed extra time",
				fmt.Sprintf("It needed another %s to drain at the target load.", formatDuration(warmupOverrun)))
		}
		failures.Reset()
	}

	activeStart := time.Now()
	r.stats.StartMeasurement(activeStart)
	reporterCtx, stopReporter := context.WithCancel(runCtx)
	reporterWait := r.startReporter(reporterCtx)

	var (
		activePhase phaseResult
		scheduleErr error
	)
	if r.cfg.requests > 0 {
		activePhase, scheduleErr = r.scheduleFixed(runCtx, jobs, activeStart, r.cfg.requests, true)
	} else {
		activePhase, scheduleErr = r.scheduleDuration(runCtx, jobs, activeStart, r.cfg.duration, true)
	}
	if scheduleErr == nil {
		activePhase.tracker.Wait(runCtx)
	}

	close(jobs)
	workers.Wait()
	completionEnd := time.Now()
	stopReporter()
	reporterWait()

	if activePhase.nominalEnd.IsZero() || activePhase.nominalEnd.Before(activeStart) {
		activePhase.nominalEnd = completionEnd
	}
	if activePhase.nominalEnd.After(completionEnd) {
		activePhase.nominalEnd = completionEnd
	}
	arrivalEnd := activePhase.nominalEnd
	if lastStart, ok := r.stats.LastRequestStart(); ok && lastStart.After(arrivalEnd) {
		arrivalEnd = lastStart
	}
	timing := runTiming{
		warmup:          activeStart.Sub(totalStart),
		active:          activePhase.nominalEnd.Sub(activeStart),
		arrivalOverrun:  max(arrivalEnd.Sub(activePhase.nominalEnd), 0),
		completionDrain: max(completionEnd.Sub(arrivalEnd), 0),
		wall:            completionEnd.Sub(activeStart),
		total:           completionEnd.Sub(totalStart),
	}
	if r.cfg.warmup == 0 {
		timing.warmup = 0
	}
	final := r.stats.FinalSnapshot(timing.wall)
	logFinalStats(final, r.cfg, timing)
	if err := r.runError(runCtx, failures, scheduleErr); err != nil {
		return err
	}
	logBottleneckWarning(r.cfg, final, timing)
	logRunFinished(r.cfg)
	return nil
}

func (r *runner) logRequestFailure(cause resultCause, format string, args ...any) {
	if r.loggedFailures[cause].Swap(true) {
		return
	}
	logError(format+" (further failures of this kind are only counted)", args...)
}

func (r *runner) worker(ctx context.Context, workerID int, jobs <-chan scheduledJob, failures *failureStats) {
	for job := range jobs {
		if ctx.Err() != nil {
			if job.measured {
				r.stats.ObserveResult(workerID, job.op, canceledOperationResult(job.op).response, false, 0, 0)
			}
			job.phase.Done()
			continue
		}

		var startedAt time.Time
		onStart := func(start time.Time) {
			startedAt = start
			if job.measured {
				r.stats.BeginRequest(start)
			}
		}
		result := r.performOperation(ctx, job.op, onStart)
		if job.measured {
			dispatchDelay := time.Duration(0)
			if result.started {
				dispatchDelay = max(startedAt.Sub(job.due), 0)
			}
			r.stats.ObserveResult(workerID, result.op, result.response, result.started, dispatchDelay, result.latency)
		}
		if result.response.cause != causeCanceled {
			failures.Observe(result.response.cause == causeSuccess)
		}
		job.phase.Done()
	}
}

func (r *runner) startReporter(ctx context.Context) func() {
	var reporter sync.WaitGroup
	reporter.Go(func() {
		ticker := time.NewTicker(MetricsInterval)
		defer ticker.Stop()
		var measured time.Duration
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				snapshot := r.stats.WindowSnapshot()
				measured += snapshot.elapsed
				logWindowStats(measured, snapshot, r.cfg)
			}
		}
	})
	return reporter.Wait
}

func (r *runner) scheduleDuration(ctx context.Context, jobs chan<- scheduledJob, start time.Time, duration time.Duration, measured bool) (result phaseResult, err error) {
	total, err := scheduledArrivalCount(r.cfg.rate, duration)
	if err != nil {
		return phaseResult{}, err
	}
	result = phaseResult{tracker: newPhaseTracker(), nominalEnd: start.Add(duration)}
	waiter := newDeadlineWaiter()
	defer func() {
		waiter.Stop()
		result.tracker.FinishScheduling()
	}()

	for sequence := uint64(0); sequence < total; sequence++ {
		if err := r.scheduleOne(ctx, jobs, result.tracker, waiter, start, sequence, total, measured); err != nil {
			return result, err
		}
		result.scheduled++
	}
	if !waiter.Wait(ctx, result.nominalEnd) {
		return result, context.Cause(ctx)
	}
	return result, nil
}

func (r *runner) scheduleFixed(ctx context.Context, jobs chan<- scheduledJob, start time.Time, putLimit uint64, measured bool) (result phaseResult, err error) {
	result.tracker = newPhaseTracker()
	waiter := newDeadlineWaiter()
	defer func() {
		waiter.Stop()
		result.tracker.FinishScheduling()
	}()

	var putCount uint64
	for sequence := uint64(0); putCount < putLimit; sequence++ {
		due, dueErr := scheduledTime(start, sequence, r.cfg.rate)
		if dueErr != nil {
			return result, dueErr
		}
		if !waiter.Wait(ctx, due) {
			return result, context.Cause(ctx)
		}
		op := r.chooseOperation()
		if op == operationPUT || op == operationScan {
			putCount++
		}
		if err := r.enqueue(ctx, jobs, result.tracker, scheduledJob{due: due, op: op, measured: measured}, measured); err != nil {
			return result, err
		}
		result.scheduled++
		r.observeBacklog(start, result.scheduled, 0)
	}
	offset, offsetErr := scheduledOffset(result.scheduled, r.cfg.rate)
	if offsetErr != nil {
		return result, offsetErr
	}
	result.nominalEnd = start.Add(offset)
	r.observeBacklog(start, result.scheduled, result.scheduled)
	if !waiter.Wait(ctx, result.nominalEnd) {
		return result, context.Cause(ctx)
	}
	return result, nil
}

func (r *runner) scheduleOne(ctx context.Context, jobs chan<- scheduledJob, tracker *phaseTracker, waiter *deadlineWaiter, start time.Time, sequence, total uint64, measured bool) error {
	due, err := scheduledTime(start, sequence, r.cfg.rate)
	if err != nil {
		return err
	}
	if !waiter.Wait(ctx, due) {
		return context.Cause(ctx)
	}
	job := scheduledJob{due: due, op: r.chooseOperation(), measured: measured}
	if err := r.enqueue(ctx, jobs, tracker, job, measured); err != nil {
		return err
	}
	if measured {
		r.observeBacklog(start, sequence+1, total)
	}
	return nil
}

func (r *runner) enqueue(ctx context.Context, jobs chan<- scheduledJob, tracker *phaseTracker, job scheduledJob, measured bool) error {
	tracker.Add()
	job.phase = tracker
	if measured {
		r.stats.ObserveScheduled()
	}
	select {
	case jobs <- job:
		return nil
	case <-ctx.Done():
		if measured {
			r.stats.ForgetScheduled()
		}
		tracker.Done()
		return context.Cause(ctx)
	default:
	}

	waitStart := time.Now()
	select {
	case jobs <- job:
		if measured {
			r.stats.ObserveEnqueueWait(time.Since(waitStart))
		}
		return nil
	case <-ctx.Done():
		if measured {
			r.stats.ForgetScheduled()
		}
		tracker.Done()
		return context.Cause(ctx)
	}
}

func (r *runner) observeBacklog(start time.Time, scheduled, limit uint64) {
	elapsed := time.Since(start)
	if elapsed < 0 {
		return
	}
	due := uint64(math.Floor(elapsed.Seconds()*r.cfg.rate)) + 1
	if limit > 0 && due > limit {
		due = limit
	}
	if due < scheduled {
		due = scheduled
	}
	started := r.stats.Started()
	if due > started {
		r.stats.ObserveBacklog(due - started)
	}
}

func scheduledArrivalCount(rate float64, duration time.Duration) (uint64, error) {
	arrivals := rate * duration.Seconds()
	if math.IsInf(arrivals, 0) || math.IsNaN(arrivals) || arrivals > float64(^uint64(0)) {
		return 0, errScheduleRange
	}
	count := math.Ceil(math.Nextafter(arrivals, math.Inf(-1)))
	if count < 1 {
		count = 1
	}
	return uint64(count), nil
}

func scheduledTime(start time.Time, sequence uint64, rate float64) (time.Time, error) {
	offset, err := scheduledOffset(sequence, rate)
	if err != nil {
		return time.Time{}, err
	}
	return start.Add(offset), nil
}

func scheduledOffset(sequence uint64, rate float64) (time.Duration, error) {
	nanoseconds := (float64(sequence) * float64(time.Second)) / rate
	if math.IsInf(nanoseconds, 0) || nanoseconds >= float64(time.Duration(1<<63-1)) {
		return 0, errScheduleRange
	}
	return time.Duration(nanoseconds), nil
}

func (r *runner) chooseOperation() operationKind {
	if r.cfg.scan {
		return operationScan
	}
	if !r.cfg.noGET && r.keys.HasKeys() && mrand.IntN(100) < ReadPercent {
		return operationGET
	}
	return operationPUT
}

func (r *runner) performOperation(ctx context.Context, op operationKind, onStart requestStartFunc) operationResult {
	switch op {
	case operationScan:
		return r.doScanGET(ctx, onStart)
	case operationGET:
		return r.doGET(ctx, onStart)
	default:
		return r.doPUT(ctx, onStart)
	}
}

func (r *runner) doPUT(ctx context.Context, onStart requestStartFunc) operationResult {
	result := newOperationResult(operationPUT)
	if ctx.Err() != nil {
		return canceledOperationResult(operationPUT)
	}
	generated := r.keyGen.Next()
	key := generated.value
	value := r.valGen.Value()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, r.urlPrefix+url.PathEscape(key), strings.NewReader(value))
	if err != nil {
		if ctx.Err() != nil {
			return canceledOperationResult(operationPUT)
		}
		r.logRequestFailure(causeRequestBuild, "%v", err)
		return result
	}
	req.Header.Set("Content-Type", r.valGen.ContentType())

	start := markRequestStart(onStart)
	result.started = true
	resp, err := r.client.Do(req)
	if err != nil {
		result.latency = time.Since(start)
		canceled := ctx.Err() != nil
		result.response = responseOutcome{class: responseNoResponse, cause: causeClient}
		if resp != nil {
			result.response = responseOutcome{class: responseHTTP, statusCode: resp.StatusCode, cause: causeClient}
			if resp.Body != nil {
				resp.Body.Close()
			}
		}
		if canceled {
			result.response.cause = causeCanceled
		}
		if !canceled {
			r.logRequestFailure(causeClient, "%v", err)
		}
		return result
	}
	result.response = responseOutcome{class: responseHTTP, statusCode: resp.StatusCode, cause: causeSuccess}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		resp.Body.Close()
		result.latency = time.Since(start)
		canceled := ctx.Err() != nil
		result.response.cause = causeBodyRead
		if canceled {
			result.response.cause = causeCanceled
		}
		if !canceled {
			r.logRequestFailure(causeBodyRead, "%v", err)
		}
		return result
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		result.latency = time.Since(start)
		result.response.cause = causeHTTPStatus
		r.logRequestFailure(causeHTTPStatus, "PUT status %d", resp.StatusCode)
		return result
	}
	result.latency = time.Since(start)
	r.keys.Put(key, value, generated.verifyValue)
	return result
}

func (r *runner) doGET(ctx context.Context, onStart requestStartFunc) operationResult {
	result := newOperationResult(operationGET)
	if ctx.Err() != nil {
		return canceledOperationResult(operationGET)
	}
	key, expectedValue, verifyValue, ok := r.keys.GetRandom()
	if !ok {
		if ctx.Err() != nil {
			return canceledOperationResult(operationGET)
		}
		return r.doPUT(ctx, onStart)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.urlPrefix+url.PathEscape(key), nil)
	if err != nil {
		if ctx.Err() != nil {
			return canceledOperationResult(operationGET)
		}
		r.logRequestFailure(causeRequestBuild, "%v", err)
		return result
	}

	start := markRequestStart(onStart)
	result.started = true
	resp, err := r.client.Do(req)
	if err != nil {
		result.latency = time.Since(start)
		canceled := ctx.Err() != nil
		result.response = responseOutcome{class: responseNoResponse, cause: causeClient}
		if resp != nil {
			result.response = responseOutcome{class: responseHTTP, statusCode: resp.StatusCode, cause: causeClient}
			if resp.Body != nil {
				resp.Body.Close()
			}
		}
		if canceled {
			result.response.cause = causeCanceled
		}
		if !canceled {
			r.logRequestFailure(causeClient, "%v", err)
		}
		return result
	}
	result.response = responseOutcome{class: responseHTTP, statusCode: resp.StatusCode, cause: causeSuccess}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		result.latency = time.Since(start)
		canceled := ctx.Err() != nil
		result.response.cause = causeBodyRead
		if canceled {
			result.response.cause = causeCanceled
		}
		if !canceled {
			r.logRequestFailure(causeBodyRead, "%v", err)
		}
		return result
	}
	if resp.StatusCode != http.StatusOK {
		result.latency = time.Since(start)
		result.response.cause = causeHTTPStatus
		r.logRequestFailure(causeHTTPStatus, "GET status %d for key %s", resp.StatusCode, key)
		return result
	}
	if r.verifyValue && verifyValue && string(body) != expectedValue {
		result.latency = time.Since(start)
		result.response.cause = causeVerification
		r.logRequestFailure(causeVerification, "GET value mismatch for key %s | want: %s | got: %s", key, expectedValue, string(body))
		return result
	}
	result.latency = time.Since(start)
	return result
}

func (r *runner) doScanGET(ctx context.Context, onStart requestStartFunc) operationResult {
	result := newOperationResult(operationScan)
	if ctx.Err() != nil {
		return canceledOperationResult(operationScan)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.urlPrefix, nil)
	if err != nil {
		if ctx.Err() != nil {
			return canceledOperationResult(operationScan)
		}
		r.logRequestFailure(causeRequestBuild, "%v", err)
		return result
	}
	start := markRequestStart(onStart)
	result.started = true
	resp, err := r.client.Do(req)
	if err != nil {
		result.latency = time.Since(start)
		canceled := ctx.Err() != nil
		result.response = responseOutcome{class: responseNoResponse, cause: causeClient}
		if resp != nil {
			result.response = responseOutcome{class: responseHTTP, statusCode: resp.StatusCode, cause: causeClient}
			if resp.Body != nil {
				resp.Body.Close()
			}
		}
		if canceled {
			result.response.cause = causeCanceled
		}
		if !canceled {
			r.logRequestFailure(causeClient, "%v", err)
		}
		return result
	}
	result.response = responseOutcome{class: responseHTTP, statusCode: resp.StatusCode, cause: causeSuccess}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		resp.Body.Close()
		result.latency = time.Since(start)
		canceled := ctx.Err() != nil
		result.response.cause = causeBodyRead
		if canceled {
			result.response.cause = causeCanceled
		}
		if !canceled {
			r.logRequestFailure(causeBodyRead, "%v", err)
		}
		return result
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		result.latency = time.Since(start)
		result.response.cause = causeHTTPStatus
		r.logRequestFailure(causeHTTPStatus, "scan status %d", resp.StatusCode)
		return result
	}
	result.latency = time.Since(start)
	return result
}

func markRequestStart(onStart requestStartFunc) time.Time {
	start := time.Now()
	if onStart != nil {
		onStart(start)
	}
	return start
}

func verifyGETValues(cfg *runConfig) bool {
	if cfg.scan {
		return false
	}
	return cfg.keyMode == keyModeMixed || cfg.sequentialMax == 0
}

func newHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 20 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:          10_000,
			MaxIdleConnsPerHost:   10_000,
			MaxConnsPerHost:       0,
			IdleConnTimeout:       30 * time.Second,
			ForceAttemptHTTP2:     false,
			DisableCompression:    true,
			ResponseHeaderTimeout: 15 * time.Second,
			TLSHandshakeTimeout:   5 * time.Second,
			ExpectContinueTimeout: 0,
			DialContext: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}
}

func ensureServerReachable(parent context.Context, client *http.Client, healthURL string) error {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, healthURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health returned %s", resp.Status)
	}
	return nil
}

func (r *runner) runError(ctx context.Context, failures *failureStats, fallback error) error {
	if failures.Triggered() {
		return fmt.Errorf("%w: %d consecutive request failures", errFailureThreshold, StopThreshold)
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return fallback
}

func newPhaseTracker() *phaseTracker {
	return &phaseTracker{done: make(chan struct{})}
}

func (p *phaseTracker) Add() {
	if p.schedulingDone.Load() {
		panic("phase tracker: add after scheduling finished")
	}
	p.pending.Add(1)
	if p.schedulingDone.Load() {
		panic("phase tracker: scheduling finished during add")
	}
}

func (p *phaseTracker) Done() {
	remaining := p.pending.Add(-1)
	if remaining < 0 {
		panic("phase tracker: negative pending count")
	}
	if remaining == 0 && p.schedulingDone.Load() {
		p.closeDone()
	}
}

func (p *phaseTracker) FinishScheduling() {
	p.schedulingDone.Store(true)
	if p.pending.Load() == 0 {
		p.closeDone()
	}
}

func (p *phaseTracker) closeDone() {
	p.closeOnce.Do(func() {
		close(p.done)
	})
}

func (p *phaseTracker) Wait(ctx context.Context) bool {
	select {
	case <-p.done:
		return true
	case <-ctx.Done():
		return false
	}
}

func newDeadlineWaiter() *deadlineWaiter {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	return &deadlineWaiter{timer: timer}
}

func (w *deadlineWaiter) Wait(ctx context.Context, deadline time.Time) bool {
	delay := time.Until(deadline)
	if delay <= 0 {
		return ctx.Err() == nil
	}
	w.timer.Reset(delay)
	select {
	case <-w.timer.C:
		return true
	case <-ctx.Done():
		w.stopAndDrain()
		return false
	}
}

func (w *deadlineWaiter) Stop() {
	w.stopAndDrain()
}

func (w *deadlineWaiter) stopAndDrain() {
	if !w.timer.Stop() {
		select {
		case <-w.timer.C:
		default:
		}
	}
}
