package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	neturl "net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/spf13/cobra"
)

const (
	BaseURL      = "http://127.0.0.1:7129/"
	TestDuration = 10 * time.Second
)

var (
	metricTitle = color.New(color.FgHiMagenta, color.Bold).SprintFunc()
	metricOp    = color.New(color.FgCyan, color.Bold).SprintFunc()
	metricLabel = color.New(color.Faint).SprintFunc()
	metricGood  = color.New(color.FgGreen).SprintFunc()
	metricErr   = color.New(color.FgRed).SprintFunc()
	metricWarn  = color.New(color.FgYellow).SprintFunc()
	metricPct   = color.New(color.FgMagenta).SprintFunc()
)

type stressFlags struct {
	seqKeys   bool
	isoKeys   bool
	uuid4Keys bool
	ulidKeys  bool
	mixedKeys bool
}

func main() {
	cfg := runConfig{
		duration: TestDuration,
		workers:  runtime.NumCPU(),
		url:      BaseURL,
		keyMode:  keyModeUUID7,
	}
	stress := stressFlags{}

	rootCmd := &cobra.Command{
		Use:           "quxdb-stress",
		Short:         "Run an open-loop load test against QuxDB",
		Args:          cobra.NoArgs,
		SilenceErrors: true,
		SilenceUsage:  true,
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg.scan = false
			cfg.keyMode = stress.keyMode()
			resolveQueueDefault(cmd, &cfg)
			if err := validateRunConfig(&cfg); err != nil {
				return err
			}
			if err := validateStressConfig(cmd, cfg); err != nil {
				return err
			}
			runner, err := newRunner(cmd.Context(), &cfg)
			if err != nil {
				return err
			}
			return runner.start(cmd.Context())
		},
	}

	scanCmd := &cobra.Command{
		Use:          "scan",
		Short:        "Run open-loop full-scan GET requests against /k/",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg.scan = true
			resolveQueueDefault(cmd, &cfg)
			if err := validateRunConfig(&cfg); err != nil {
				return err
			}
			runner, err := newRunner(cmd.Context(), &cfg)
			if err != nil {
				return err
			}
			return runner.start(cmd.Context())
		},
	}

	addCommonFlags(rootCmd, &cfg)
	addStressFlags(rootCmd, &stress, &cfg)
	rootCmd.AddCommand(scanCmd)

	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			fmt.Printf("%s run canceled\n", metricWarn("STOPPED"))
			os.Exit(130)
		}
		logError("%v", err)
		os.Exit(1)
	}
}

func addCommonFlags(cmd *cobra.Command, cfg *runConfig) {
	flags := cmd.PersistentFlags()
	flags.DurationVarP(&cfg.duration, "duration", "d", cfg.duration, "how long to generate load (example: 10s, 2m)")
	flags.Uint64VarP(&cfg.requests, "requests", "n", cfg.requests, "number of PUT requests to schedule (scan: GET requests)")
	flags.Float64VarP(&cfg.rate, "rate", "r", cfg.rate, "target total request rate in requests/second (required)")
	flags.IntVarP(&cfg.workers, "workers", "w", cfg.workers, "maximum concurrent HTTP requests")
	flags.IntVar(&cfg.queue, "queue", cfg.queue, "maximum queued requests (default: workers)")
	flags.DurationVar(&cfg.warmup, "warmup", cfg.warmup, "unmeasured warm-up duration")
	flags.StringVarP(&cfg.url, "url", "u", cfg.url, "base URL (example: http://localhost:7129)")
	cmd.MarkFlagsMutuallyExclusive("duration", "requests")
}

func addStressFlags(cmd *cobra.Command, stress *stressFlags, cfg *runConfig) {
	flags := cmd.Flags()
	flags.BoolVar(&stress.seqKeys, "kseq", stress.seqKeys, "use sequential keys for PUT requests")
	flags.Uint64Var(&cfg.sequentialMax, "kseqmax", cfg.sequentialMax, "maximum sequential key before wrapping to 1 (applies to kseq and kall)")
	flags.BoolVar(&stress.isoKeys, "kts", stress.isoKeys, "use unique fixed-width RFC3339 nanosecond timestamp keys for PUT requests")
	flags.BoolVar(&stress.uuid4Keys, "kuuid4", stress.uuid4Keys, "use UUIDv4 keys for PUT requests")
	flags.BoolVar(&stress.ulidKeys, "kulid", stress.ulidKeys, "use ULID keys for PUT requests")
	flags.BoolVar(&stress.mixedKeys, "kall", stress.mixedKeys, "use an equal mix of sequential, RFC3339, UUIDv4, UUIDv7, and ULID keys for PUT requests")
	flags.BoolVar(&cfg.keyPrefix, "kprefix", cfg.keyPrefix, "prefix keys with uuid4/, uuid7/, ulid/, seq/, or ts/")
	flags.BoolVar(&cfg.timestampValue, "vts", cfg.timestampValue, "send Unix nanosecond timestamp values instead of JSON")
	flags.BoolVar(&cfg.noGET, "no-get", cfg.noGET, "disable random GET requests")
	cmd.MarkFlagsMutuallyExclusive("kseq", "kts", "kuuid4", "kulid", "kall")
}

func (f stressFlags) keyMode() keyMode {
	switch {
	case f.seqKeys:
		return keyModeSequential
	case f.isoKeys:
		return keyModeISO
	case f.uuid4Keys:
		return keyModeUUID4
	case f.ulidKeys:
		return keyModeULID
	case f.mixedKeys:
		return keyModeMixed
	default:
		return keyModeUUID7
	}
}

func validateRunConfig(cfg *runConfig) error {
	if cfg.duration <= 0 {
		return fmt.Errorf("duration must be > 0")
	}
	if math.IsNaN(cfg.rate) || math.IsInf(cfg.rate, 0) || cfg.rate <= 0 {
		return fmt.Errorf("rate must be a finite number > 0")
	}
	if cfg.rate > float64(time.Second) {
		return fmt.Errorf("rate must be <= %d requests/second", time.Second)
	}
	if cfg.workers <= 0 {
		return fmt.Errorf("workers must be > 0")
	}
	if cfg.queue < 0 {
		return fmt.Errorf("queue must be >= 0")
	}
	if cfg.warmup < 0 {
		return fmt.Errorf("warmup must be >= 0")
	}
	normalizedURL, err := normalizeBaseURL(cfg.url)
	if err != nil {
		return err
	}
	cfg.url = normalizedURL
	return nil
}

func resolveQueueDefault(cmd *cobra.Command, cfg *runConfig) {
	if flag := cmd.Flag("queue"); flag != nil && !flag.Changed {
		cfg.queue = cfg.workers
	}
}

func validateStressConfig(cmd *cobra.Command, cfg runConfig) error {
	if cmd.Flag("kseqmax").Changed && cfg.keyMode != keyModeSequential && cfg.keyMode != keyModeMixed {
		return fmt.Errorf("kseqmax requires kseq or kall")
	}
	return nil
}

func normalizeBaseURL(raw string) (string, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", fmt.Errorf("url must not be empty")
	}
	if !strings.Contains(s, "://") {
		s = "http://" + s
	}
	u, err := neturl.Parse(s)
	if err != nil {
		return "", fmt.Errorf("invalid url: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("url must include scheme and host")
	}
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimRight(u.String(), "/"), nil
}

func logError(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "%s %s\n", metricErr("ERR"), fmt.Sprintf(format, args...))
}

func logWarning(title string, details ...string) {
	line := metricWarn("WARN") + " " + metricWarn(title)
	if len(details) > 0 {
		line += ": " + strings.Join(details, " ")
	}
	fmt.Println(line)
}

func logRunFinished(cfg runConfig) {
	reason := fmt.Sprintf("duration reached (%s)", cfg.duration)
	if cfg.requests > 0 {
		unit := "PUT"
		if cfg.scan {
			unit = "scan"
		}
		reason = fmt.Sprintf("%s request count reached (%s)", unit, formatCount(cfg.requests))
	}
	fmt.Printf("%s %s\n", metricGood("DONE"), reason)
}

func logRunConfig(cfg runConfig, verifyValue bool) {
	fmt.Println(formatRunConfig(cfg, verifyValue))
}

func formatRunConfig(cfg runConfig, verifyValue bool) string {
	run := cfg.duration.String()
	if cfg.requests > 0 {
		unit := "PUTs"
		if cfg.scan {
			unit = "scans"
		}
		run = formatCount(cfg.requests) + " " + unit
	}
	if cfg.warmup > 0 {
		run = cfg.warmup.String() + " warm-up + " + run
	}
	parts := []string{
		cfg.url,
		formatRate(cfg.rate) + " req/s",
		fmt.Sprintf("%s workers, queue %s", formatCount(uint64(cfg.workers)), formatCount(uint64(cfg.queue))),
		run,
	}
	if cfg.scan {
		parts = append(parts, "GET /k/ scans")
	} else {
		mix := fmt.Sprintf("%d%% PUT / %d%% GET", 100-ReadPercent, ReadPercent)
		if cfg.noGET {
			mix = "PUT only"
		}
		keys := keyModeName(cfg.keyMode) + " keys"
		if cfg.sequentialMax > 0 {
			keys += fmt.Sprintf(" (seq wraps at %s)", formatCount(cfg.sequentialMax))
		}
		if cfg.keyPrefix {
			keys += " (prefixed)"
		}
		values := "JSON values"
		if cfg.timestampValue {
			values = "unix-nano values"
		}
		parts = append(parts, mix, keys, values)
		if !cfg.noGET {
			verify := "verify GET status"
			if cfg.keyMode == keyModeMixed && cfg.sequentialMax > 0 {
				verify = "verify GET value except wrapping seq keys"
			} else if verifyValue {
				verify = "verify GET value"
			}
			parts = append(parts, verify)
		}
	}
	return metricTitle("quxdb-stress") + " " + strings.Join(parts, " · ")
}

func keyModeName(mode keyMode) string {
	switch mode {
	case keyModeUUID4:
		return "UUIDv4"
	case keyModeUUID7:
		return "UUIDv7"
	case keyModeULID:
		return "ULID"
	case keyModeSequential:
		return "sequential"
	case keyModeISO:
		return "RFC3339"
	case keyModeMixed:
		return "mixed seq/RFC3339/UUIDv4/UUIDv7/ULID"
	default:
		return string(mode)
	}
}

type reportOperation struct {
	name  string
	stats operationSnapshot
}

// logWindowStats prints one line for the completions observed in a reporting window.
func logWindowStats(at time.Duration, snapshot statsSnapshot, cfg runConfig) {
	prefix := metricLabel(fmt.Sprintf("[%7s]", formatDuration(at.Round(time.Millisecond))))
	operations := reportOperations(snapshot, cfg)
	if len(operations) == 0 {
		fmt.Printf("%s %s\n", prefix, metricLabel("no completions"))
		return
	}
	segments := make([]string, 0, len(operations)+1)
	for _, operation := range operations {
		segment := metricOp(operation.name) + " " + metricGood(formatRate(successRate(operation.stats, snapshot.elapsed))+"/s")
		if outcomes := formatFailures(operation.stats, false); outcomes != "" {
			segment += " " + outcomes
		}
		if latency := operation.stats.endToEnd; latency.count > 0 {
			segment += " " + percentile("p50", latency.p50) + " " + percentile("p99", latency.p99) + " " + percentile("p999", latency.p999)
		}
		segments = append(segments, segment)
	}
	if snapshot.dispatch.count > 0 {
		segments = append(segments, metricLabel("start delay ")+percentile("p99", snapshot.dispatch.p99))
	}
	fmt.Printf("%s %s\n", prefix, strings.Join(segments, metricLabel(" │ ")))
}

// logFinalStats prints one line per operation, then one line each for totals and delivery.
func logFinalStats(snapshot statsSnapshot, cfg runConfig, timing runTiming) {
	fmt.Println()
	operations := reportOperations(snapshot, cfg)
	var successful uint64
	for _, operation := range operations {
		stats := operation.stats
		successful += stats.successCount()
		line := fmt.Sprintf("%s %s req · %s",
			metricOp(fmt.Sprintf("%-8s", operation.name)),
			formatCount(stats.totalCount()),
			metricGood(formatRate(successRate(stats, snapshot.elapsed))+"/s ok"))
		if failures := formatFailures(stats, true); failures != "" {
			line += " · " + failures
		}
		if latency := stats.endToEnd; latency.count > 0 {
			line += " · " + percentile("p50", latency.p50) + " " + percentile("p95", latency.p95) + " " +
				percentile("p99", latency.p99) + " " + percentile("p999", latency.p999) +
				" · http " + percentile("p99", stats.service.p99)
		}
		fmt.Println(line)
	}

	requests := formatCount(snapshot.scheduled) + " req"
	if snapshot.started != snapshot.scheduled || snapshot.finished != snapshot.scheduled {
		requests = fmt.Sprintf("%s planned, %s started, %s finished",
			formatCount(snapshot.scheduled), formatCount(snapshot.started), formatCount(snapshot.finished))
	}
	okRate := float64(0)
	if snapshot.elapsed > 0 {
		okRate = float64(successful) / snapshot.elapsed.Seconds()
	}
	wall := fmt.Sprintf("wall %s (load %s, late starts %s, drain %s",
		formatDuration(timing.wall), formatDuration(timing.active),
		formatDuration(timing.arrivalOverrun), formatDuration(timing.completionDrain))
	if timing.warmup > 0 {
		wall += ", +" + formatDuration(timing.warmup) + " warm-up"
	}
	fmt.Printf("%s %s · %s · %s)\n", metricOp(fmt.Sprintf("%-8s", "total")), requests,
		metricGood(formatRate(okRate)+"/s ok"), wall)

	startWindow := timing.active + timing.arrivalOverrun
	startRate := float64(0)
	if startWindow > 0 {
		startRate = float64(snapshot.started) / startWindow.Seconds()
	}
	queueBlocked := "none"
	if snapshot.enqueueWait > 0 {
		queueBlocked = fmt.Sprintf("%s (max %s)", formatDuration(snapshot.enqueueWait), formatDuration(snapshot.maxEnqueueWait))
	}
	fmt.Printf("%s starts %s/s of %s/s · start delay %s %s · backlog peak %s · queue blocked %s · workers %.1f%% busy, peak %s/%s\n",
		metricOp(fmt.Sprintf("%-8s", "delivery")),
		formatRate(startRate), formatRate(cfg.rate),
		percentile("p50", snapshot.dispatch.p50), percentile("p99", snapshot.dispatch.p99),
		formatCount(snapshot.maxBacklog), queueBlocked,
		100*snapshot.HTTPWorkerUtilization(cfg.workers),
		formatCount(uint64(max(snapshot.maxInFlight, 0))), formatCount(uint64(cfg.workers)))
}

// percentile renders a latency percentile with its label highlighted, e.g. "p99 1.23ms".
func percentile(label string, latency time.Duration) string {
	return metricPct(label) + " " + formatDuration(latency)
}

func successRate(operation operationSnapshot, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(operation.successCount()) / elapsed.Seconds()
}

// formatFailures summarizes non-success outcomes, e.g. "err 12 (0.10%: 500×10, no response×2) · canceled 3".
func formatFailures(operation operationSnapshot, withRate bool) string {
	failures := operation.failureCount()
	canceled := operation.canceledCount()
	parts := make([]string, 0, 2)
	if failures > 0 {
		groups := make([]string, 0, len(operation.outcomes))
		for _, outcome := range operation.outcomes {
			if outcome.response.cause != causeSuccess && outcome.response.cause != causeCanceled {
				groups = append(groups, responseOutcomeLabel(outcome.response)+"×"+formatCount(outcome.count))
			}
		}
		detail := strings.Join(groups, ", ")
		if withRate {
			detail = fmt.Sprintf("%.2f%%: %s", 100*float64(failures)/float64(failures+operation.successCount()), detail)
		}
		parts = append(parts, metricErr(fmt.Sprintf("err %s (%s)", formatCount(failures), detail)))
	}
	if canceled > 0 {
		parts = append(parts, metricWarn("canceled "+formatCount(canceled)))
	}
	return strings.Join(parts, " · ")
}

func responseOutcomeLabel(response responseOutcome) string {
	switch response.cause {
	case causeHTTPStatus:
		return strconv.Itoa(response.statusCode)
	case causeVerification:
		return "value mismatch"
	case causeBodyRead:
		return "body read"
	case causeClient:
		if response.class == responseHTTP {
			return strconv.Itoa(response.statusCode) + " client error"
		}
		return "no response"
	case causeRequestBuild:
		return "not sent"
	default:
		return "unknown"
	}
}

func reportOperations(snapshot statsSnapshot, cfg runConfig) []reportOperation {
	operations := make([]reportOperation, 0, 2)
	if snapshot.put.totalCount() > 0 {
		operations = append(operations, reportOperation{name: "PUT", stats: snapshot.put})
	}
	if snapshot.get.totalCount() > 0 {
		name := "GET"
		if cfg.scan {
			name = "SCAN"
		}
		operations = append(operations, reportOperation{name: name, stats: snapshot.get})
	}
	return operations
}

func logBottleneckWarning(cfg runConfig, final statsSnapshot, timing runTiming) {
	if final.scheduled == 0 || timing.wall <= 0 {
		return
	}
	lagLimit := rateLagThreshold(cfg.rate)
	backlogLimit := max(uint64(2), uint64(math.Ceil(cfg.rate*lagLimit.Seconds())))
	deliveryBehind := timing.arrivalOverrun > lagLimit || (final.dispatch.p99 > lagLimit && final.maxBacklog > backlogLimit)
	backpressureFloor := max(lagLimit, timing.arrivalOverrun/2)
	deliveryPathBackpressured := final.enqueueWait >= backpressureFloor
	workerUtilization := final.HTTPWorkerUtilization(cfg.workers)
	workersBusy := workerUtilization >= 0.8
	startsEndedLate := timing.arrivalOverrun > lagLimit
	evidence := deliveryDelayEvidence(final, timing, startsEndedLate)

	if deliveryBehind {
		if workersBusy {
			title := "Request start-delay spikes detected"
			if startsEndedLate {
				title = "Request starts fell behind the target"
			}
			logWarning(title, evidence,
				fmt.Sprintf("HTTP slots averaged %.1f%% busy; server latency or too few --workers is likely.", 100*workerUtilization))
			return
		}
		if deliveryPathBackpressured {
			title := "Load generator showed delivery pressure"
			if startsEndedLate {
				title = "Load generator may be limiting delivery"
			}
			logWarning(title, evidence,
				fmt.Sprintf("Enqueue blocking totaled %s · HTTP slots averaged %.1f%% busy.", formatDuration(final.enqueueWait), 100*workerUtilization),
				"Check client CPU/GC and request generation, or run the generator on a separate host.")
			return
		}
		logWarning("Load generator or host scheduler may be limiting delivery",
			evidence,
			"The delay occurred without sustained queue pressure.")
		return
	}
	if final.dispatch.p99 > lagLimit && final.maxEnqueueWait < lagLimit && workerUtilization < 0.5 {
		logWarning("Load-generator or host scheduling jitter detected",
			fmt.Sprintf("Request start-delay p99 was %s · maximum enqueue block %s.", formatDuration(final.dispatch.p99), formatDuration(final.maxEnqueueWait)),
			"All requests still started by the end of the load period.")
	}
}

func deliveryDelayEvidence(final statsSnapshot, timing runTiming, startsEndedLate bool) string {
	if startsEndedLate {
		return fmt.Sprintf("Last request start was %s late · start-delay p99 %s.",
			formatDuration(timing.arrivalOverrun), formatDuration(final.dispatch.p99))
	}
	return fmt.Sprintf("Request start-delay p99 was %s · peak waiting to start was %s.",
		formatDuration(final.dispatch.p99), formatCount(final.maxBacklog))
}

func formatRate(rate float64) string {
	var value string
	switch {
	case rate == 0:
		value = "0.00"
	case math.Abs(rate) >= 1:
		value = fmt.Sprintf("%.2f", rate)
	case math.Abs(rate) >= 0.01:
		value = fmt.Sprintf("%.3f", rate)
	default:
		value = fmt.Sprintf("%.6g", rate)
	}
	return groupNumeric(value)
}

func formatCount(count uint64) string {
	return groupNumeric(strconv.FormatUint(count, 10))
}

func groupNumeric(value string) string {
	if strings.ContainsAny(value, "eE") {
		return value
	}
	sign := ""
	if strings.HasPrefix(value, "-") || strings.HasPrefix(value, "+") {
		sign = value[:1]
		value = value[1:]
	}
	integer, fraction, hasFraction := strings.Cut(value, ".")
	if len(integer) > 3 {
		var grouped strings.Builder
		first := len(integer) % 3
		if first == 0 {
			first = 3
		}
		grouped.WriteString(integer[:first])
		for index := first; index < len(integer); index += 3 {
			grouped.WriteByte(',')
			grouped.WriteString(integer[index : index+3])
		}
		integer = grouped.String()
	}
	if hasFraction {
		return sign + integer + "." + fraction
	}
	return sign + integer
}

func rateLagThreshold(rate float64) time.Duration {
	intervalNanos := float64(time.Second) / rate
	if math.IsInf(intervalNanos, 0) || intervalNanos >= float64(maxHistogramLatency)/2 {
		return maxHistogramLatency
	}
	return max(time.Millisecond, 2*time.Duration(intervalNanos))
}

func formatDuration(duration time.Duration) string {
	if duration < 0 {
		duration = 0
	}
	if duration == 0 {
		return "0s"
	}
	switch {
	case duration < time.Microsecond:
		return fmt.Sprintf("%dns", duration.Nanoseconds())
	case duration < time.Millisecond:
		return fmt.Sprintf("%.2fµs", float64(duration)/float64(time.Microsecond))
	case duration < time.Second:
		return fmt.Sprintf("%.2fms", float64(duration)/float64(time.Millisecond))
	default:
		return fmt.Sprintf("%.3fs", duration.Seconds())
	}
}
