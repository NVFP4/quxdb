package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"

	"github.com/yashgorana/quxdb/pkg/config"
	"github.com/yashgorana/quxdb/pkg/quxdb"
	"github.com/yashgorana/quxdb/pkg/server"
	"github.com/yashgorana/quxdb/pkg/version"
)

// Config holds every setting the server binary loads.
type Config struct {
	HTTP server.Config `yaml:"http"`
	Log  LogConfig     `yaml:"log"`
	DB   quxdb.Options `yaml:",inline"`
}

// LogConfig holds the stderr logger settings.
type LogConfig struct {
	Level slog.Level `yaml:"level"`
	Time  bool       `yaml:"time"`
}

func main() {
	flags := flag.NewFlagSet(version.AppName, flag.ExitOnError)
	showVersion := flags.Bool("version", false, "print the version and exit")
	cfg := Config{
		HTTP: server.DefaultConfig(),
		Log:  LogConfig{Level: slog.LevelInfo, Time: true},
		DB:   quxdb.DefaultOptions(),
	}
	if err := config.Load(&cfg, "QUXDB", flags, os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(-1)
	}
	if *showVersion {
		fmt.Println(version.DetailedWithApp)
		return
	}

	logger := newLogger(cfg.Log)
	// routes dependencies' log and slog output through the same handler
	slog.SetDefault(logger)
	logger.Info("starting", "version", version.Detailed)
	logger.Info("config",
		"logLevel", cfg.Log.Level,
		"addr", cfg.HTTP.Addr(),
		"dataDir", cfg.DB.DataDir, "maxBatchRequests", cfg.DB.MaxBatchRequests,
		"memtableType", cfg.DB.Memtable.Type, "memtableCapacityBytes", cfg.DB.Memtable.CapacityBytes,
		"walSegmentBytes", cfg.DB.WAL.SegmentBytes, "walCorruptionPolicy", cfg.DB.WAL.CorruptionPolicy,
		"l0FileTarget", cfg.DB.Compaction.L0FileTarget, "levelFanout", cfg.DB.Compaction.LevelFanout)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	cfg.DB.Logger = logger
	qdb, err := quxdb.NewWithOptions(cfg.DB)
	if err != nil {
		logger.Error("open db failed", "err", err)
		os.Exit(-1)
	}
	// recovery finishes before the listener accepts requests
	if err := qdb.Start(ctx); err != nil {
		logger.Error("start db failed", "err", err)
		os.Exit(-1)
	}

	serveErr := server.New(cfg.HTTP, qdb, logger).StartWithContext(ctx)

	// the server has drained, so no request is still using the db
	stopCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := errors.Join(serveErr, qdb.Stop(stopCtx)); err != nil {
		logger.Error("shutdown failed", "err", err)
		os.Exit(-1)
	}
	logger.Info("shutdown complete")
}

// newLogger builds the stderr logger: colored for a terminal, logfmt for journald and log shippers.
func newLogger(c LogConfig) *slog.Logger {
	opts := &slog.HandlerOptions{Level: c.Level}
	if !c.Time {
		opts.ReplaceAttr = func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				return slog.Attr{}
			}
			return a
		}
	}
	if !isatty.IsTerminal(os.Stderr.Fd()) {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(tint.NewTextHandler(os.Stderr, &tint.Options{
		Level:       c.Level,
		ReplaceAttr: opts.ReplaceAttr,
	}))
}
