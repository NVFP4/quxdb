package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/yashgorana/quxdb/pkg/quxdb"
)

// Server serves a DB over http.
type Server struct {
	config Config
	db     *quxdb.DB
	log    *slog.Logger
}

// New creates a server for db, which the caller starts before serving and stops after.
func New(config Config, db *quxdb.DB, logger *slog.Logger) *Server {
	return &Server{
		config: config,
		db:     db,
		log:    logger.With("mod", "server"),
	}
}

func (s *Server) StartWithContext(ctx context.Context) error {
	addr := s.config.Addr()

	srv := &http.Server{
		Addr:              addr,
		Handler:           setupHttpRoutes(s.db, s.log),
		ErrorLog:          slog.NewLogLogger(s.log.Handler(), slog.LevelError),
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
		TLSConfig: &tls.Config{
			MinVersion:               tls.VersionTLS13,
			PreferServerCipherSuites: true,
			CurvePreferences:         []tls.CurveID{tls.X25519, tls.CurveP256},
		},
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("quxdb server: %w", err)
	}

	// buffered so Serve's return after Shutdown doesn't block
	errCh := make(chan error, 1)
	go func() {
		s.log.Info("listening", "addr", "http://"+addr)
		errCh <- srv.Serve(ln)
	}()

	select {
	case <-ctx.Done():
		s.log.Info("shutting down")

		stopCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := srv.Shutdown(stopCtx); err != nil {
			_ = srv.Close()
			return err
		}
		s.log.Info("stopped")
		return nil
	case err := <-errCh:
		return err
	}
}
