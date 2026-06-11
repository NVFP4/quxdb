package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/yashgorana/quxdb/pkg/db"
)

type QuxServer struct {
	config QuxServerConfig
	db     *db.QuxDB
}

func NewServer(config QuxServerConfig) (*QuxServer, error) {
	db, err := db.New(config.DataDir)
	if err != nil {
		return nil, err
	}
	return &QuxServer{
		config: config,
		db:     db,
	}, nil
}

func (s *QuxServer) StartWithContext(ctx context.Context) error {
	addr := s.config.Addr()

	srv := &http.Server{
		Addr:    addr,
		Handler: setupHttpRoutes(s.db),
		// Configs
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MB
		TLSConfig: &tls.Config{
			// Force TLS 1.3 to ensure modern ciphers and better performance
			MinVersion:               tls.VersionTLS13,
			PreferServerCipherSuites: true,
			CurvePreferences:         []tls.CurveID{tls.X25519, tls.CurveP256},
		},
	}

	errCh := make(chan error, 1)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("quxdb server: %w", err)
	}

	go func() {
		fmt.Printf("quxdb server: http://%s\n", addr)
		if srvErr := srv.Serve(ln); srvErr != nil && srvErr != http.ErrServerClosed {
			errCh <- srvErr // http.Serve crashed
		}
	}()

	go func() {
		err := s.db.Start(ctx)
		if err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		var errs []error

		fmt.Println("server context cancelled")

		// give 60s for server to stop and drain connections
		stopCtx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		// first stop server
		if err := srv.Shutdown(stopCtx); err != nil {
			_ = srv.Close() // force close if graceful shutdown fails
			errs = append(errs, err)
		}
		fmt.Println("server stopped")

		if err := s.db.Stop(stopCtx); err != nil {
			errs = append(errs, err)
		}

		return errors.Join(errs...)
	case err := <-errCh:
		close(errCh)
		return err
	}
}
