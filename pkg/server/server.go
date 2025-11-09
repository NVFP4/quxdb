package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/yashgorana/quxdb/pkg/db"
	"github.com/yashgorana/quxdb/pkg/version"
)

type QuxServer struct {
	config QuxServerConfig
	db     *db.QuxDB
}

func NewServer(config QuxServerConfig) *QuxServer {
	return &QuxServer{
		config: config,
		db:     db.New(config.DataDir),
	}
}

func (s *QuxServer) StartWithContext(ctx context.Context) error {
	fmt.Printf("quxdb %s\n", version.Detailed)

	addr := s.config.Addr()

	srv := &http.Server{
		Addr:    addr,
		Handler: setupHttpRoutes(s.db),
		// Configs
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
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
		return fmt.Errorf("bind %q: %w", addr, err)
	}

	go func() {
		fmt.Printf("starting quxserver on http://%s\n", addr)
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
		fmt.Println("server context cancelled. stopping gracefully...")
		defer fmt.Println("server stopped")

		stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if err := srv.Shutdown(stopCtx); err != nil {
			_ = srv.Close() // force close if graceful shutdown fails
			return fmt.Errorf("failed to stop server: %w", err)
		}

		return nil
	case err := <-errCh:
		close(errCh)
		return err
	}
}
