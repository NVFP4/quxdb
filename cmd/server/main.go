package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yashgorana/quxdb/pkg/server"
	"github.com/yashgorana/quxdb/pkg/version"
)

func main() {
	// Create a cancellable context
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	fmt.Printf("%s\n", version.DetailedWithApp)

	dbServer, err := server.NewServer(server.NewConfig())
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(-1)
	}

	if err := dbServer.StartWithContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(-1)
	}

	fmt.Println("bye!")
}
