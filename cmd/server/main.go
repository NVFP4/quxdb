package main

import (
	"context"
	"fmt"
	"os/signal"
	"syscall"

	"github.com/yashgorana/quxdb/pkg/server"
)

func main() {
	// Create a cancellable context
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer stop()

	dbServer := server.NewServer(server.NewConfig())

	if err := dbServer.StartWithContext(ctx); err != nil {
		fmt.Println(err)
	}

	fmt.Println("bye!")
}
