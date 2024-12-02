package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ttn-nguyen42/taskq"
)

func main() {
	tq := taskq.NewTaskq(&taskq.Options{})

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGTERM,
	)
	defer stop()

	go func() {
		err := tq.Run()
		if err != nil {
			stop()
		}
	}()

	<-ctx.Done()

	tq.Close()

	os.Exit(0)
}
