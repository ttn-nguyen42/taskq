package taskq

import (
	"log"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/ttn-nguyen42/taskq/internal/broker"
	"github.com/ttn-nguyen42/taskq/internal/queue"
	"github.com/ttn-nguyen42/taskq/internal/server"
	"github.com/ttn-nguyen42/taskq/internal/state"
	"github.com/ttn-nguyen42/taskq/internal/utils"
	"github.com/ttn-nguyen42/taskq/pkg/queues/bq"
)

type Taskq struct {
	opts *Options

	stop chan utils.Empty

	logger *slog.Logger

	br broker.Broker
	qu queue.MessageQueue
	st state.Store

	hs *server.Server
}

func NewTaskq(opts *Options) *Taskq {
	o := DefaultOptions(opts)

	logger := slog.NewTextHandler(
		os.Stdout,
		&slog.HandlerOptions{
			Level: slog.LevelDebug,
		},
	)

	tq := &Taskq{
		opts:   o,
		logger: slog.New(logger),
		stop:   make(chan utils.Empty, 1),
	}
	if err := tq.init(); err != nil {
		tq.logger.
			With("err", err).
			Error("failed to initialize taskq")
		log.Fatalf("failed to initialize taskq: %v", err)
	}

	return tq
}

func (t *Taskq) init() error {
	t.mkdir(t.opts.QueuePath)
	q, err := bq.NewQueue(&bq.Options{
		Logger: t.logger,
		Path:   t.opts.QueuePath,
	})
	if err != nil {
		t.logger.
			With("err", err).
			Error("failed to create queue")
		log.Fatalf("failed to create queue: %v", err)
	}
	t.qu = q

	t.mkdir(t.opts.StatePath)
	st, err := state.NewStore(&state.StoreOpts{
		Logger: t.logger,
		Path:   t.opts.StatePath,
	})
	if err != nil {
		t.logger.
			With("err", err).
			Error("failed to create state store")
		log.Fatalf("failed to create state store: %v", err)
	}
	t.st = st

	t.br, err = broker.New(t.logger, t.qu, t.st)
	if err != nil {
		t.logger.
			With("err", err).
			Error("failed to create broker")
		log.Fatalf("failed to create broker: %v", err)
	}

	s := server.NewServer(&server.Options{
		Addr: t.opts.Addr,
	},
		t.st,
		t.br,
		t.qu,
	)
	t.hs = s

	return nil
}

func (t *Taskq) mkdir(path string) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.logger.
			With("err", err).
			Error("failed to create directory")
		log.Fatalf("failed to create directory: %v", err)
	}
	t.logger.
		With("dir", dir).
		Info("directory created")
}

func (t *Taskq) Run() error {
	if err := t.br.Run(); err != nil {
		t.logger.
			With("err", err).
			Error("failed to run broker")
		return err
	}

	if err := t.hs.Run(); err != nil {
		t.logger.
			With("err", err).
			Error("failed to run server")
		return err
	}

	<-t.stop

	t.logger.Info("taskq is stopping")
	if err := t.hs.Close(); err != nil {
		t.logger.
			With("err", err).
			Error("failed to close server")
	}

	if err := t.st.Close(); err != nil {
		t.logger.
			With("err", err).
			Error("failed to close state store")
	}

	t.br.Stop()

	if err := t.qu.Close(); err != nil {
		t.logger.
			With("err", err).
			Error("failed to close queue")
	}

	t.logger.Info("taskq is stopped")

	return nil
}

func (t *Taskq) Close() {
	t.stop <- utils.Empty{}
}
