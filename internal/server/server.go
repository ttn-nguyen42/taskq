package server

import (
	"log/slog"
	"net/http"

	"github.com/ttn-nguyen42/taskq/internal/broker"
	"github.com/ttn-nguyen42/taskq/internal/queue"
	"github.com/ttn-nguyen42/taskq/internal/state"
)

type Options struct {
	Addr   string
	Logger *slog.Logger
}

type runtime struct {
	logger *slog.Logger
	st     state.Store
	br     broker.Broker
	q      queue.MessageQueue
}

type Server struct {
	opts    *Options
	logger  *slog.Logger
	sm      *http.ServeMux
	hs      *http.Server
	runtime *runtime
}

func NewServer(opts *Options, st state.Store, br broker.Broker, qu queue.MessageQueue) *Server {
	o := defaultOpts(opts)

	s := &Server{
		logger: o.Logger,
		opts:   o,
		sm:     http.NewServeMux(),
		runtime: &runtime{
			st:     st,
			br:     br,
			q:      qu,
			logger: o.Logger,
		},
	}

	s.registerV1()

	hs := http.Server{
		Addr:    o.Addr,
		Handler: s.sm,
	}
	s.hs = &hs

	return s
}

func defaultOpts(opts *Options) *Options {
	o := &Options{
		Addr:   ":8080",
		Logger: slog.Default(),
	}

	if len(opts.Addr) > 0 {
		o.Addr = opts.Addr
	}

	return o
}

func (s *Server) registerV1() {
	s.sm.HandleFunc("POST /api/v1/tasks", submitTask(s.runtime))
	s.sm.HandleFunc("POST /api/v1/queues", registerQueue(s.runtime))
}

func (s *Server) Run() error {
	go func() {
		s.logger.
			With("addr", s.opts.Addr).
			Info("server is running")

		err := s.hs.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			s.logger.
				With("err", err).
				Error("failed to run server")
			return
		}
	}()

	return nil
}

func (s *Server) Close() error {
	s.logger.Info("server is closing")
	return s.hs.Close()
}
