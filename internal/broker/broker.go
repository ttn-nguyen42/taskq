package broker

import (
	"context"
	"log/slog"
	"sync"

	"github.com/ttn-nguyen42/taskq/internal/queue"
	"github.com/ttn-nguyen42/taskq/internal/state"
	"github.com/ttn-nguyen42/taskq/internal/utils"
)

type Broker interface {
	// Submit submits a task into the system.
	// It create a record of that task and submits it into a message queue for ordered consumption.
	Submit(t *Task) (id string, err error)

	// Move moves a message from one queue to another.
	// Can can be use to implement a priority system (moving between a lower and higher prioritized queue).
	Move(id string, from string, to string) (err error)

	// Cancel cancels a task.
	// If it is in pending state, the task simply removed from the queue and database.
	// Else, if it is in progress, it will be marked as cancelled and the worker will be notified, the context will be canceled in the function.
	Cancel(id string) (err error)

	// Acquire returns a list of task in pending, or ready to be retried.
	//
	// It pops the queues.
	Acquire(queue string, count int) (tasks []*Task, err error)

	// ExtendLease allows a client to extend the lease on a task.
	// After the lease, the task will be visible to other clients via Acquire.
	//
	// By default, it is 30s.
	ExtendLease(ids ...string) (err error)

	// Get returns the information of a task.
	Get(id string) (t *Task, err error)
}

type broker struct {
	logger *slog.Logger
	q      queue.MessageQueue
	state  state.Store

	mu       sync.RWMutex
	canceled map[string]utils.Empty
}

func New(ctx context.Context, logger *slog.Logger, q queue.MessageQueue, s state.Store) (Broker, error) {
	b := &broker{
		logger:   logger,
		q:        q,
		state:    s,
		canceled: make(map[string]utils.Empty),
	}

	if err := b.pullCache(ctx); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *broker) pullCache(_ context.Context) error {
	return nil
}
