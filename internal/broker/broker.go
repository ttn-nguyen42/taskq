package broker

import (
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

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
	// By default, it is 30s
	ExtendLease(ids ...string) (err error)

	// Retry moves a task from in-progress to retry queue.
	// It is used when a task fails to process.
	Retry(id string, reason string) (err error)

	// Get returns the information of a task.
	Get(id string) (t *Task, err error)

	// RegisterQueue registers a queue.
	RegisterQueue(name string, priority int) (queueId string, err error)

	// DeregisterQueue deregisters a queue.
	DeregisterQueue(name string) (err error)
}

type broker struct {
	logger *slog.Logger
	q      queue.MessageQueue
	state  state.Store

	rec *reconciler

	mu       sync.RWMutex
	canceled map[string]utils.Empty
	queues   map[string]utils.Empty
}

func New(logger *slog.Logger, q queue.MessageQueue, s state.Store) (Broker, error) {
	b := &broker{
		logger:   logger,
		q:        q,
		state:    s,
		canceled: make(map[string]utils.Empty),
	}

	if err := b.pullCache(); err != nil {
		return nil, err
	}

	rec := newReconciler(
		logger,
		b,
		q,
		b.rec.queues,
		1*time.Second)

	b.rec = rec

	return b, nil
}

func (b *broker) pullCache() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	var skip uint64 = 0
	var limit uint64 = 100

	for {
		queueInfos, err := b.state.ListQueues(skip, limit)
		if err != nil {
			b.logger.
				With("err", err).
				Error("failed to list queues")
			log.Fatalf("failed to list queues: %v", err)
		}

		if len(queueInfos) == 0 {
			break
		}

		for _, qi := range queueInfos {
			b.queues[qi.Name] = utils.Empty{}
		}

		skip += uint64(len(queueInfos))
	}

	skip = 0

	for {
		tasks, err := b.state.ListInfo(skip, limit)
		if err != nil {
			b.logger.
				With("err", err).
				Error("failed to list canceled tasks")
			log.Fatalf("failed to list canceled tasks: %v", err)
		}

		if len(tasks) == 0 {
			break
		}

		for _, t := range tasks {
			if t.Status == state.TaskStatusCanceled {
				b.canceled[t.ID] = utils.Empty{}
			}
		}

		skip += uint64(len(tasks))
	}

	return nil
}

func (b *broker) Run() error {
	b.mu.RLock()
	rec := b.rec
	b.mu.RUnlock()

	if rec == nil {
		return fmt.Errorf("broker is not started")
	}

	rec.Watch()
	return nil
}

func (b *broker) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.rec == nil {
		return
	}

	b.rec.Stop()
	b.rec = nil
}
