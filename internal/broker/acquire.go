package broker

import (
	"fmt"

	errs "github.com/ttn-nguyen42/taskq/internal/errors"
	"github.com/ttn-nguyen42/taskq/internal/queue"
	"github.com/ttn-nguyen42/taskq/internal/state"
	"github.com/ttn-nguyen42/taskq/internal/utils"
)

func (b *broker) Acquire(qname string, count int) (tasks []*Task, err error) {
	if count <= 0 {
		return nil, fmt.Errorf("count must be greater than 0")
	}

	b.mu.RLock()
	_, exists := b.queues[qname]
	b.mu.RUnlock()

	if !exists {
		err := errs.NewErrNotFound("queue")
		b.logger.
			With("queue", qname).
			With("err", err).
			Error("failed to acquire tasks")
		return nil, err
	}
	tasks = make([]*Task, 0, count)

	for count > 0 {
		ts, err := b.fetchQueue(qname, count)
		if err != nil {
			return nil, err
		}
		count -= len(ts)
		if len(ts) == 0 {
			break
		}
	}

	return tasks, nil
}

func (b *broker) fetchQueue(qname string, count int) (tasks []*Task, err error) {
	tasks = make([]*Task, 0, count)

	opts := queue.DequeueOpts{
		Limit: count,
	}

	messages, err := b.q.Dequeue(&opts, qname)
	if err != nil {
		err = fmt.Errorf("failed to dequeue messages: %w", err)
		b.logger.
			With("queue", qname).
			With("err", err).
			Error("failed to acquire tasks")
		return
	}

	if len(messages) == 0 {
		return
	}

	tis, err := b.state.GetMultiInfo(b.listInfoIds(messages)...)
	if err != nil {
		err = fmt.Errorf("failed to retrieve task info: %w", err)
		b.logger.
			With("queue", qname).
			With("err", err).
			Error("failed to acquire tasks")
		return
	}

	b.mu.RLock()
	for _, t := range tis {
		_, alreadyCanceled := b.canceled[t.ID]

		if alreadyCanceled {
			continue
		}

		t := Task{
			Queue:       qname,
			Input:       t.Input,
			Timeout:     t.Timeout,
			MaxRetry:    t.MaxRetry,
			LastRetryAt: t.LastRetryAt,
			RetryCount:  t.RetryCount,
			Status:      t.Status,
			SubmittedAt: t.SubmittedAt,
		}
		tasks = append(tasks, &t)
	}
	b.mu.RUnlock()

	return
}

func (b *broker) listInfoIds(msgs queue.Messages) []string {
	ids := make([]string, 0, len(msgs))

	for _, msg := range msgs {
		var met MessageMetadata

		err := msg.Into(&met)
		if err != nil {
			b.logger.
				With("err", err).
				Error("failed to unmarshal message")
			continue
		}

		ids = append(ids, met.TaskID)
	}

	return ids
}

func (b *broker) Cancel(id string) (err error) {
	if len(id) == 0 {
		return fmt.Errorf("task id is required")
	}

	err = b.markTaskAsCanceled(id)
	if err != nil {
		b.logger.
			With("task_id", id).
			With("err", err).
			Error("failed to cancel task")
		return
	}

	b.cacheCanceledTask(id)

	return nil
}

func (b *broker) markTaskAsCanceled(id string) (err error) {
	alreadyCanceled := false

	upd := func(t *state.TaskInfo) bool {
		if t.Status == state.TaskStatusCanceled {
			alreadyCanceled = true
			return false
		}
		t.Status = state.TaskStatusCanceled
		return true
	}

	found, err := b.state.UpdateInfo(id, upd)
	if err != nil {
		return fmt.Errorf("failed to update task info: %w", err)
	}

	if !found {
		return errs.NewErrNotFound("task")
	}

	if alreadyCanceled {
		return nil
	}

	return
}

func (b *broker) cacheCanceledTask(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.canceled[id] = utils.Empty{}
}

func (b *broker) ExtendLease(ids ...string) (err error) {
	panic("unimplemented")
}

func (b *broker) Retry(id string, reason string) (err error) {
	
}

func (b *broker) retry(id string, reason string) error {
	b.q.Retry()
}