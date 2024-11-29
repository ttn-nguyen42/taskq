package broker

import (
	"errors"
	"fmt"
	"time"

	errs "github.com/ttn-nguyen42/taskq/internal/errors"
	"github.com/ttn-nguyen42/taskq/internal/queue"
	"github.com/ttn-nguyen42/taskq/internal/state"
)

func (b *broker) Submit(t *Task) (id string, err error) {
	err = b.validateTask(t)
	if err != nil {
		b.logger.
			With("err", err).
			With("queue", t.Queue).
			Error("unable to submit, invalid task")
		return
	}

	id, err = b.submitTask(t)
	if err != nil {
		b.logger.
			With("err", err).
			With("queue", t.Queue).
			Error("failed to submit task")
		return
	}

	b.logger.
		With("queue", t.Queue).
		With("task_id", id).
		Debug("task submitted")
	return id, nil
}

func (b *broker) validateTask(t *Task) (err error) {
	queueExists, queueInfo, err := b.isQueueExists(t.Queue)
	if err != nil {
		err = fmt.Errorf("failed to retrieve queue info: %w", err)
		return
	}
	if !queueExists {
		err = errs.NewErrNotFound("queue")
		return
	}

	switch queueInfo.Status {
	case state.QueueStatusPaused:
		err = fmt.Errorf("queue is paused")
	case state.QueueStatusFlushing:
		err = fmt.Errorf("queue is in deletion (flushing)")
	case state.QueueStatusDeleted:
		err = fmt.Errorf("queue is deleted")
	default:
	}

	if err != nil {
		return
	}

	if t.MaxRetry < 0 {
		err = fmt.Errorf("max retry must be greater than or equal to 0")
		return
	}

	if t.Timeout < 0 {
		err = fmt.Errorf("timeout must be greater than or equal to 0")
		return
	}

	return
}

func (b *broker) submitTask(t *Task) (id string, err error) {
	ti := state.NewTaskInfo(
		t.MaxRetry,
		t.Timeout,
		t.Queue,
		t.Input,
	)

	id, err = b.state.RecordInfo(ti)
	if err != nil {
		err = fmt.Errorf("failed to record task info: %w", err)
		return
	}

	meta := MessageMetadata{
		TaskID:      id,
		RetryCount:  0,
		LastRetryAt: time.Time{},
	}
	msg, _ := queue.NewMessage(t.Queue, meta)

	err = b.q.Enqueue(queue.Single(*msg))
	if err != nil {
		err = fmt.Errorf("failed to enqueue task: %w", err)
		return
	}

	return
}

func (b *broker) isQueueExists(name string) (bool, *state.QueueInfo, error) {
	qi, err := b.state.GetQueueByName(name)
	if errors.Is(err, errs.ErrNotFound) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}

	return true, qi, nil
}
