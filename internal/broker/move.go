package broker

import (
	"fmt"

	"github.com/ttn-nguyen42/taskq/internal/state"
)

func (b *broker) Get(id string) (t *Task, err error) {
	if len(id) == 0 {
		return nil, fmt.Errorf("task id is required")
	}

	ti, err := b.state.GetInfo(id)
	if err != nil {
		err = fmt.Errorf("failed to retrieve task info: %w", err)
		b.logger.
			With("err", err).
			Error("failed to get task")
		return
	}

	t = &Task{
		Queue:       ti.QueueName,
		Input:       ti.Input,
		Timeout:     ti.Timeout,
		MaxRetry:    ti.MaxRetry,
		LastRetryAt: ti.LastRetryAt,
		RetryCount:  ti.RetryCount,
		Status:      ti.Status,
		SubmittedAt: ti.SubmittedAt,
	}
	return t, nil
}

func (b *broker) Move(id string, from string, to string) (err error) {
	if len(id) == 0 {
		return fmt.Errorf("task id is required")
	}

	qiSrc, err := b.state.GetQueueByName(from)
	if err != nil {
		err = fmt.Errorf("failed to retrieve source queue info: %w", err)
		b.logger.
			With("queue", from).
			With("err", err).
			Error("failed to move task")
		return
	}

	qiDest, err := b.state.GetQueueByName(to)
	if err != nil {
		err = fmt.Errorf("failed to retrieve destination queue info: %w", err)
		b.logger.
			With("queue", to).
			With("err", err).
			Error("failed to move task")
		return
	}

	if qiSrc.ID == qiDest.ID {
		return fmt.Errorf("source and destination queues are the same")
	}

	task, err := b.state.GetInfo(id)
	if err != nil {
		err = fmt.Errorf("failed to retrieve task info: %w", err)
		b.logger.
			With("task_id", id).
			With("err", err).
			Error("failed to move task")
		return
	}

	switch task.Status {
	case state.TaskStatusPending:
	default:
		return fmt.Errorf("task is not in pending state")
	}

	newId, err := b.q.Move(task.MessageId, from, to)
	if err != nil {
		err = fmt.Errorf("failed to move message: %w", err)
		b.logger.
			With("task_id", id).
			With("from", from).
			With("to", to).
			With("err", err).
			Error("failed to move task")
		return
	}

	_, err = b.state.UpdateInfo(id, func(t *state.TaskInfo) bool {
		t.QueueName = to
		t.MessageId = newId
		return true
	})
	if err != nil {
		err = fmt.Errorf("failed to update task info: %w", err)
		b.logger.
			With("task_id", id).
			With("err", err).
			Error("failed to move task")
		return
	}

	return
}
