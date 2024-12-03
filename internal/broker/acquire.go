package broker

import (
	"fmt"
	"time"

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
		tasks, err = b.fetchQueue(qname, count)
		if err != nil {
			return nil, err
		}
		count -= len(tasks)
		if len(tasks) == 0 {
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

	inProgressIds := make([]string, 0, len(tis))
	inProgressTaskInfos := make([]*state.TaskInfo, 0, len(tis))

	b.mu.RLock()
	for _, t := range tis {
		_, alreadyCanceled := b.canceled[t.ID]

		if alreadyCanceled {
			continue
		}

		inProgressIds = append(inProgressIds, t.ID)
		inProgressTaskInfos = append(inProgressTaskInfos, t)
	}
	b.mu.RUnlock()

	toInProgress := func(ti *state.TaskInfo) bool {
		switch ti.Status {
		case state.TaskStatusPending:
			ti.Status = state.TaskStatusInProgress
			ti.StartedAt = time.Now()
			return true
		default:
			b.logger.
				With("task_id", ti.ID).
				With("status", ti.Status).
				With("queue", ti.QueueName).
				Error("task status is invalid, the task must be in pending state")
			return false
		}
	}
	_, err = b.state.UpdateMultiInfo(inProgressIds, toInProgress)
	if err != nil {
		err = fmt.Errorf("failed to change task status to in progress: %w", err)
		b.logger.
			With("queue", qname).
			With("err", err).
			With("task_ids", inProgressIds).
			Error("failed to change task status")
		return
	}

	tasks = make([]*Task, 0, len(messages))

	for _, t := range inProgressTaskInfos {

		task := Task{
			TaskId:      t.ID,
			Queue:       qname,
			Input:       t.Input,
			Timeout:     t.Timeout,
			MaxRetry:    t.MaxRetry,
			LastRetryAt: t.LastRetryAt,
			RetryCount:  t.RetryCount,
			Status:      state.TaskStatusInProgress,
			SubmittedAt: t.SubmittedAt,
		}
		tasks = append(tasks, &task)
	}

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

func (b *broker) Success(id string) (err error) {
	if len(id) == 0 {
		return fmt.Errorf("task id is required")
	}

	err = b.markTaskAsSuccess(id)
	if err != nil {
		b.logger.
			With("task_id", id).
			With("err", err).
			Error("failed to mark task as success")
		return
	}

	return nil
}

func (b *broker) markTaskAsSuccess(id string) (err error) {
	invalidStatus := false

	upd := func(t *state.TaskInfo) bool {
		switch t.Status {
		case state.TaskStatusInProgress:
			t.Status = state.TaskStatusComplete
			return true
		default:
			invalidStatus = true
			return false
		}
	}

	found, err := b.state.UpdateInfo(id, upd)
	if err != nil {
		return fmt.Errorf("failed to update task info: %w", err)
	}

	if invalidStatus {
		return fmt.Errorf("task status is invalid, the task must be in progress state")
	}

	if !found {
		return errs.NewErrNotFound("task")
	}

	return
}

func (b *broker) Failure(id string, reason string) (err error) {
	if len(id) == 0 {
		return fmt.Errorf("task id is required")
	}

	err = b.markTaskAsFailed(id, reason)
	if err != nil {
		b.logger.
			With("task_id", id).
			With("err", err).
			Error("failed to mark task as failed")
		return
	}

	return nil
}

func (b *broker) markTaskAsFailed(id string, reason string) (err error) {
	taskInfo, err := b.state.GetInfo(id)
	if err != nil {
		return fmt.Errorf("failed to retrieve task info: %w", err)
	}

	canRetry := taskInfo.RetryCount < taskInfo.MaxRetry

	switch canRetry {
	case true:
		err = b.onFailureRetryable(id, reason)
	default:
		err = b.onFailureCantRetry(id, reason)
	}

	if err != nil {
		b.logger.
			With("task_id", id).
			With("err", err).
			With("can_retry", canRetry).
			Error("failed to change task status")
		return
	}

	if !canRetry {
		return
	}

	b.logger.
		With("task_id", id).
		With("queue", taskInfo.QueueName).
		With("message_id", taskInfo.MessageId).
		Info("retrying message")

	err = b.q.Retry(taskInfo.QueueName, taskInfo.MessageId)
	if err != nil {
		b.logger.
			With("task_id", id).
			With("err", err).
			Error("failed to retry message")
		return
	}

	return
}

func (b *broker) onFailureRetryable(id string, reason string) (err error) {
	invalidStatus := false

	upd := func(t *state.TaskInfo) bool {
		switch t.Status {
		case state.TaskStatusInProgress:
			t.Status = state.TaskStatusRetry
			t.Reason = reason
			return true
		default:
			invalidStatus = true
			return false
		}
	}

	found, err := b.state.UpdateInfo(id, upd)
	if err != nil {
		return fmt.Errorf("failed to update task info: %w", err)
	}

	if invalidStatus {
		return fmt.Errorf("task status is invalid, the task must be in progress state")
	}

	if !found {
		return errs.NewErrNotFound("task")
	}

	return
}

func (b *broker) onFailureCantRetry(id string, reason string) (err error) {
	invalidStatus := false

	upd := func(t *state.TaskInfo) bool {
		switch t.Status {
		case state.TaskStatusInProgress:
			t.Status = state.TaskStatusFailed
			t.Reason = reason
			return true
		default:
			invalidStatus = true
			return false
		}
	}

	found, err := b.state.UpdateInfo(id, upd)
	if err != nil {
		return fmt.Errorf("failed to update task info: %w", err)
	}

	if invalidStatus {
		return fmt.Errorf("task status is invalid, the task must be in progress state")
	}

	if !found {
		return errs.NewErrNotFound("task")
	}

	return
}
