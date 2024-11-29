package broker

import (
	"fmt"

	errs "github.com/ttn-nguyen42/taskq/internal/errors"
	"github.com/ttn-nguyen42/taskq/internal/state"
	"github.com/ttn-nguyen42/taskq/internal/utils"
)

func (b *broker) Acquire(queue string, count int) (tasks []*Task, err error) {
	panic("unimplemented")
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
