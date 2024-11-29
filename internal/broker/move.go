package broker

import "fmt"

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
	panic("unimplemented")
}
