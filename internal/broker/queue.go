package broker

import (
	"fmt"

	"github.com/ttn-nguyen42/taskq/internal/state"
	"github.com/ttn-nguyen42/taskq/internal/utils"
)

func (b *broker) RegisterQueue(name string, priority uint) (queueId string, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.queues[name]; ok {
		return "", fmt.Errorf("queue %q already registered", name)
	}

	qi := state.NewQueueInfo(name, priority)
	queueId, err = b.state.RegisterQueue(qi)
	if err != nil {
		delete(b.queues, name)
		b.logger.
			With("queue", name).
			With("err", err).
			Error("failed to register queue")
		return "", err
	}

	b.queues[name] = utils.Empty{}
	b.rec.SetQueues(b.queues)

	b.logger.
		With("queue", name).
		With("id", queueId).
		Info("queue registered")
	return queueId, nil
}

func (b *broker) DeregisterQueue(name string) (err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.queues[name]; !ok {
		return fmt.Errorf("queue %q not registered", name)
	}

	qi, err := b.state.GetQueueByName(name)
	if err != nil {
		b.logger.
			With("queue", name).
			With("err", err).
			Error("failed to get queue")
		return err
	}

	if _, err := b.state.DeleteQueue(qi.ID); err != nil {
		b.logger.
			With("queueId", qi.ID).
			With("queue", name).
			With("err", err).
			Error("failed to deregister queue")
		return err
	}

	delete(b.queues, name)
	b.rec.SetQueues(b.queues)
	if err := b.q.Flush(name); err != nil {
		b.logger.
			With("queue", name).
			With("err", err).
			Error("failed to flush queue")
		return err
	}

	b.logger.
		With("queue", name).
		Info("queue deregistered")
	return nil
}
