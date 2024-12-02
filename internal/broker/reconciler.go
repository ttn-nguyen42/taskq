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

type reconciler struct {
	mu sync.RWMutex
	wg *sync.WaitGroup

	stop   chan utils.Empty
	queues map[string]utils.Empty
	q      queue.MessageQueue
	br     *broker
	dur    time.Duration
	logger *slog.Logger
}

func newReconciler(logger *slog.Logger, br *broker, q queue.MessageQueue, queues map[string]utils.Empty, dur time.Duration) *reconciler {
	return &reconciler{
		q:      q,
		stop:   make(chan utils.Empty, 1),
		queues: queues,
		dur:    dur,
		br:     br,
		logger: logger,
		wg:     &sync.WaitGroup{},
	}
}

func (w *reconciler) SetQueues(queues map[string]utils.Empty) {
	w.mu.Lock()
	w.queues = queues
	w.mu.Unlock()
}

func (w *reconciler) Watch() {
	w.wg.Add(1)

	timer := time.NewTimer(w.dur)
	go func() {
		defer func() {
			timer.Stop()
			w.wg.Done()
		}()

		for {
			select {
			case <-w.stop:
				return
			case <-timer.C:
				w.mu.RLock()
				queues := w.queues
				w.mu.RUnlock()

				if err := w.reconcile(100, queues); err != nil {
					w.logger.
						With("err", err).
						Error("failed to reconcile retry queue")
					log.Fatalf("failed to reconcile retry queue: %v", err)
				}

				timer.Reset(w.dur)
			}

		}
	}()

}

func (w *reconciler) reconcile(limit int, queues map[string]utils.Empty) error {
	handleTaskInfo := func(taskIds []string, oldMessageIds []uint64, newMessageIds []uint64) error {
		newIdByOldId := make(map[uint64]uint64, len(oldMessageIds))
		for i, oldId := range oldMessageIds {
			newIdByOldId[oldId] = newMessageIds[i]
		}

		_, err := w.br.state.UpdateMultiInfo(taskIds, func(ti *state.TaskInfo) bool {
			ti.MessageId = newIdByOldId[ti.MessageId]
			ti.Status = state.TaskStatusPending
			return true
		})
		if err != nil {
			return fmt.Errorf("failed to update task info: %w", err)
		}

		return nil
	}

	for qu := range queues {
		oldIds, newIds, err := w.q.ReconcileRetry(limit, qu)
		if err != nil {
			w.logger.
				With("err", err).
				With("queue", qu).
				Error("failed to reconcile retry queue, skipping")
			return err
		}

		taskInfos, err := w.br.state.GetMultiInfoByMessageID(qu, oldIds...)
		if err != nil {
			w.logger.
				With("err", err).
				With("queue", qu).
				Error("failed to retrieve task info, skipping")
			return err
		}

		taskIds := make([]string, 0, len(taskInfos))
		for _, ti := range taskInfos {
			taskIds = append(taskIds, ti.ID)
		}

		if err := handleTaskInfo(taskIds, oldIds, newIds); err != nil {
			w.logger.
				With("err", err).
				With("queue", qu).
				Error("failed to handle task info")
			return err
		}

		w.logger.
			With("queue", qu).
			With("old_ids", oldIds).
			With("new_ids", newIds).
			Info("reconciled retry queue")
	}

	return nil
}

func (w *reconciler) Stop() {
	w.stop <- utils.Empty{}

	w.wg.Wait()
}
