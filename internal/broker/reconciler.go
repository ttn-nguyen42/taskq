package broker

import (
	"github.com/ttn-nguyen42/taskq/internal/queue"
	"github.com/ttn-nguyen42/taskq/internal/utils"
)

type reconciler struct {
	stop chan utils.Empty
	q    queue.MessageQueue
}

func newReconciler(q queue.MessageQueue) *reconciler {
	return &reconciler{
		q:    q,
		stop: make(chan utils.Empty, 1),
	}
}

func (w *reconciler) Watch() error {
	for {
		select {
		case <-w.stop:
			return nil
		default:
		}

	}
}

func (w *reconciler) Stop() {
	w.stop <- utils.Empty{}
}
