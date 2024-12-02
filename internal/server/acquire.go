package server

import (
	"net/http"

	"github.com/ggicci/httpin"
	"github.com/go-chi/chi/v5"
	"github.com/ttn-nguyen42/taskq/internal/utils"
	"github.com/ttn-nguyen42/taskq/pkg/api"
)

func acquireTasks(sm chi.Router, rt *runtime) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		req := r.
			Context().
			Value(httpin.Input).(*api.AcquireTasksRequest)

		if req.Count == 0 {
			http.Error(w, "count must be greater than 0", http.StatusBadRequest)
			return
		}

		rt.logger.
			With("queue", req.Queue).
			With("count", req.Count).
			Info("acquiring tasks")

		tasks, err := rt.br.Acquire(req.Queue, req.Count)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := api.AcquireTasksResponse{
			Tasks: make([]api.TaskInfo, 0, len(tasks)),
		}

		for _, task := range tasks {
			resp.Tasks = append(resp.Tasks, api.TaskInfo{
				TaskId:      task.TaskId,
				Queue:       task.Queue,
				Input:       task.Input,
				MaxRetry:    task.MaxRetry,
				LastRetryAt: task.LastRetryAt,
				RetryCount:  task.RetryCount,
				Status:      task.Status,
				SubmittedAt: task.SubmittedAt,
				Timeout:     utils.Duration(task.Timeout),
			})
		}

		w.WriteHeader(http.StatusOK)
		if err := encode(w, resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	sm.
		With(httpin.NewInput(api.AcquireTasksRequest{})).
		Get("/api/v1/acquire/{queueName}", handler)
}
