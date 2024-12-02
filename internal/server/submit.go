package server

import (
	"net/http"
	"time"

	"github.com/ttn-nguyen42/taskq/internal/broker"
	"github.com/ttn-nguyen42/taskq/internal/state"
	"github.com/ttn-nguyen42/taskq/pkg/api"
)

func submitTask(rt *runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req api.SubmitTaskRequest

		if err := decode(r, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		task := broker.Task{
			Queue:       req.Queue,
			Input:       req.Input,
			MaxRetry:    req.MaxRetry,
			SubmittedAt: time.Now(),
			Status:      state.TaskStatusPending,
		}

		if req.Timeout > 0 {
			task.Timeout = time.Duration(req.Timeout)
		}

		id, err := rt.br.Submit(&task)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := api.SubmitTaskResponse{
			TaskId: id,
		}

		if err := encode(w, resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}
