package server

import (
	"net/http"
	"time"

	"github.com/ggicci/httpin"
	"github.com/go-chi/chi/v5"
	"github.com/ttn-nguyen42/taskq/internal/broker"
	"github.com/ttn-nguyen42/taskq/internal/state"
	"github.com/ttn-nguyen42/taskq/pkg/api"
)

func submitTask(sm chi.Router, rt *runtime) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		req := r.
			Context().
			Value(httpin.Input).(*api.SubmitTaskRequest)

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

		w.WriteHeader(http.StatusCreated)
		if err := encode(w, resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	}

	sm.
		With(httpin.NewInput(api.SubmitTaskRequest{})).
		Post("/api/v1/tasks", handler)
}
