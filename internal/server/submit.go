package server

import (
	"net/http"
	"time"

	"github.com/ggicci/httpin"
	"github.com/go-chi/chi/v5"
	"github.com/ttn-nguyen42/taskq/internal/broker"
	"github.com/ttn-nguyen42/taskq/internal/state"
	"github.com/ttn-nguyen42/taskq/internal/utils"
	"github.com/ttn-nguyen42/taskq/pkg/api"
)

func submitTask(sm chi.Router, rt *runtime) {
	handler := func(w http.ResponseWriter, r *http.Request) {
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

		w.WriteHeader(http.StatusCreated)
		if err := encode(w, resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

	}

	sm.
		Post("/api/v1/tasks", handler)
}

func getTask(sm chi.Router, rt *runtime) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		req := r.
			Context().
			Value(httpin.Input).(*api.GetTaskRequest)

		task, err := rt.br.Get(req.TaskId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := api.GetTaskResponse{
			TaskId:      task.TaskId,
			Queue:       task.Queue,
			LastRetryAt: task.LastRetryAt,
			Input:       task.Input,
			MaxRetry:    task.MaxRetry,
			RetryCount:  task.RetryCount,
			Status:      task.Status,
			SubmittedAt: task.SubmittedAt,
			Timeout:     utils.Duration(task.Timeout),
		}

		w.WriteHeader(http.StatusOK)
		if err := encode(w, resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	sm.
		With(httpin.NewInput(api.GetTaskRequest{})).
		Get("/api/v1/tasks/{taskId}", handler)
}
