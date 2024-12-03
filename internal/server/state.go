package server

import (
	"net/http"

	"github.com/ggicci/httpin"
	"github.com/go-chi/chi/v5"
	"github.com/ttn-nguyen42/taskq/pkg/api"
)

func markAsSuccess(sm chi.Router, rt *runtime) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		req := r.
			Context().
			Value(httpin.Input).(*api.MarkAsSuccessRequest)

		rt.logger.
			With("taskId", req.TaskId).
			Info("marking task as success")

		if err := rt.br.Success(req.TaskId); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}

	sm.
		With(httpin.NewInput(api.MarkAsSuccessRequest{})).
		Put("/api/v1/tasks_success/{taskId}", handler)
}

func cancelTask(sm chi.Router, rt *runtime) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		req := r.
			Context().
			Value(httpin.Input).(*api.CancelTaskRequest)

		rt.logger.
			With("taskId", req.TaskId).
			Info("canceling task")

		if err := rt.br.Cancel(req.TaskId); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}

	sm.
		With(httpin.NewInput(api.CancelTaskRequest{})).
		Put("/api/v1/tasks_cancel/{taskId}", handler)
}

func markAsFailure(sm chi.Router, rt *runtime) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		req := r.
			Context().
			Value(httpin.Input).(*api.MarkAsFailureRequest)

		opts := req.Opts
		rt.logger.
			With("taskId", req.TaskId).
			Info("marking task as failure")

		if err := rt.br.Failure(req.TaskId, opts.Reason); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	}

	sm.
		With(httpin.NewInput(api.MarkAsFailureRequest{})).
		Put("/api/v1/tasks_failure/{taskId}", handler)
}
