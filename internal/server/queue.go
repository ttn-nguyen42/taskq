package server

import (
	"net/http"

	"github.com/ggicci/httpin"
	"github.com/go-chi/chi/v5"
	"github.com/ttn-nguyen42/taskq/pkg/api"
)

func registerQueue(sm chi.Router, rt *runtime) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		var req api.RegisterQueueRequest

		if err := decode(r, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		id, err := rt.br.RegisterQueue(req.Name, req.Priority)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := api.RegisterQueueResponse{
			QueueId: id,
		}

		w.WriteHeader(http.StatusCreated)
		if err := encode(w, resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	sm.
		Post("/api/v1/queues", handler)
}

func deleteQueue(sm chi.Router, rt *runtime) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		req := r.
			Context().
			Value(httpin.Input).(*api.DeleteQueueRequest)

		err := rt.br.DeregisterQueue(req.Name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}

	sm.
		With(httpin.NewInput(api.DeleteQueueRequest{})).
		Delete("/api/v1/queues/{name}", handler)
}
