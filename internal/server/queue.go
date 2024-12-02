package server

import (
	"net/http"

	"github.com/ggicci/httpin"
	"github.com/go-chi/chi/v5"
	"github.com/ttn-nguyen42/taskq/internal/utils"
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

func listQueues(sm chi.Router, rt *runtime) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		req := r.
			Context().
			Value(httpin.Input).(*api.ListQueuesRequest)

		skip, limit := utils.ToSkipAndLimit(req.Page, req.Size)

		queues, err := rt.st.ListQueues(skip, limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		queuesList := make([]api.QueueInfo, 0, len(queues))
		for _, q := range queues {
			queuesList = append(queuesList, api.QueueInfo{
				Id:           q.ID,
				RegisteredAt: q.RegisteredAt,
				PausedAt:     q.PausedAt,
				Name:         q.Name,
				Priority:     q.Priority,
				Status:       string(q.Status),
			})
		}

		resp := api.ListQueuesResponse{
			Queues: queuesList,
		}

		if err := encode(w, resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	sm.
		With(httpin.NewInput(api.ListQueuesRequest{})).
		Get("/api/v1/queues", handler)
}

func getQueue(sm chi.Router, rt *runtime) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		req := r.
			Context().
			Value(httpin.Input).(*api.GetQueueRequest)

		q, err := rt.st.GetQueueByName(req.Name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := api.GetQueueResponse{
			Id:           q.ID,
			RegisteredAt: q.RegisteredAt,
			PausedAt:     q.PausedAt,
			Name:         q.Name,
			Priority:     q.Priority,
			Status:       string(q.Status),
		}

		if err := encode(w, resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	sm.
		With(httpin.NewInput(api.GetQueueRequest{})).
		Get("/api/v1/queues/{name}", handler)
}
