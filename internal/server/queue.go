package server

import (
	"net/http"

	"github.com/ttn-nguyen42/taskq/pkg/api"
)

func registerQueue(rt *runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

		if err := encode(w, resp); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}
