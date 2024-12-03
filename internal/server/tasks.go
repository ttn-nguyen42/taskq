package server

import (
	"net/http"

	"github.com/ggicci/httpin"
	"github.com/go-chi/chi/v5"
	"github.com/ttn-nguyen42/taskq/internal/utils"
	"github.com/ttn-nguyen42/taskq/pkg/api"
)

func listTasks(sm chi.Router, rt *runtime) {
	handler := func(w http.ResponseWriter, r *http.Request) {
		req := r.
			Context().
			Value(httpin.Input).(*api.ListTasksRequest)

		skip, limit := utils.ToSkipAndLimit(req.Page, req.Size)
		tasks, err := rt.st.ListInfo(skip, limit)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp := api.ListTasksResponse{
			Tasks: make([]api.TaskInfo, 0, len(tasks)),
		}

		for _, task := range tasks {
			resp.Tasks = append(resp.Tasks, api.TaskInfo{
				TaskId:      task.ID,
				Queue:       task.QueueName,
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

	sm.With(httpin.NewInput(api.ListTasksRequest{})).Get("/api/v1/tasks", handler)
}
