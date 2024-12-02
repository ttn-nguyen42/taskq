package api

import (
	"github.com/ttn-nguyen42/taskq/internal/utils"
)

type SubmitTaskRequest struct {
	Queue    string         `json:"queue"`
	Input    map[string]any `json:"input"`
	MaxRetry int            `json:"maxRetry"`
	Timeout  utils.Duration `json:"timeout"`
}

type SubmitTaskResponse struct {
	TaskId string `json:"taskId"`
}
