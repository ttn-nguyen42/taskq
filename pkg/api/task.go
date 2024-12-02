package api

import "time"

type SubmitTaskRequest struct {
	Queue    string         `json:"queue"`
	Input    map[string]any `json:"input"`
	MaxRetry int            `json:"maxRetry"`
	Timeout  time.Duration  `json:"timeout"`
}

type SubmitTaskResponse struct {
	TaskId string `json:"taskId"`
}
