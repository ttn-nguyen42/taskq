package api

import (
	"time"

	"github.com/ttn-nguyen42/taskq/internal/broker"
	"github.com/ttn-nguyen42/taskq/internal/state"
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

type GetTaskRequest struct {
	TaskId string `in:"path=taskId"`
}

type TaskInfo struct {
	TaskId      string           `json:"taskId"`
	Queue       string           `json:"queue"`
	Input       broker.Input     `json:"input"`
	MaxRetry    int              `json:"maxRetry"`
	LastRetryAt time.Time        `json:"lastRetryAt"`
	RetryCount  int              `json:"retryCount"`
	Status      state.TaskStatus `json:"status"`
	SubmittedAt time.Time        `json:"submittedAt"`
	Timeout     utils.Duration   `json:"timeout"`
}

type GetTaskResponse TaskInfo

type AcquireTasksRequest struct {
	Queue string `in:"path=queueName"`
	Count int    `in:"query=count"`
}

type AcquireTasksResponse struct {
	Tasks []TaskInfo `json:"tasks"`
}

type MarkAsSuccessRequest struct {
	TaskId string `in:"path=taskId"`
}

type RetryTaskRequest struct {
	TaskId string `in:"path=taskId"`
	Reason string `json:"reason"`
}

type CancelTaskRequest struct {
	TaskId string `in:"path=taskId"`
}

type MarkAsFailureRequest struct {
	TaskId string      `in:"path=taskId"`
	Opts   FailureOpts `in:"body"`
}

type FailureOpts struct {
	Reason string `json:"reason"`
}

type ListTasksRequest struct {
	Queue string `in:"path=queueName"`
	Page  uint64 `in:"query=page"`
	Size  uint64 `in:"query=size"`
}

type ListTasksResponse struct {
	Tasks []TaskInfo `json:"tasks"`
}
