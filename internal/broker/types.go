package broker

import (
	"time"

	"github.com/ttn-nguyen42/taskq/internal/state"
)

type Task struct {
	TaskId string
	Queue  string

	Input       Input
	Timeout     time.Duration
	MaxRetry    int
	LastRetryAt time.Time
	RetryCount  int
	Status      state.TaskStatus
	SubmittedAt time.Time
}

type Input map[string]any

type MessageMetadata struct {
	TaskID      string
	RetryCount  int
	LastRetryAt time.Time
}
