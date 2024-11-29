package state

import (
	"encoding/json"
	"fmt"
	"time"
)

type Store interface {
	// RecordInfo upserts a task info into a persistent store.
	RecordInfo(t *TaskInfo) (id string, err error)

	// GetInfo retrieves a task info from a persistent store.
	GetInfo(id string) (info *TaskInfo, err error)

	// DeleteInfo removes a task info from a persistent store.
	// It returns true if the task info exists and is deleted.
	DeleteInfo(id string) (ok bool, err error)

	// ListInfo retrieves a list of task info from a persistent store.
	// The result is sorted in ascending order, oldest first.
	ListInfo(skip uint64, limit uint64) (info []TaskInfo, err error)

	// UpdateInfo updates a task info atomically.
	// It returns true if the task info exists and is updated.
	UpdateInfo(id string, upd func(*TaskInfo)) (ok bool, err error)
}

type Status string

const (
	StatusPending    Status = "pending"
	StatusInProgress Status = "in_progress"
	StatusComplete   Status = "complete"
	StatusFailed     Status = "failed"
	StatusRetry      Status = "retry"
	StatusCancelled  Status = "cancelled"
)

type TaskInfo struct {
	ID          string
	SubmittedAt time.Time
	MessageID   string

	MaxRetry int
	Timeout  time.Duration
	Status   Status
}

func NewTaskInfo(messageId string, maxRetry int, timeout time.Duration) *TaskInfo {
	return &TaskInfo{
		SubmittedAt: time.Now(),
		MessageID:   messageId,
		MaxRetry:    maxRetry,
		Timeout:     timeout,
		Status:      StatusPending,
	}
}

func Encode(t *TaskInfo) ([]byte, error) {
	return json.Marshal(t)
}

func Decode(data []byte) (*TaskInfo, error) {
	t := &TaskInfo{}
	if err := json.Unmarshal(data, t); err != nil {
		return nil, err
	}
	return t, nil
}

const (
	BucketTaskInfo = "task_info"
	BucketQueues   = "queues"
)

func ns(name string) string {
	return "taskq:" + name
}

// TaskInfoKey builds a key used by a single task info
func TaskInfoKey(inc uint64) string {
	return ns("task:" + fmt.Sprintf("%d", inc))
}

// QueueKey builds a key used by items in the queues bucket.
//
// It stores queue metadata and stats.
func QueueKey(name string) string {
	return ns("queue:" + name)
}
