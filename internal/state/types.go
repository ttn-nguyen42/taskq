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
	// The result is sorted oldest first.
	ListInfo(skip uint64, limit uint64) (info []TaskInfo, err error)

	// UpdateInfo updates a task info atomically.
	// It returns true if the task info exists and is updated.
	UpdateInfo(id string, upd func(*TaskInfo) bool) (ok bool, err error)

	// RecordQueue inserts a queue info into a persistent store.
	// It returns ErrAlreadyExists if the queue with the same name already exists.
	RegisterQueue(q *QueueInfo) (id string, err error)

	// GetQueue retrieves a queue info from a persistent store.
	GetQueue(id string) (info *QueueInfo, err error)

	// GetQueueByName retrieves a queue info by name from a persistent store.
	GetQueueByName(name string) (info *QueueInfo, err error)

	// DeleteQueue removes a queue info from a persistent store.
	// It returns true if the queue info exists and is deleted.
	DeleteQueue(id string) (ok bool, err error)

	// ListQueues retrieves a list of queue info from a persistent store.
	// The result is sorted oldest first.
	ListQueues(skip, limit uint64) (info []QueueInfo, err error)

	// UpdateQueue updates a queue info atomically.
	// It returns true if the queue info exists and is updated.
	UpdateQueue(id string, upd func(*QueueInfo) bool) (ok bool, err error)
}

type TaskStatus string

const (
	TaskStatusPending    TaskStatus = "pending"
	TaskStatusInProgress TaskStatus = "in_progress"
	TaskStatusComplete   TaskStatus = "complete"
	TaskStatusFailed     TaskStatus = "failed"
	TaskStatusRetry      TaskStatus = "retry"
	TaskStatusCanceled   TaskStatus = "canceled"
)

type TaskInfo struct {
	ID          string
	SubmittedAt time.Time

	QueueName   string
	Input       map[string]any
	MaxRetry    int
	Timeout     time.Duration
	Status      TaskStatus
	LastRetryAt time.Time
	RetryCount  int
}

func NewTaskInfo(maxRetry int, timeout time.Duration, queueName string, input map[string]any) *TaskInfo {
	return &TaskInfo{
		SubmittedAt: time.Now(),
		MaxRetry:    maxRetry,
		Timeout:     timeout,
		QueueName:   queueName,
		Status:      TaskStatusPending,
		Input:       input,
		LastRetryAt: time.Time{},
		RetryCount:  0,
	}
}

func EncodeInfo(t *TaskInfo) ([]byte, error) {
	return json.Marshal(t)
}

func DecodeInfo(data []byte) (*TaskInfo, error) {
	t := &TaskInfo{}
	if err := json.Unmarshal(data, t); err != nil {
		return nil, err
	}
	return t, nil
}

type QueueStatus string

const (
	QueueStatusActive   QueueStatus = "active"
	QueueStatusPaused   QueueStatus = "paused"
	QueueStatusDeleted  QueueStatus = "deleted"
	QueueStatusFlushing QueueStatus = "flushing"
)

type QueueInfo struct {
	ID           string
	RegisteredAt time.Time
	PausedAt     time.Time

	Name     string
	Priority uint

	Status QueueStatus
}

func NewQueueInfo() *QueueInfo {
	return &QueueInfo{
		RegisteredAt: time.Now(),
		Status:       QueueStatusActive,
	}
}

func EncodeQueue(q *QueueInfo) ([]byte, error) {
	return json.Marshal(q)
}

func DecodeQueue(data []byte) (*QueueInfo, error) {
	q := &QueueInfo{}
	if err := json.Unmarshal(data, q); err != nil {
		return nil, err
	}
	return q, nil
}

var (
	BucketTaskInfo  = ns("task_info")
	BucketQueueInfo = ns("queue_info")
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
func QueueKey(inc uint64) string {
	return ns("queue[id]:" + fmt.Sprintf("%d", inc))
}

// QueueKeyByName builds a key used by items in the queues bucket but by queue name.
//
// It stores queue metadata and stats.
func QueueKeyByName(name string) string {
	return ns("queue[name]:" + name)
}
