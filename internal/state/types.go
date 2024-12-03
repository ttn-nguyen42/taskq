package state

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Store interface {
	Close() error

	// RecordInfo upserts a task info into a persistent store.
	RecordInfo(t *TaskInfo) (id string, err error)

	// GetInfo retrieves a task info from a persistent store.
	GetInfo(id string) (info *TaskInfo, err error)
	GetInfoByMessageID(queue string, id uint64) (info *TaskInfo, err error)

	// GetMultiInfo retrieves multiple task info from a persistent store.
	GetMultiInfo(ids ...string) (info []*TaskInfo, err error)
	GetMultiInfoByMessageID(queue string, ids ...uint64) (info []*TaskInfo, err error)

	// DeleteInfo removes a task info from a persistent store.
	// It returns true if the task info exists and is deleted.
	DeleteInfo(id string) (ok bool, err error)

	// ListInfo retrieves a list of task info from a persistent store.
	// The result is sorted oldest first.
	ListInfo(skip uint64, limit uint64) (info []TaskInfo, err error)

	// UpdateInfo updates a task info atomically.
	// It returns true if the task info exists and is updated.
	UpdateInfo(id string, upd func(*TaskInfo) bool) (ok bool, err error)

	// UpdateMultiInfo updates multiple task info atomically.
	UpdateMultiInfo(ids []string, upd func(*TaskInfo) bool) (updated []string, err error)

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
	MessageId   uint64
	QueueName   string
	SubmittedAt time.Time

	Input       map[string]any
	MaxRetry    int
	Timeout     time.Duration
	Status      TaskStatus
	LastRetryAt time.Time
	Reason      string
	RetryCount  int

	StartedAt   time.Time
	CompletedAt time.Time
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

func NewQueueInfo(name string, priority uint) *QueueInfo {
	return &QueueInfo{
		RegisteredAt: time.Now(),
		Status:       QueueStatusActive,
		Name:         name,
		Priority:     priority,
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
func TaskInfoKey(uuid string) string {
	return ns("task:" + uuid)
}

func TaskQueueAndIDKey(queue string, id uint64) string {
	return ns(fmt.Sprintf("task:%s:%d", queue, id))
}

func isTaskQueueAndIdKey(key string) (queue string, id uint64, ok bool) {
	parts := strings.Split(key, ":")
	if len(parts) != 4 {
		return
	}

	if parts[1] != "task" {
		return
	}

	queue = parts[2]
	id, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return
	}

	ok = true
	return
}

// QueueKey builds a key used by items in the queues bucket.
//
// It stores queue metadata and stats.
func QueueKey(uuid string) string {
	return ns("queue_id:" + uuid)
}

// QueueKeyByName builds a key used by items in the queues bucket but by queue name.
//
// It stores queue metadata and stats.
func QueueKeyByName(name string) string {
	return ns("queue_name:" + name)
}
