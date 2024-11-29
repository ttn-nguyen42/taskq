package queue

import "fmt"

func ns(name string) string {
	return "taskq:" + name
}

// MessageKey builds a key used by a single message
func MessageKey(queue string, id uint64) string {
	return ns(queue + ":msg:" + fmt.Sprintf("%d", id))
}

// PendingKey builds the key of the pending queue.
// The pending queue is a list of message IDs that are waiting to be processed.
// It is the default queue that messages first arrive at.
func PendingKey(name string) string {
	return ns(name + ":pending")
}

// RetryKey builds the key of the retry queue.
// The retry queue is a list of message IDs that are waiting to be retried.
// It is the queue that messages are moved to when they fail to be processed.
func RetryKey(name string) string {
	return ns(name + ":retry")
}

// InProgressKey builds the key of the in-progress queue.
// The in-progress queue is a list of message IDs that are currently being processed.
// It is the queue that messages are moved to when they are being dequeued from the pending queue.
func InProgressKey(name string) string {
	return ns(name + ":in_progress")
}

// StatsKey builds the key of the stats bucket.
func StatsKey() string {
	return ns("stats")
}

// CompletedKey builds the key of the completed queue.
// The completed queue is a list of message IDs that have been successfully processed.
// It is the queue that messages are moved to when they are being acknowledged.
func CompletedCountKey(name string) string {
	return ns(name + ":completed")
}

// LeaseKey builds the key of the lease bucket.
func LeaseKey(name string) string {
	return ns(name + ":lease")
}
