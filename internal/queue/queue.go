package queue

import (
	"encoding/json"
	"time"
)

type Message struct {
	ID           uint64
	Queue        string
	Payload      []byte
	MaxRetry     int
	Retried      int
	LastFailedAt time.Time
}

func NewMessage(qname string, payload any, maxRetry int) (*Message, error) {
	pl, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Message{
		Queue:        qname,
		Payload:      pl,
		ID:           0,
		MaxRetry:     maxRetry,
		Retried:      0,
		LastFailedAt: time.Time{},
	}, nil
}

func (msg Message) Into(v any) error {
	return json.Unmarshal(msg.Payload, v)
}

type Messages []Message

func (msg Messages) IDs() []uint64 {
	ids := make([]uint64, len(msg))
	for i, m := range msg {
		ids[i] = m.ID
	}
	return ids
}

// Single wraps a single message into a Messages.
func Single(msg Message) Messages {
	return Messages{msg}
}

// Encode encodes a message into a byte slice.
func Encode(m *Message) ([]byte, error) {
	return json.Marshal(m)
}

// Decode decodes a byte slice into a message.
func Decode(data []byte) (*Message, error) {
	var m Message
	err := json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	return &m, nil
}

type DequeueOpts struct {
	Limit int
}

type RetryOpts struct {
	LastFailedAt time.Time
	Reason       string
}

type MessageQueue interface {
	Close() error

	// Enqueue submits messages into the pending queue.
	//
	// It returns the IDs in the same order as the input messages.
	Enqueue(msgs Messages) (ids []uint64, err error)

	// Dequeue retrieves messages from the pending queue.
	//
	// Under the hood, the retrieved messages are removed from the pending queue and moved to the in-progress queue
	// and acquires a lease on those messages.
	Dequeue(opts *DequeueOpts, name string) (Messages, error)

	// Ack acknowledges the successful processing of messages.
	//
	// It completes the lease and move that messages from in-progress queue to the completed queue.
	Ack(queue string, id uint64) error

	// Requeue moves messages from the in-progress or retry queue back to the pending queue.
	Requeue(queue string, id uint64) (newId uint64, err error)

	// Discard removes messages from the in-progress queue
	Discard(queue string, id uint64) error

	// Retry moves messages from the in-progress to retry queue.
	Retry(queue string, id uint64) error

	// Completed returns the number of messages that have been successfully processed.
	Completed(name string) (uint64, error)

	// Pending returns the number of messages that are waiting to be processed.
	Pending(name string) (uint64, error)

	// InProgress returns the number of messages that are currently being processed.
	InProgress(name string) (uint64, error)

	// Move removes a message from one queue and push it to another.
	Move(id uint64, from, to string) (newId uint64, err error)

	// ReconcileRetry moves messages from the retry queue back to the pending queue.
	ReconcileRetry(limit int, queues ...string) (ids []uint64, newIds []uint64, err error)
}
