package queue

import (
	"encoding/json"
	"time"
)

type Message struct {
	ID      uint64
	Queue   string
	Payload []byte
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
	Enqueue(msgs Messages) error

	// Dequeue retrieves messages from the pending queue.
	//
	// Under the hood, the retrieved messages are removed from the pending queue and moved to the in-progress queue
	// and acquires a lease on those messages.
	Dequeue(opts *DequeueOpts, name string) (Messages, error)

	// Ack acknowledges the successful processing of messages.
	//
	// It completes the lease and move that messages from in-progress queue to the completed queue.
	Ack(msgs Messages) error

	// Requeue moves messages from the in-progress or retry queue back to the pending queue.
	Requeue(msgs Messages) error

	// Discard removes messages from the in-progress queue
	Discard(msg Messages) error

	// Retry moves messages from the in-progress to retry queue.
	Retry(msgs Messages) error

	// Completed returns the number of messages that have been successfully processed.
	Completed(name string) (uint64, error)

	// Pending returns the number of messages that are waiting to be processed.
	Pending(name string) (uint64, error)

	// InProgress returns the number of messages that are currently being processed.
	InProgress(name string) (uint64, error)
}
