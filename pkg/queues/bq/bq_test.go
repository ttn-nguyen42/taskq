package bq_test

import (
	"os"
	"testing"

	"github.com/ttn-nguyen42/taskq/internal/queue"
	"github.com/ttn-nguyen42/taskq/pkg/queues/bq"
)

func TestBboltQueue(t *testing.T) {
	path := "./tmp/test.db"
	if err := os.MkdirAll("./tmp", 0755); err != nil {
		t.Fatal(err)
	}

	q, err := bq.NewQueue(&bq.Options{
		Path: path,
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := q.Close(); err != nil {
			t.Fatal(err)
		}

		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}

		if err := os.RemoveAll("./tmp"); err != nil {
			t.Fatal(err)
		}
	})

	testMsgs := queue.Messages{
		{Queue: "1", Payload: []byte("1")},
		{Queue: "2", Payload: []byte("2")},
		{Queue: "3", Payload: []byte("3")},
		{Queue: "1", Payload: []byte("4")},
		{Queue: "2", Payload: []byte("5")},
		{Queue: "4", Payload: []byte("6")},
	}
	t.Run("enqueue", func(t *testing.T) {
		if err := q.Enqueue(testMsgs); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("dequeue", func(t *testing.T) {
		queues := []string{
			"1",
			"2",
			"3",
			"4",
		}

		expMap := map[string]queue.Messages{
			"1": {
				{Queue: "1", Payload: []byte("1")},
				{Queue: "1", Payload: []byte("4")},
			},
			"2": {
				{Queue: "2", Payload: []byte("2")},
				{Queue: "2", Payload: []byte("5")},
			},
			"3": {
				{Queue: "3", Payload: []byte("3")},
			},
			"4": {
				{Queue: "4", Payload: []byte("6")},
			},
		}

		for _, qname := range queues {
			msgs, err := q.Dequeue(&queue.DequeueOpts{Limit: 10}, qname)
			if err != nil {
				t.Fatal(err)
			}

			t.Logf("queue: %s, messages: %v", qname, msgs)

			expMsgs, ok := expMap[qname]
			if !ok {
				t.Fatalf("unexpected queue name: %s", qname)
			}

			if len(msgs) != len(expMsgs) {
				t.Fatalf("expected messages count: %d, got: %d", len(expMsgs), len(msgs))
			}

			for i, msg := range msgs {
				expMsg := expMsgs[i]
				if string(msg.Payload) != string(expMsg.Payload) {
					t.Fatalf("unexpected message payload: %s", msg.Payload)
				}
			}
		}
	})
}
