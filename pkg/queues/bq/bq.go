package bq

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ttn-nguyen42/taskq/internal/queue"
	"go.etcd.io/bbolt"
)

const (
	LeaseDuration = time.Minute
)

type bqueue struct {
	mu sync.RWMutex

	logger *slog.Logger
	db     *bbolt.DB
	opts   *Options
}

type Options struct {
	Logger *slog.Logger
	Path   string
}

func NewQueue(o *Options) (queue.MessageQueue, error) {
	opts := buildOptions(o)
	bq := bqueue{
		logger: opts.Logger,
		opts:   opts,
	}
	if err := bq.init(); err != nil {
		bq.logger.
			With("err", err).
			Error("failed to initialize queue")
		return nil, err
	}
	return &bq, nil
}

func buildOptions(opts *Options) *Options {
	def := &Options{
		Logger: slog.Default(),
		Path:   "taskq.db",
	}
	if opts == nil {
		return def
	}
	if opts.Logger != nil {
		def.Logger = opts.Logger
	}
	if len(opts.Path) > 0 {
		def.Path = opts.Path
	}
	return def
}

func (q *bqueue) init() error {
	db, err := bbolt.Open(q.opts.Path, 0600, &bbolt.Options{
		Timeout: time.Second * 1,
	})
	if err != nil {
		return err
	}
	q.db = db

	return nil
}

func (q *bqueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	err := q.db.Close()
	if err != nil {
		return err
	}

	q.db = nil

	return nil
}

func (q *bqueue) Ack(msgs queue.Messages) error {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return fmt.Errorf("queue is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		for _, msg := range msgs {
			if err := q.ackSingle(tx, &msg); err != nil {
				q.logger.
					With("err", err).
					With("met", "bqueue.Ack").
					Error("failed to ack message")
				return err
			}
		}

		return nil
	}

	if err := bq.Update(tx); err != nil {
		return fmt.Errorf("failed to update database messages: %w", err)
	}

	return nil
}

func (q *bqueue) ackSingle(tx *bbolt.Tx, msg *queue.Message) error {
	inProgressKey := queue.InProgressKey(msg.Queue)
	msgKey := queue.MessageKey(msg.Queue, msg.ID)

	statsKey := queue.StatsKey()
	completedKey := queue.CompletedCountKey(msg.Queue)

	inProgBucket, err := tx.CreateBucketIfNotExists(bytes(inProgressKey))
	if err != nil {
		return fmt.Errorf("failed to create in-progress bucket: %w", err)
	}

	completedBucket, err := tx.CreateBucketIfNotExists(bytes(completedKey))
	if err != nil {
		return fmt.Errorf("failed to create completed bucket: %w", err)
	}

	statsBucket, err := tx.CreateBucketIfNotExists(bytes(statsKey))
	if err != nil {
		return fmt.Errorf("failed to create stats bucket: %w", err)
	}

	err = inProgBucket.Delete(bytes(msgKey))
	if err != nil {
		return fmt.Errorf("failed to delete message from in-progress: %w", err)
	}

	enc, err := queue.Encode(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	err = completedBucket.Put(bytes(msgKey), enc)
	if err != nil {
		return fmt.Errorf("failed to put message into completed: %w", err)
	}

	count := 0
	rawCount := statsBucket.Get(bytes(completedKey))
	if rawCount == nil {
		count = 1
	}

	count = int(binary.BigEndian.Uint64(rawCount))
	count += 1

	rawCount = binary.
		BigEndian.
		AppendUint64(
			make([]byte, 0),
			uint64(count),
		)
	err = statsBucket.Put(bytes(completedKey), rawCount)
	if err != nil {
		return fmt.Errorf("failed to update completed count: %w", err)
	}
	return nil
}

func (q *bqueue) Dequeue(opts *queue.DequeueOpts, name string) (queue.Messages, error) {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return nil, fmt.Errorf("queue is already shutdown")
	}

	var data queue.Messages

	tx := func(tx *bbolt.Tx) error {
		var err error

		data, err = q.dequeue(tx, name, opts)
		if err != nil {
			q.logger.
				With("err", err).
				With("met", "bqueue.Dequeue").
				Error("failed to dequeue messages")
			return err
		}

		return nil
	}

	if err := bq.Update(tx); err != nil {
		return nil, fmt.Errorf("failed to update database messages: %w", err)
	}

	return data, nil
}

func (q *bqueue) dequeue(tx *bbolt.Tx, name string, opts *queue.DequeueOpts) (queue.Messages, error) {
	pendingKey := queue.PendingKey(name)
	inProgressKey := queue.InProgressKey(name)
	leaseKey := queue.LeaseKey(name)

	pending, err := tx.CreateBucketIfNotExists(bytes(pendingKey))
	if err != nil {
		return nil, fmt.Errorf("failed to create pending bucket: %w", err)
	}

	pendingCur := pending.Cursor()
	msgs := make(queue.Messages, 0, opts.Limit)

	type msgData struct {
		raw []byte
		key []byte
	}

	rawData := make(map[uint64]msgData, opts.Limit)
	limit := opts.Limit

	for key, val := pendingCur.First(); key != nil; key, val = pendingCur.Next() {
		limit -= 1

		msg, err := queue.Decode(val)
		if err != nil {
			return nil, fmt.Errorf("failed to decode message: %w", err)
		}
		msgs = append(msgs, *msg)
		rawData[msg.ID] = msgData{
			raw: val,
			key: key,
		}

		if limit == 0 {
			break
		}
	}

	inProgress, err := tx.CreateBucketIfNotExists(bytes(inProgressKey))
	if err != nil {
		return nil, fmt.Errorf("failed to create in-progress bucket: %w", err)
	}

	leaseDur := time.
		Now().
		Add(LeaseDuration).
		UnixMicro()
	lease, err := tx.CreateBucketIfNotExists(bytes(leaseKey))
	if err != nil {
		return nil, fmt.Errorf("failed to create lease bucket: %w", err)
	}

	for i := len(msgs) - 1; i >= 0; i-- {
		msg := msgs[i]
		rd := rawData[msg.ID]

		err := inProgress.Put(rd.key, rd.raw)
		if err != nil {
			return nil, fmt.Errorf("failed to move message to in-progress: %w", err)
		}

		err = pending.Delete(rd.key)
		if err != nil {
			return nil, fmt.Errorf("failed to delete message from pending: %w", err)
		}

		leaseDat := binary.
			BigEndian.
			AppendUint64(
				make([]byte, 0),
				uint64(leaseDur),
			)
		err = lease.Put(rd.key, leaseDat)
		if err != nil {
			return nil, fmt.Errorf("failed to put lease on message: %w", err)
		}
	}

	return msgs, nil
}

func (q *bqueue) Enqueue(msgs queue.Messages) error {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return fmt.Errorf("queue is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		for _, m := range msgs {
			if err := q.enqueueSingle(tx, &m); err != nil {
				q.logger.
					With("err", err).
					With("met", "bqeue.Enqueue").
					Error("failed to enqueue message")
				return err
			}
		}
		return nil
	}

	if err := bq.Update(tx); err != nil {
		return fmt.Errorf("failed to update database messages: %w", err)
	}

	return nil
}

func (q *bqueue) enqueueSingle(tx *bbolt.Tx, msg *queue.Message) error {
	enc, err := queue.Encode(msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	pendingKey := queue.PendingKey(msg.Queue)

	pending, err := tx.CreateBucketIfNotExists(bytes(pendingKey))
	if err != nil {
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	if msg.ID == 0 {
		msgId, err := pending.NextSequence()
		if err != nil {
			return fmt.Errorf("failed to get next sequence: %w", err)
		}
		msg.ID = msgId
	}

	msgKey := queue.MessageKey(msg.Queue, msg.ID)

	err = pending.Put(bytes(msgKey), enc)
	if err != nil {
		return fmt.Errorf("failed to put message: %w", err)
	}

	return nil
}

func (q *bqueue) Requeue(msgs queue.Messages) error {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return fmt.Errorf("queue is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		for _, msg := range msgs {
			if err := q.requeueSingle(tx, msg); err != nil {
				q.logger.
					With("err", err).
					With("met", "bqueue.Requeue").
					Error("failed to requeue message")
				return err
			}
		}

		return nil
	}

	if err := bq.Update(tx); err != nil {
		return fmt.Errorf("failed to update database messages: %w", err)
	}

	return nil
}

func (q *bqueue) requeueSingle(tx *bbolt.Tx, msg queue.Message) error {
	pending := queue.PendingKey(msg.Queue)
	inProgress := queue.InProgressKey(msg.Queue)
	msgKey := queue.MessageKey(msg.Queue, msg.ID)

	inProgBucket, err := tx.CreateBucketIfNotExists(bytes(inProgress))
	if err != nil {
		return fmt.Errorf("failed to create in-progress bucket: %w", err)
	}

	pendingBucket, err := tx.CreateBucketIfNotExists(bytes(pending))
	if err != nil {
		return fmt.Errorf("failed to create pending bucket: %w", err)
	}

	enc, err := queue.Encode(&msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	err = inProgBucket.Delete(bytes(msgKey))
	if err != nil {
		return fmt.Errorf("failed to delete message from in-progress: %w", err)
	}

	pendingBucket.Put(bytes(msgKey), enc)

	return nil
}

func (q *bqueue) Discard(msg queue.Messages) error {
	panic("unimplemented")
}

func (q *bqueue) Retry(msgs queue.Messages) error {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return fmt.Errorf("queue is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		for _, msg := range msgs {
			if err := q.retrySingle(tx, msg); err != nil {
				q.logger.
					With("err", err).
					With("met", "bqueue.Retry").
					Error("failed to retry message")
				return err
			}
		}

		return nil
	}

	if err := bq.Update(tx); err != nil {
		return fmt.Errorf("failed to update database messages: %w", err)
	}

	return nil
}

func (q *bqueue) retrySingle(tx *bbolt.Tx, msg queue.Message) error {
	inProgressKey := queue.InProgressKey(msg.Queue)
	retryKey := queue.RetryKey(msg.Queue)
	msgKey := queue.MessageKey(msg.Queue, msg.ID)

	inProgBucket, err := tx.CreateBucketIfNotExists(bytes(inProgressKey))
	if err != nil {
		return fmt.Errorf("failed to create in-progress bucket: %w", err)
	}
	retryBucket, err := tx.CreateBucketIfNotExists(bytes(retryKey))
	if err != nil {
		return fmt.Errorf("failed to create retry bucket: %w", err)
	}

	err = inProgBucket.Delete(bytes(msgKey))
	if err != nil {
		return fmt.Errorf("failed to delete message from in-progress: %w", err)
	}

	enc, err := queue.Encode(&msg)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	err = retryBucket.Put(bytes(msgKey), enc)
	if err != nil {
		return fmt.Errorf("failed to put message into retry: %w", err)
	}

	return nil
}

func (q *bqueue) Completed(name string) (uint64, error) {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return 0, fmt.Errorf("queue is already shutdown")
	}

	var count uint64
	tx := func(tx *bbolt.Tx) error {
		var err error

		count, err = q.getCompletedCount(tx, name)
		if err != nil {
			q.logger.
				With("err", err).
				With("met", "bqueue.Completed").
				Error("failed to get completed count")
			return err
		}

		return nil
	}

	if err := bq.View(tx); err != nil {
		return 0, fmt.Errorf("failed to view database messages: %w", err)
	}

	return count, nil
}

func (q *bqueue) getCompletedCount(tx *bbolt.Tx, name string) (uint64, error) {
	statsKey := queue.StatsKey()
	completedKey := queue.CompletedCountKey(name)

	statsBucket, err := tx.CreateBucketIfNotExists(bytes(statsKey))
	if err != nil {
		return 0, fmt.Errorf("failed to create stats bucket: %w", err)
	}

	rawCount := statsBucket.Get(bytes(completedKey))
	if rawCount == nil {
		return 0, nil
	}

	return binary.BigEndian.Uint64(rawCount), nil
}

func (q *bqueue) InProgress(name string) (uint64, error) {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return 0, fmt.Errorf("queue is already shutdown")
	}

	var count uint64
	tx := func(tx *bbolt.Tx) error {
		var err error

		count, err = q.getInProgressCount(tx, name)
		if err != nil {
			q.logger.
				With("err", err).
				With("met", "bqueue.InProgress").
				Error("failed to get in-progress count")
			return err
		}

		return nil
	}

	if err := bq.View(tx); err != nil {
		return 0, fmt.Errorf("failed to view database messages: %w", err)
	}

	return count, nil
}

func (q *bqueue) getInProgressCount(tx *bbolt.Tx, name string) (uint64, error) {
	inProgressKey := queue.InProgressKey(name)

	inProg := tx.Bucket(bytes(inProgressKey))
	if inProg == nil {
		return 0, nil
	}

	return uint64(inProg.Stats().KeyN), nil
}

func (q *bqueue) Pending(name string) (uint64, error) {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return 0, fmt.Errorf("queue is already shutdown")
	}

	var count uint64
	tx := func(tx *bbolt.Tx) error {
		var err error

		count, err = q.getPendingCount(tx, name)
		if err != nil {
			q.logger.
				With("err", err).
				With("met", "bqueue.Pending").
				Error("failed to get pending count")
			return err
		}

		return nil
	}

	if err := bq.View(tx); err != nil {
		return 0, fmt.Errorf("failed to view database messages: %w", err)
	}

	return count, nil
}

func (q *bqueue) getPendingCount(tx *bbolt.Tx, name string) (uint64, error) {
	pendingKey := queue.PendingKey(name)

	pending := tx.Bucket(bytes(pendingKey))
	if pending == nil {
		return 0, nil
	}

	return uint64(pending.Stats().KeyN), nil
}

func bytes(s string) []byte {
	return []byte(s)
}
