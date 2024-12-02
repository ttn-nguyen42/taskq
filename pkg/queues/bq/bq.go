package bq

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"sync"
	"time"

	errs "github.com/ttn-nguyen42/taskq/internal/errors"
	"github.com/ttn-nguyen42/taskq/internal/queue"
	"go.etcd.io/bbolt"
)

const (
	LeaseDuration = time.Minute
	RetryTimeout  = time.Second * 30
)

type bqueue struct {
	mu sync.RWMutex

	logger *slog.Logger
	db     *bbolt.DB
	opts   *Options

	key *keyer
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
		key:    &keyer{curUnix: time.Now().Unix()},
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

func (q *bqueue) Ack(queue string, id uint64) error {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return fmt.Errorf("queue is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		if err := q.ackSingle(tx, queue, id); err != nil {
			q.logger.
				With("err", err).
				With("met", "bqueue.Ack").
				Error("failed to ack message")
			return err
		}

		return nil
	}

	if err := bq.Update(tx); err != nil {
		return fmt.Errorf("failed to update database messages: %w", err)
	}

	return nil
}

func (q *bqueue) ackSingle(tx *bbolt.Tx, name string, id uint64) error {
	inProgressKey := queue.InProgressKey(name)
	msgKey := queue.MessageKey(name, id)

	statsKey := queue.StatsKey()
	completedKey := queue.CompletedCountKey(name)

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

	msg := inProgBucket.Get(msgKey)
	if msg == nil {
		return errs.NewErrAlreadyExists("message")
	}

	err = inProgBucket.Delete(msgKey)
	if err != nil {
		return fmt.Errorf("failed to delete message from in-progress: %w", err)
	}

	err = completedBucket.Put(msgKey, msg)
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

func (q *bqueue) Enqueue(msgs queue.Messages) (ids []uint64, err error) {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return nil, fmt.Errorf("queue is already shutdown")
	}

	ids = make([]uint64, len(msgs))

	tx := func(tx *bbolt.Tx) error {
		for _, m := range msgs {
			id, err := q.enqueueSingle(tx, &m)
			if err != nil {
				q.logger.
					With("err", err).
					With("met", "bqeue.Enqueue").
					Error("failed to enqueue message")
				return err
			}
			ids = append(ids, id)
		}
		return nil
	}

	if err := bq.Update(tx); err != nil {
		return nil, fmt.Errorf("failed to update database messages: %w", err)
	}

	return ids, nil
}

func (q *bqueue) enqueueSingle(tx *bbolt.Tx, msg *queue.Message) (id uint64, err error) {
	pendingKey := queue.PendingKey(msg.Queue)

	pending, err := tx.CreateBucketIfNotExists(bytes(pendingKey))
	if err != nil {
		return 0, fmt.Errorf("failed to create bucket: %w", err)
	}

	if msg.ID == 0 {
		msgId := q.key.Next()
		msg.ID = msgId
	}

	enc, err := queue.Encode(msg)
	if err != nil {
		return 0, fmt.Errorf("failed to encode message: %w", err)
	}

	msgKey := queue.MessageKey(msg.Queue, msg.ID)

	err = pending.Put(msgKey, enc)
	if err != nil {
		return 0, fmt.Errorf("failed to put message: %w", err)
	}

	return msg.ID, nil
}

func (q *bqueue) Requeue(queue string, id uint64) (newId uint64, err error) {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return 0, fmt.Errorf("queue is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		newId, err = q.requeueSingle(tx, queue, id)
		if err != nil {
			q.logger.
				With("err", err).
				With("met", "bqueue.Requeue").
				Error("failed to requeue message")
			return err
		}

		return nil
	}

	if err := bq.Update(tx); err != nil {
		return 0, fmt.Errorf("failed to update database messages: %w", err)
	}

	return 0, nil
}

func (q *bqueue) requeueSingle(tx *bbolt.Tx, name string, id uint64) (newId uint64, err error) {
	pending := queue.PendingKey(name)
	inProgress := queue.InProgressKey(name)
	msgKey := queue.MessageKey(name, id)

	inProgBucket, err := tx.CreateBucketIfNotExists(bytes(inProgress))
	if err != nil {
		return 0, fmt.Errorf("failed to create in-progress bucket: %w", err)
	}

	pendingBucket, err := tx.CreateBucketIfNotExists(bytes(pending))
	if err != nil {
		return 0, fmt.Errorf("failed to create pending bucket: %w", err)
	}

	msg := inProgBucket.Get(msgKey)
	if msg == nil {
		return 0, errs.NewErrNotFound("message")
	}

	dec, err := queue.Decode(msg)
	if err != nil {
		return 0, fmt.Errorf("failed to decode message: %w", err)
	}

	newId, err = pendingBucket.NextSequence()
	if err != nil {
		return 0, fmt.Errorf("failed to get next sequence: %w", err)
	}
	dec.ID = newId

	enc, err := queue.Encode(dec)
	if err != nil {
		return 0, fmt.Errorf("failed to encode message: %w", err)
	}

	err = inProgBucket.Delete(msgKey)
	if err != nil {
		return 0, fmt.Errorf("failed to delete message from in-progress: %w", err)
	}

	pendingBucket.Put(msgKey, enc)

	return newId, nil
}

func (q *bqueue) Discard(queue string, id uint64) error {
	panic("unimplemented")
}

func (q *bqueue) Retry(queue string, id uint64) error {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return fmt.Errorf("queue is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		if err := q.retrySingle(tx, queue, id); err != nil {
			q.logger.
				With("err", err).
				With("met", "bqueue.Retry").
				Error("failed to retry message")
			return err
		}

		return nil
	}

	if err := bq.Update(tx); err != nil {
		return fmt.Errorf("failed to update database messages: %w", err)
	}

	return nil
}

func (q *bqueue) retrySingle(tx *bbolt.Tx, name string, id uint64) error {
	inProgressKey := queue.InProgressKey(name)
	retryKey := queue.RetryKey(name)
	msgKey := queue.MessageKey(name, id)

	inProgBucket, err := tx.CreateBucketIfNotExists(bytes(inProgressKey))
	if err != nil {
		return fmt.Errorf("failed to create in-progress bucket: %w", err)
	}
	retryBucket, err := tx.CreateBucketIfNotExists(bytes(retryKey))
	if err != nil {
		return fmt.Errorf("failed to create retry bucket: %w", err)
	}

	err = inProgBucket.Delete(msgKey)
	if err != nil {
		return fmt.Errorf("failed to delete message from in-progress: %w", err)
	}

	msg := inProgBucket.Get(msgKey)
	if msg == nil {
		return errs.NewErrNotFound("message")
	}

	dec, err := queue.Decode(msg)
	if err != nil {
		return fmt.Errorf("failed to decode message: %w", err)
	}

	dec.Retried += 1
	dec.LastFailedAt = time.Now()

	enc, err := queue.Encode(dec)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	err = retryBucket.Put(msgKey, enc)
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

func (q *bqueue) Move(id uint64, from string, to string) (newId uint64, err error) {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return 0, fmt.Errorf("queue is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		newId, err = q.moveSingle(tx, id, from, to)
		if err != nil {
			q.logger.
				With("err", err).
				With("met", "bqueue.Move").
				Error("failed to move message")
			return err
		}

		return nil
	}

	if err := bq.Update(tx); err != nil {
		return 0, fmt.Errorf("failed to update database messages: %w", err)
	}

	return newId, nil
}

func (q *bqueue) moveSingle(tx *bbolt.Tx, id uint64, from, to string) (newId uint64, err error) {
	fromKey := queue.PendingKey(from)
	toKey := queue.PendingKey(to)
	msgKey := queue.MessageKey(from, id)

	fromBucket := tx.Bucket(bytes(fromKey))
	if fromBucket == nil {
		return 0, fmt.Errorf("source queue not found")
	}

	toBucket := tx.Bucket(bytes(toKey))
	if toBucket == nil {
		return 0, fmt.Errorf("destination queue not found")
	}

	raw := fromBucket.Get(msgKey)
	if raw == nil {
		return 0, fmt.Errorf("message not found")
	}

	newId, err = toBucket.NextSequence()
	if err != nil {
		return 0, fmt.Errorf("failed to get next sequence: %w", err)
	}

	newMsgKey := queue.MessageKey(to, newId)

	err = toBucket.Put(newMsgKey, raw)
	if err != nil {
		return 0, fmt.Errorf("failed to put message into destination queue: %w", err)
	}

	err = fromBucket.Delete(msgKey)
	if err != nil {
		return 0, fmt.Errorf("failed to delete message from source queue: %w", err)
	}

	return newId, nil
}

func (q *bqueue) ReconcileRetry(limit int, name string) (ids []uint64, newIds []uint64, err error) {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return nil, nil, fmt.Errorf("queue is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		ids, newIds, err = q.reconcileRetryQueue(tx, name, limit)
		if err != nil {
			q.logger.
				With("err", err).
				With("met", "bqueue.reconcileRetry").
				Error("failed to reconcile retry queue")
			return err
		}

		return nil
	}

	if err := bq.Update(tx); err != nil {
		return nil, nil, fmt.Errorf("failed to update database messages: %w", err)
	}

	return ids, newIds, nil
}

func (q *bqueue) reconcileRetryQueue(tx *bbolt.Tx, name string, limit int) (ids []uint64, newIds []uint64, err error) {
	retryKey := queue.RetryKey(name)
	pendingKey := queue.PendingKey(name)

	retryBucket, err := tx.CreateBucketIfNotExists(bytes(retryKey))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create retry bucket: %w", err)
	}

	pendingBucket, err := tx.CreateBucketIfNotExists(bytes(pendingKey))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create pending bucket: %w", err)
	}

	retryCur := retryBucket.Cursor()

	itemsReconciled := 0
	ids = make([]uint64, 0, limit)
	newIds = make([]uint64, 0, limit)

	for key, val := retryCur.First(); key != nil; key, val = retryCur.Next() {
		msg, err := queue.Decode(val)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to decode message: %w", err)
		}

		err = retryBucket.Delete(key)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to delete message from retry: %w", err)
		}

		if msg.Retried >= msg.MaxRetry {
			continue
		}

		ids = append(ids, msg.ID)

		newId, err := pendingBucket.NextSequence()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get next sequence: %w", err)
		}
		msg.ID = newId

		newIds = append(newIds, newId)

		newKey := queue.MessageKey(name, newId)

		enc, err := queue.Encode(msg)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to encode message: %w", err)
		}

		err = pendingBucket.Put(newKey, enc)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to put message into pending: %w", err)
		}

		itemsReconciled += 1
		if itemsReconciled >= limit {
			break
		}
	}

	return ids, newIds, nil
}

func (q *bqueue) Flush(queue string) error {
	q.mu.RLock()
	bq := q.db
	q.mu.RUnlock()

	if bq == nil {
		return fmt.Errorf("queue is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		if err := q.flushSingle(tx, queue); err != nil {
			q.logger.
				With("err", err).
				With("met", "bqueue.Flush").
				Error("failed to flush queue")
			return err
		}

		return nil
	}

	if err := bq.Update(tx); err != nil {
		return fmt.Errorf("failed to update database messages: %w", err)
	}

	return nil
}

func (q *bqueue) flushSingle(tx *bbolt.Tx, name string) error {
	pendingKey := queue.PendingKey(name)
	inProgressKey := queue.InProgressKey(name)
	retryKey := queue.RetryKey(name)

	pending := tx.Bucket(bytes(pendingKey))
	if pending != nil {
		err := pending.ForEach(func(k, v []byte) error {
			return pending.Delete(k)
		})
		if err != nil {
			return fmt.Errorf("failed to flush pending: %w", err)
		}
	}

	inProgress := tx.Bucket(bytes(inProgressKey))
	if inProgress != nil {
		err := inProgress.ForEach(func(k, v []byte) error {
			return inProgress.Delete(k)
		})
		if err != nil {
			return fmt.Errorf("failed to flush in-progress: %w", err)
		}
	}

	retry := tx.Bucket(bytes(retryKey))
	if retry != nil {
		err := retry.ForEach(func(k, v []byte) error {
			return retry.Delete(k)
		})
		if err != nil {
			return fmt.Errorf("failed to flush retry: %w", err)
		}
	}

	tx.DeleteBucket(bytes(pendingKey))
	tx.DeleteBucket(bytes(inProgressKey))
	tx.DeleteBucket(bytes(retryKey))

	return nil
}

func bytes(s string) []byte {
	return []byte(s)
}
