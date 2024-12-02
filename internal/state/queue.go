package state

import (
	"fmt"
	"log"
	"strings"

	errs "github.com/ttn-nguyen42/taskq/internal/errors"
	"go.etcd.io/bbolt"
)

func (s *store) RegisterQueue(q *QueueInfo) (id string, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return "", fmt.Errorf("store is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		id, err = s.registerQueue(tx, q)
		if err != nil {
			return err
		}
		return nil
	}

	err = db.Update(tx)
	if err != nil {
		return "", err
	}

	return id, nil
}

func (s *store) registerQueue(tx *bbolt.Tx, q *QueueInfo) (id string, err error) {
	bucket, err := tx.CreateBucketIfNotExists(bytes(BucketQueueInfo))
	if err != nil {
		return "", fmt.Errorf("failed to create queue info bucket: %w", err)
	}

	if len(q.ID) > 0 {
		id = q.ID
	} else {
		inc, err := bucket.NextSequence()
		if err != nil {
			return "", err
		}

		id = QueueKey(inc)
		q.ID = id
	}

	queueId := bucket.Get(bytes(QueueKeyByName(q.Name)))
	if queueId != nil {
		return "", errs.NewErrAlreadyExists("queue")
	}

	data, err := EncodeQueue(q)
	if err != nil {
		return "", fmt.Errorf("failed to encode queue: %w", err)
	}

	err = bucket.Put(bytes(id), data)
	if err != nil {
		return "", fmt.Errorf("failed to save queue info: %w", err)
	}

	nameIndexKey := QueueKeyByName(q.Name)
	err = bucket.Put(bytes(nameIndexKey), bytes(id))
	if err != nil {
		return "", fmt.Errorf("failed to save queue name index: %w", err)
	}

	return id, nil
}

func (s *store) DeleteQueue(id string) (ok bool, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return false, fmt.Errorf("store is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		ok, err = s.deleteQueue(tx, id)
		if err != nil {
			return err
		}
		return nil
	}

	err = db.Update(tx)
	if err != nil {
		return false, err
	}

	return
}

func (s *store) deleteQueue(tx *bbolt.Tx, id string) (ok bool, err error) {
	bucket, err := tx.CreateBucketIfNotExists(bytes(BucketQueueInfo))
	if err != nil {
		return false, fmt.Errorf("failed to create queue info bucket: %w", err)
	}

	qu, err := s.getQueueById(tx, id)
	if err != nil {
		return false, err
	}

	err = bucket.Delete(bytes(qu.ID))
	if err != nil {
		return false, fmt.Errorf("failed to delete queue info: %w", err)
	}

	nameKey := QueueKeyByName(qu.Name)
	err = bucket.Delete(bytes(nameKey))
	if err != nil {
		return false, fmt.Errorf("failed to delete queue name index: %w", err)
	}

	return true, nil
}

func (s *store) GetQueue(id string) (info *QueueInfo, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return nil, fmt.Errorf("store is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		info, err = s.getQueueById(tx, id)
		if err != nil {
			return err
		}
		return nil
	}

	err = db.View(tx)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func (s *store) getQueueById(tx *bbolt.Tx, id string) (info *QueueInfo, err error) {
	bucket := tx.Bucket(bytes(BucketQueueInfo))
	if bucket == nil {
		return nil, errs.NewErrNotFound("queue")
	}

	dat := bucket.Get(bytes(id))
	if dat == nil {
		return nil, errs.NewErrNotFound("queue")
	}

	return DecodeQueue(dat)
}

func (s *store) GetQueueByName(name string) (info *QueueInfo, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return nil, fmt.Errorf("store is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		info, err = s.getQueueByName(tx, name)
		if err != nil {
			return err
		}
		return nil
	}

	err = db.View(tx)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func (s *store) getQueueByName(tx *bbolt.Tx, name string) (info *QueueInfo, err error) {
	bucket := tx.Bucket(bytes(BucketQueueInfo))
	if bucket == nil {
		return nil, errs.NewErrNotFound("queue")
	}

	id := bucket.Get(bytes(QueueKeyByName(name)))
	if id == nil {
		return nil, errs.NewErrNotFound("queue")
	}

	dat := bucket.Get(id)
	if dat == nil {
		return nil, errs.NewErrNotFound("queue")
	}

	return DecodeQueue(dat)
}

func (s *store) ListQueues(skip uint64, limit uint64) (info []QueueInfo, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return nil, fmt.Errorf("store is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		info, err = s.listQueues(tx, skip, limit)
		if err != nil {
			return err
		}

		return nil
	}

	err = db.View(tx)
	if err != nil {
		return nil, err
	}

	return info, nil
}

func (s *store) listQueues(tx *bbolt.Tx, skip uint64, limit uint64) (info []QueueInfo, err error) {
	bucket := tx.Bucket(bytes(BucketQueueInfo))
	if bucket == nil {
		return nil, nil
	}

	if limit == 0 {
		return info, nil
	}

	cur := bucket.Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		if s.isKeyName(k) {
			continue
		}

		if skip > 0 {
			skip -= 1
			continue
		}

		limit -= 1
		q, err := DecodeQueue(v)
		if err != nil {
			return nil, fmt.Errorf("failed to decode queue: %w", err)
		}

		info = append(info, *q)
		if limit == 0 {
			break
		}
	}

	return info, nil
}

func (s *store) isKeyName(k []byte) bool {
	ks := string(k)
	parts := strings.Split(ks, ":")
	if len(parts) < 3 {
		log.Fatalf("invalid key: %s", ks)
	}
	if parts[1] == "queue[name]" {
		return true
	}
	return false
}

func (s *store) UpdateQueue(id string, upd func(*QueueInfo) bool) (ok bool, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return false, fmt.Errorf("store is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		ok, err = s.updateQueue(tx, id, upd)
		if err != nil {
			return err
		}
		return nil
	}

	err = db.Update(tx)
	if err != nil {
		return false, err
	}

	return
}

func (s *store) updateQueue(tx *bbolt.Tx, id string, upd func(*QueueInfo) bool) (ok bool, err error) {
	bucket := tx.Bucket(bytes(BucketQueueInfo))
	if bucket == nil {
		return false, errs.NewErrNotFound("queue")
	}

	dat := bucket.Get(bytes(id))
	if dat == nil {
		return false, nil
	}

	info, err := DecodeQueue(dat)
	if err != nil {
		return false, fmt.Errorf("failed to decode queue info: %w", err)
	}

	if updated := upd(info); !updated {
		// aborted
		return true, nil
	}

	data, err := EncodeQueue(info)
	if err != nil {
		return false, fmt.Errorf("failed to encode queue info: %w", err)
	}

	err = bucket.Put(bytes(id), data)
	if err != nil {
		return false, fmt.Errorf("failed to save queue info: %w", err)
	}

	nameKey := QueueKeyByName(info.Name)
	err = bucket.Put(bytes(nameKey), bytes(id))
	if err != nil {
		return false, fmt.Errorf("failed to save queue name index: %w", err)
	}

	return true, nil
}
