package state

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	errs "github.com/ttn-nguyen42/taskq/internal/errors"
	"go.etcd.io/bbolt"
)

type store struct {
	mu sync.RWMutex

	logger *slog.Logger
	db     *bbolt.DB
	opts   *StoreOpts
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	s.db = nil
	return nil
}

type StoreOpts struct {
	Path   string
	Logger *slog.Logger
}

func NewStore(opts *StoreOpts) (Store, error) {
	o := defaultOpts(opts)
	str := &store{
		opts:   o,
		logger: o.Logger,
	}
	return str, str.init()
}

func defaultOpts(o *StoreOpts) *StoreOpts {
	def := &StoreOpts{
		Path:   "state.db",
		Logger: slog.Default(),
	}
	if o == nil {
		return def
	}
	if len(o.Path) > 0 {
		def.Path = o.Path
	}
	if o.Logger != nil {
		def.Logger = o.Logger
	}

	return def
}

func (s *store) init() error {
	db, err := bbolt.Open(s.opts.Path, 0600, nil)
	if err != nil {
		return err
	}
	s.db = db

	return nil
}

func bytes(str string) []byte {
	return []byte(str)
}

func (s *store) RecordInfo(t *TaskInfo) (id string, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return "", fmt.Errorf("store is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		id, err = s.recordInfo(tx, t)
		return err
	}

	if err := db.Update(tx); err != nil {
		return "", err
	}

	return id, nil
}

func (s *store) recordInfo(tx *bbolt.Tx, t *TaskInfo) (id string, err error) {
	bucket, err := tx.CreateBucketIfNotExists(bytes(BucketTaskInfo))
	if err != nil {
		return "", fmt.Errorf("failed to initialize task info bucket: %w", err)
	}

	if len(t.ID) > 0 {
		id = t.ID
	} else {
		id = uuid.NewString()
		t.ID = id
	}

	t.SubmittedAt = time.Now()

	enc, err := EncodeInfo(t)
	if err != nil {
		return "", err
	}

	if err := bucket.Put(bytes(TaskInfoKey(id)), enc); err != nil {
		return "", fmt.Errorf("failed to save task info: %w", err)
	}

	// additional indexes
	if t.MessageId > 0 && len(t.QueueName) > 0 {
		queueAndId := TaskQueueAndIDKey(t.QueueName, t.MessageId)
		if err := bucket.Put(bytes(queueAndId), bytes(id)); err != nil {
			return "", fmt.Errorf("failed to save task info index: %w", err)
		}
	}

	return id, nil
}

func (s *store) GetInfo(id string) (info *TaskInfo, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("store is already shutdown")
	}

	err = db.View(func(tx *bbolt.Tx) error {
		info, err = s.getInfo(tx, id)
		return err
	})

	return info, err
}

func (s *store) getInfo(tx *bbolt.Tx, id string) (*TaskInfo, error) {
	bucket := tx.Bucket(bytes(BucketTaskInfo))
	if bucket == nil {
		return nil, errs.NewErrNotFound("task")
	}

	data := bucket.Get(bytes(TaskInfoKey(id)))
	if data == nil {
		return nil, errs.NewErrNotFound("task")
	}

	return DecodeInfo(data)
}

func (s *store) DeleteInfo(id string) (ok bool, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return false, fmt.Errorf("store is already shutdown")
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		ok, err = s.deleteInfo(tx, id)
		return err
	})

	return ok, err
}

func (s *store) deleteInfo(tx *bbolt.Tx, id string) (ok bool, err error) {
	bucket := tx.Bucket(bytes(BucketTaskInfo))
	if bucket == nil {
		return false, errs.NewErrNotFound("task")
	}

	key := TaskInfoKey(id)
	dat := bucket.Get(bytes(key))
	if dat == nil {
		return false, nil
	}

	t, err := DecodeInfo(dat)
	if err != nil {
		return false, err
	}

	if err := bucket.Delete(bytes(key)); err != nil {
		return false, fmt.Errorf("failed to delete task info: %w", err)
	}

	// additional indexes
	if t.MessageId > 0 && len(t.QueueName) > 0 {
		queueAndId := TaskQueueAndIDKey(t.QueueName, t.MessageId)
		if err := bucket.Delete(bytes(queueAndId)); err != nil {
			return false, fmt.Errorf("failed to delete task info index: %w", err)
		}
	}

	return true, nil
}

func (s *store) ListInfo(skip uint64, limit uint64) (info []TaskInfo, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return nil, fmt.Errorf("store is already shutdown")
	}

	err = db.View(func(tx *bbolt.Tx) error {
		info, err = s.listInfo(
			tx,
			skip,
			limit,
		)
		return err
	})

	return info, err
}

func (s *store) listInfo(tx *bbolt.Tx, skip, limit uint64) ([]TaskInfo, error) {
	bucket := tx.Bucket(bytes(BucketTaskInfo))
	if bucket == nil {
		return nil, nil
	}

	var list []TaskInfo

	if limit == 0 {
		return list, nil
	}

	cur := bucket.Cursor()

	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		decodedKey := string(k)

		// additional indexes
		if _, _, yes := isTaskQueueAndIdKey(decodedKey); yes {
			continue
		}

		if skip > 0 {
			skip -= 1
			continue
		}

		limit -= 1
		t, err := DecodeInfo(v)
		if err != nil {
			return nil, fmt.Errorf("failed to DecodeInfo task info: %w", err)
		}

		list = append(list, *t)
		if limit == 0 {
			break
		}
	}

	return list, nil
}

func (s *store) UpdateInfo(id string, upd func(*TaskInfo) bool) (ok bool, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()
	if db == nil {
		return false, fmt.Errorf("store is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		ok, err = s.updateInfo(tx, id, upd)
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

func (s *store) updateInfo(tx *bbolt.Tx, id string, upd func(*TaskInfo) bool) (ok bool, err error) {
	bucket := tx.Bucket(bytes(BucketTaskInfo))
	if bucket == nil {
		return false, errs.NewErrNotFound("task")
	}

	key := TaskInfoKey(id)
	dat := bucket.Get(bytes(key))
	if dat == nil {
		return false, nil
	}

	t, err := DecodeInfo(dat)
	if err != nil {
		return false, fmt.Errorf("failed to DecodeInfo task info: %w", err)
	}

	// additional indexes
	if t.MessageId > 0 && len(t.QueueName) > 0 {
		queueAndId := TaskQueueAndIDKey(t.QueueName, t.MessageId)
		if err := bucket.Delete(bytes(queueAndId)); err != nil {
			return false, fmt.Errorf("failed to delete task info index: %w", err)
		}
	}

	if updated := upd(t); !updated {
		// aborted
		return true, nil
	}

	// additional indexes
	if t.MessageId > 0 && len(t.QueueName) > 0 {
		queueAndId := TaskQueueAndIDKey(t.QueueName, t.MessageId)
		if err := bucket.Put(bytes(queueAndId), bytes(id)); err != nil {
			return false, fmt.Errorf("failed to save task info index: %w", err)
		}
	}

	enc, err := EncodeInfo(t)
	if err != nil {
		return false, err
	}

	if err := bucket.Put(bytes(key), enc); err != nil {
		return false, fmt.Errorf("failed to save task info: %w", err)
	}

	return true, nil
}

func (s *store) GetMultiInfo(ids ...string) (info []*TaskInfo, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return nil, fmt.Errorf("store is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		info, err = s.getMultiInfo(tx, ids...)
		if err != nil {
			return err
		}
		return nil
	}

	err = db.View(tx)

	return info, err
}

func (s *store) getMultiInfo(tx *bbolt.Tx, ids ...string) ([]*TaskInfo, error) {
	infos := make([]*TaskInfo, 0, len(ids))

	bucket := tx.Bucket(bytes(BucketTaskInfo))
	if bucket == nil {
		return nil, nil
	}

	for _, id := range ids {
		data := bucket.Get(bytes(TaskInfoKey(id)))
		if data == nil {
			return nil, errs.NewErrNotFound("task")
		}

		info, err := DecodeInfo(data)
		if err != nil {
			return nil, fmt.Errorf("failed to DecodeInfo task info: %w", err)
		}

		infos = append(infos, info)
	}

	return infos, nil
}

func (s *store) UpdateMultiInfo(ids []string, upd func(*TaskInfo) bool) (updated []string, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return nil, fmt.Errorf("store is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		updated, err = s.updateMultiInfo(tx, ids, upd)
		if err != nil {
			return err
		}
		return nil
	}

	err = db.Update(tx)
	if err != nil {
		return nil, err
	}

	return updated, nil
}

func (s *store) updateMultiInfo(tx *bbolt.Tx, ids []string, upd func(*TaskInfo) bool) (updated []string, err error) {
	bucket, err := tx.CreateBucketIfNotExists(bytes(BucketTaskInfo))
	if err != nil {
		return updated, fmt.Errorf("failed to initialize task info bucket: %w", err)
	}

	updated = make([]string, 0, len(ids))

	for _, id := range ids {
		key := TaskInfoKey(id)
		dat := bucket.Get(bytes(key))
		if dat == nil {
			continue
		}

		t, err := DecodeInfo(dat)
		if err != nil {
			return updated, fmt.Errorf("failed to DecodeInfo task info: %w", err)
		}

		// additional indexes
		if t.MessageId > 0 && len(t.QueueName) > 0 {
			queueAndId := TaskQueueAndIDKey(t.QueueName, t.MessageId)
			if err := bucket.Delete(bytes(queueAndId)); err != nil {
				return updated, fmt.Errorf("failed to delete task info index: %w", err)
			}
		}

		if updated := upd(t); !updated {
			// aborted
			continue
		}

		// additional indexes
		if t.MessageId > 0 && len(t.QueueName) > 0 {
			queueAndId := TaskQueueAndIDKey(t.QueueName, t.MessageId)
			if err := bucket.Put(bytes(queueAndId), bytes(id)); err != nil {
				return updated, fmt.Errorf("failed to save task info index: %w", err)
			}
		}

		enc, err := EncodeInfo(t)
		if err != nil {
			return updated, err
		}

		if err := bucket.Put(bytes(key), enc); err != nil {
			return updated, fmt.Errorf("failed to save task info: %w", err)
		}

		updated = append(updated, id)
	}

	return updated, nil
}

func (s *store) GetInfoByMessageID(queue string, id uint64) (info *TaskInfo, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return nil, fmt.Errorf("store is already shutdown")
	}

	err = db.View(func(tx *bbolt.Tx) error {
		info, err = s.getInfoByMessageID(tx, queue, id)
		return err
	})

	return info, err
}

func (s *store) getInfoByMessageID(tx *bbolt.Tx, queue string, id uint64) (*TaskInfo, error) {
	bucket := tx.Bucket(bytes(BucketTaskInfo))
	if bucket == nil {
		return nil, errs.NewErrNotFound("task")
	}

	queueAndId := TaskQueueAndIDKey(queue, id)
	messageId := bucket.Get(bytes(queueAndId))
	if messageId == nil {
		return nil, errs.NewErrNotFound("task")
	}

	return s.getInfo(tx, string(messageId))
}

func (s *store) GetMultiInfoByMessageID(queue string, ids ...uint64) (info []*TaskInfo, err error) {
	s.mu.RLock()
	db := s.db
	s.mu.RUnlock()

	if db == nil {
		return nil, fmt.Errorf("store is already shutdown")
	}

	tx := func(tx *bbolt.Tx) error {
		info, err = s.getMultiInfoByMessageID(tx, queue, ids...)
		if err != nil {
			return err
		}
		return nil
	}

	err = db.View(tx)

	return info, err
}

func (s *store) getMultiInfoByMessageID(tx *bbolt.Tx, queue string, ids ...uint64) ([]*TaskInfo, error) {
	bucket := tx.Bucket(bytes(BucketTaskInfo))
	if bucket == nil {
		return nil, nil
	}

	requestedIds := make([]string, 0, len(ids))
	for _, id := range ids {
		queueAndId := TaskQueueAndIDKey(queue, id)
		data := bucket.Get(bytes(queueAndId))
		if data == nil {
			continue
		}
		requestedIds = append(requestedIds, string(data))
	}

	return s.getMultiInfo(tx, requestedIds...)
}
