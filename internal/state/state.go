package state

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

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
		next, err := bucket.NextSequence()
		if err != nil {
			return "", fmt.Errorf("failed to generate id: %w", err)
		}
		id = TaskInfoKey(next)
		t.ID = id
	}

	t.SubmittedAt = time.Now()

	enc, err := Encode(t)
	if err != nil {
		return "", err
	}

	if err := bucket.Put(bytes(id), enc); err != nil {
		return "", fmt.Errorf("failed to save task info: %w", err)
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
		return nil, fmt.Errorf("task info bucket not found")
	}

	data := bucket.Get(bytes(id))
	if data == nil {
		return nil, fmt.Errorf("task info not found")
	}

	return Decode(data)
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
		return false, fmt.Errorf("task info bucket not found")
	}

	dat := bucket.Get(bytes(id))
	if dat == nil {
		return false, nil
	}

	if err := bucket.Delete(bytes(id)); err != nil {
		return false, fmt.Errorf("failed to delete task info: %w", err)
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
	bucket, err := tx.CreateBucketIfNotExists(bytes(BucketTaskInfo))
	if err != nil {
		return nil, fmt.Errorf("failed to initialize task info bucket: %w", err)
	}

	var list []TaskInfo

	cur := bucket.Cursor()

	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		if skip > 0 {
			skip -= 1
			continue
		}

		limit -= 1
		t, err := Decode(v)
		if err != nil {
			return nil, fmt.Errorf("failed to decode task info: %w", err)
		}

		list = append(list, *t)
		if limit == 0 {
			break
		}
	}

	return list, nil
}

func (s *store) UpdateInfo(id string, upd func(*TaskInfo)) (ok bool, err error) {
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

func (s *store) updateInfo(tx *bbolt.Tx, id string, upd func(*TaskInfo)) (ok bool, err error) {
	bucket := tx.Bucket(bytes(BucketTaskInfo))
	if bucket == nil {
		return false, fmt.Errorf("task info bucket not found")
	}

	dat := bucket.Get(bytes(id))
	if dat == nil {
		return false, nil
	}

	t, err := Decode(dat)
	if err != nil {
		return false, fmt.Errorf("failed to decode task info: %w", err)
	}

	upd(t)

	enc, err := Encode(t)
	if err != nil {
		return false, err
	}

	if err := bucket.Put(bytes(id), enc); err != nil {
		return false, fmt.Errorf("failed to save task info: %w", err)
	}

	return true, nil
}