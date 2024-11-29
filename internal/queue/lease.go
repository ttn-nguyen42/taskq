package queue

import (
	"sync"
	"time"

	"github.com/ttn-nguyen42/taskq/internal/utils"
)

type Lease struct {
	mu        sync.RWMutex
	expiresAt time.Time
	notify    chan utils.Empty
}

func NewLease(expiresAt time.Time) *Lease {
	return &Lease{
		expiresAt: expiresAt,
		notify:    make(chan utils.Empty, 1),
	}
}

func (l *Lease) IsExpired() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.isExpired()
}

func (l *Lease) isExpired() bool {
	return time.Now().After(l.expiresAt)
}

func (l *Lease) ExpiresAt() time.Time {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.expiresAt
}

func (l *Lease) Done() <-chan utils.Empty {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.notify
}

func (l *Lease) Extend(expiresAt time.Time) (valid bool) {
	l.mu.RLock()
	valid = !l.isExpired()
	l.mu.Unlock()

	if !valid {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.isExpired() {
		return false
	}

	l.expiresAt = expiresAt
	return true
}

func (l *Lease) Notify() (success bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if !l.isExpired() {
		return false
	}

	select {
	case l.notify <- utils.Empty{}:
		return true
	default:
		return false
	}
}
