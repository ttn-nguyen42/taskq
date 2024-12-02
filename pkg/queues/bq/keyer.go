package bq

import (
	"sync"
	"time"
)

type keyer struct {
	sync.RWMutex
	curUnix int64
}

func (k *keyer) Next() uint64 {
	k.Lock()
	defer k.Unlock()

	now := time.Now().UnixNano()
	if now <= k.curUnix {
		now = k.curUnix + 1
	}

	k.curUnix = now
	return uint64(now)
}
