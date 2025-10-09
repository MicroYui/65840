package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck  kvtest.IKVClerk
	key string
	id  string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, key: l, id: kvtest.RandValue(8)}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey || value == "" {
			putErr := lk.ck.Put(lk.key, lk.id, version)
			if putErr == rpc.OK {
				return
			}
		} else if value == lk.id {
			// 已经持有锁
			return
		}
	}
}

func (lk *Lock) Release() {
	for {
		value, version, err := lk.ck.Get(lk.key)
		if err == rpc.OK && value == lk.id {
			putErr := lk.ck.Put(lk.key, "", version)
			if putErr == rpc.OK {
				return
			}
		} else {
			return
		}
	}
}
