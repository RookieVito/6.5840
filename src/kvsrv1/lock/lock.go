package lock

import (
	"fmt"
	"math/rand"
	"time"

	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
)

const (
	unlocked = "unlocked"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
	holder   string // 当前 client 持有锁时的标识符
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockname: lockname,
		holder:   fmt.Sprintf("holder-%d", rand.Int63()),
	}

	// 首先检查锁是否已经在 kvserver 中存在
	_, _, err := lk.ck.Get(lockname)
	if err == rpc.OK {
		// 锁已存在，直接返回
		return lk
	}

	// 锁不存在，尝试创建并初始化为 unlocked
	// 使用 version 0 来创建新的 key
	for {
		err = lk.ck.Put(lockname, unlocked, 0)
		if err == rpc.OK {
			// 成功创建锁
			return lk
		}
		
		if err == rpc.ErrVersion {
			// 其他 client 已经创建了这个锁
			return lk
		}

		if err == rpc.ErrMaybe {
			// Put 可能成功也可能失败，需要确认
			_, _, getErr := lk.ck.Get(lockname)
			if getErr == rpc.OK {
				// 锁已经存在（可能是我们创建的，也可能是其他 client 创建的）
				return lk
			}
			// Get 失败，继续重试
		}

		// 其他错误，继续重试
		time.Sleep(10 * time.Millisecond)
	}
}

// Acquire attempts to acquire the lock.
// It blocks until the lock is successfully acquired.
func (lk *Lock) Acquire() {
	lockedValue := fmt.Sprintf("locked:%s", lk.holder)

	for {
		value, version, err := lk.ck.Get(lk.lockname)

		// 只有在锁不存在或者锁处于 unlocked 状态时才尝试获取
		if err == rpc.ErrNoKey || value == unlocked {
			// 尝试获取锁
			// 使用 CAS (Compare-And-Swap) 语义：只有在 version 匹配时才会成功
			err = lk.ck.Put(lk.lockname, lockedValue, version)

			if err == rpc.OK {
				// 成功获取锁
				return
			}

			if err == rpc.ErrMaybe {
				// Put 可能成功也可能失败，需要检查当前状态
				newValue, _, getErr := lk.ck.Get(lk.lockname)
				if getErr == rpc.OK && newValue == lockedValue {
					// 确认锁已被当前 client 获取
					return
				}
				// 否则说明获取失败，继续重试
			}

			// err == rpc.ErrVersion: version 不匹配，说明其他 client 修改了锁
			// 继续循环重试
		}

		// 锁被其他 client 持有，等待后重试
		time.Sleep(10 * time.Millisecond)
	}
}

// Release releases the lock.
// Only the holder of the lock should call this method.
func (lk *Lock) Release() {
	expectedValue := fmt.Sprintf("locked:%s", lk.holder)

	for {
		value, version, err := lk.ck.Get(lk.lockname)

		if err == rpc.ErrNoKey {
			// 锁不存在，说明已经被释放（或从未存在）
			return
		}

		if err != rpc.OK {
			// Get 失败，重试
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// 检查锁是否被当前 client 持有
		if value != expectedValue {
			// 锁不是当前 client 持有的
			// 可能的情况：
			// 1. 锁已经是 unlocked（已被释放）
			// 2. 锁被其他 client 持有（不应该发生，但要处理）
			return
		}

		// 锁确实被当前 client 持有，尝试释放
		err = lk.ck.Put(lk.lockname, unlocked, version)

		if err == rpc.OK {
			// 成功释放锁
			return
		}

		if err == rpc.ErrMaybe {
			// Put 可能成功也可能失败，需要检查当前状态
			newValue, _, getErr := lk.ck.Get(lk.lockname)
			
			if getErr != rpc.OK {
				// Get 失败，无法确认状态，继续重试
				time.Sleep(10 * time.Millisecond)
				continue
			}

			if newValue == unlocked {
				// 锁已经是 unlocked 状态，说明释放成功
				return
			}

			if newValue != expectedValue {
				// 锁被其他 client 持有了
				// 这意味着我们的 Release 已经成功，然后其他 client 获取了锁
				return
			}

			// newValue == expectedValue: 锁还是被我们持有，说明 Put 失败了
			// 继续重试
		}

		// err == rpc.ErrVersion: version 不匹配
		// 可能其他操作修改了锁（不应该发生，但继续重试）
		time.Sleep(10 * time.Millisecond)
	}
}