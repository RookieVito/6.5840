package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"sync"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	curCfgKey  string = "curCfg"
	nextCfgKey string = "nextCfg"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()
	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	for {
		cur := sck.readConfig(curCfgKey)
		next := sck.readConfig(nextCfgKey)
		if next.Num <= cur.Num {
			return
		}
		sck.finishConfigChange(cur, next)
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
// InitConfig initializes both the committed config and the pending
// config so that recovery always starts from a consistent base.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.ensureConfig(curCfgKey, cfg)
	sck.ensureConfig(nextCfgKey, cfg)
}

// ensureConfig stores cfg under key and treats ErrMaybe as success if a
// follow-up Get confirms the desired value is already present.
func (sck *ShardCtrler) ensureConfig(key string, cfg *shardcfg.ShardConfig) {
	want := cfg.String()
	// CAS判断写入kvsrv
	for {
		val, ver, err := sck.Get(key)
		if err == rpc.OK {
			if val == want {
				return
			}
			err = sck.Put(key, want, ver)
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrMaybe {
				got, _, getErr := sck.Get(key)
				if getErr == rpc.OK && got == want {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if err == rpc.ErrNoKey {
			err = sck.Put(key, want, rpc.Tversion(0))
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrMaybe {
				got, _, getErr := sck.Get(key)
				if getErr == rpc.OK && got == want {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
			continue
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// readConfig blocks until it can read and decode a configuration from
// the controller's backing kvsrv.
func (sck *ShardCtrler) readConfig(key string) *shardcfg.ShardConfig {
	for {
		val, _, err := sck.Get(key)
		if err != rpc.OK {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		return shardcfg.FromString(val)
	}
}

// tryAdvanceConfig CAS-updates one stored configuration from "from" to
// "to". It returns false if another controller has already changed the
// key to a conflicting value.
func (sck *ShardCtrler) tryAdvanceConfig(key string, from, to *shardcfg.ShardConfig) bool {
	want := to.String()
	for {
		val, ver, err := sck.Get(key)
		if err != rpc.OK {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		cur := shardcfg.FromString(val)
		if cur.Num >= to.Num {
			return true
		}
		if cur.Num != from.Num || val != from.String() {
			return false
		}
		err = sck.Put(key, want, ver)
		if err == rpc.OK {
			return true
		}
		if err == rpc.ErrMaybe {
			got, _, getErr := sck.Get(key)
			if getErr == rpc.OK {
				if got == want {
					return true
				}
				if got != from.String() {
					return false
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// configSuperseded reports whether a target configuration has already
// been committed or replaced by a newer pending configuration.
func (sck *ShardCtrler) configSuperseded(target shardcfg.Tnum) bool {
	cur := sck.readConfig(curCfgKey)
	if cur.Num >= target {
		return true
	}
	next := sck.readConfig(nextCfgKey)
	return next.Num > target
}

// getShardClerkFactory memoizes shardgrp clerks per gid so migration
// workers can reuse RPC clients safely across goroutines.
func (sck *ShardCtrler) getShardClerkFactory() func(gid tester.Tgid, srvs []string) *shardgrp.Clerk {
	clerks := make(map[tester.Tgid]*shardgrp.Clerk)
	var mu sync.Mutex
	return func(gid tester.Tgid, srvs []string) *shardgrp.Clerk {
		mu.Lock()
		defer mu.Unlock()
		if _, ok := clerks[gid]; !ok {
			clerks[gid] = shardgrp.MakeClerk(sck.clnt, srvs)
		}
		return clerks[gid]
	}
}

// moveShard executes the migration protocol for a single shard. The
// protocol is written to be retryable so a recovering controller can
// resume an interrupted configuration change.
func (sck *ShardCtrler) moveShard(old, new *shardcfg.ShardConfig, shard shardcfg.Tshid, getClerk func(gid tester.Tgid, srvs []string) *shardgrp.Clerk) {
	oldGid := old.Shards[shard]
	newGid := new.Shards[shard]
	if oldGid == newGid {
		return
	}

	if oldGid == 0 {
		newClerk := getClerk(newGid, new.Groups[newGid])
		for {
			err := newClerk.InstallShard(shard, nil, new.Num)
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrWrongGroup && sck.configSuperseded(new.Num) {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	oldClerk := getClerk(oldGid, old.Groups[oldGid])
	if newGid == 0 {
		for {
			_, err := oldClerk.FreezeShard(shard, new.Num)
			if err != rpc.OK {
				if err == rpc.ErrWrongGroup && sck.configSuperseded(new.Num) {
					return
				}
				time.Sleep(20 * time.Millisecond)
				continue
			}
			break
		}
		for {
			err := oldClerk.DeleteShard(shard, new.Num)
			if err == rpc.OK {
				return
			}
			if err == rpc.ErrWrongGroup && sck.configSuperseded(new.Num) {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	newClerk := getClerk(newGid, new.Groups[newGid])
	var state []byte
	for {
		var err rpc.Err
		state, err = oldClerk.FreezeShard(shard, new.Num)
		if err == rpc.OK {
			break
		}
		if err == rpc.ErrWrongGroup {
			if sck.configSuperseded(new.Num) {
				return
			}
			state = nil
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	for {
		err := newClerk.InstallShard(shard, state, new.Num)
		if err == rpc.OK {
			break
		}
		if err == rpc.ErrWrongGroup && sck.configSuperseded(new.Num) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	for {
		err := oldClerk.DeleteShard(shard, new.Num)
		if err == rpc.OK {
			return
		}
		if err == rpc.ErrWrongGroup && sck.configSuperseded(new.Num) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// finishConfigChange migrates all shards that differ between old and
// new, then commits new as the current configuration.
func (sck *ShardCtrler) finishConfigChange(old, new *shardcfg.ShardConfig) {
	if old.Num >= new.Num {
		return
	}

	getClerk := sck.getShardClerkFactory()
	var wg sync.WaitGroup
	for shard := shardcfg.Tshid(0); shard < shardcfg.NShards; shard++ {
		if old.Shards[shard] == new.Shards[shard] {
			continue
		}
		wg.Add(1)
		go func(shard shardcfg.Tshid) {
			defer wg.Done()
			sck.moveShard(old, new, shard, getClerk)
		}(shard)
	}
	wg.Wait()
	sck.tryAdvanceConfig(curCfgKey, old, new)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
// ChangeConfigTo first records the target in nextCfg, then finishes any
// outstanding migration before advancing curCfg.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		cur := sck.readConfig(curCfgKey)
		if cur.Num >= new.Num {
			return
		}

		next := sck.readConfig(nextCfgKey)
		if next.Num < cur.Num {
			sck.tryAdvanceConfig(nextCfgKey, next, cur)
			continue
		}
		if next.Num > cur.Num {
			sck.finishConfigChange(cur, next)
			continue
		}
		if cur.Num+1 != new.Num {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if !sck.tryAdvanceConfig(nextCfgKey, cur, new) {
			continue
		}
		sck.finishConfigChange(cur, new)
		return
	}
}

// Return the current configuration
// Query returns the latest committed configuration.
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	return sck.readConfig(curCfgKey)
}
