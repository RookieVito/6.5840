package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"fmt"
	"sync"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const configKey string = "shardcfg"

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
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
// 把配置存储在lab2 的kvsrv
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// 传递cfg到kvsrv
	for {
		err := sck.Put(configKey, cfg.String(), rpc.Tversion(0))
		if err == rpc.OK {
			return
		}
		if err == rpc.ErrVersion {
			fmt.Println("init config err version")
			return
		}
		if err == rpc.ErrMaybe {
			value, _, getErr := sck.Get(configKey)
			if getErr == rpc.OK && value == cfg.String() {
				return
			}
		}
	}

}

func (sck *ShardCtrler) updateConfig(new *shardcfg.ShardConfig) {
	// CAS判断写入kvsrv
	for {
		_, ver, err := sck.Get(configKey)
		if err != rpc.OK {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		err = sck.Put(configKey, new.String(), ver)
		if err == rpc.OK {
			return // 成功
		}
		if err == rpc.ErrVersion {
			return // 被其他 controller 抢先，已被 superseded
		}
		if err == rpc.ErrMaybe {
			// 验证是否实际成功
			val, _, getErr := sck.Get(configKey)
			if getErr == rpc.OK {
				if val == new.String() {
					return // 实际成功了
				}
				return // 被 superseded
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
// TODO 更改配置
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	clerks := make(map[tester.Tgid]*shardgrp.Clerk)
	var mu sync.Mutex
	getClerk := func(gid tester.Tgid, srvs []string) *shardgrp.Clerk {
		mu.Lock()
		defer mu.Unlock()
		// 没有则创建，有则直接返回
		if _, ok := clerks[gid]; !ok {
			clerks[gid] = shardgrp.MakeClerk(sck.clnt, srvs)
		}
		return clerks[gid]
	}

	// 完成Freeze/Install/Delete

	// 获取旧的配置
	old := sck.Query()

	//
	var wg sync.WaitGroup
	for shard := shardcfg.Tshid(0); shard < shardcfg.NShards; shard++ {
		oldGid := old.Shards[shard]
		newGid := new.Shards[shard]
		if oldGid == newGid {
			continue // 没有迁移，跳过
		}

		wg.Add(1)
		go func(shard shardcfg.Tshid, oldGid, newGid tester.Tgid) {
			defer wg.Done()

			if oldGid == 0 {
				// 初始状态，该分片从未被分配过，直接安装空状态
				newClerk := getClerk(newGid, new.Groups[newGid])
				newClerk.InstallShard(shard, nil, new.Num)
				return
			}

			if newGid == 0 {
				// 组退出后 Rebalance，理论上不应出现 newGid==0
				// 除非所有组都离开了，此时直接冻结删除
				oldClerk := getClerk(oldGid, old.Groups[oldGid])
				oldClerk.FreezeShard(shard, new.Num)
				oldClerk.DeleteShard(shard, new.Num)
				return
			}

			oldClerk := getClerk(oldGid, old.Groups[oldGid])
			newClerk := getClerk(newGid, new.Groups[newGid])
			state, err := oldClerk.FreezeShard(shard, new.Num) // 1 FreezeShard
			if err != rpc.OK {
				return
			}
			err = newClerk.InstallShard(shard, state, new.Num) // 2 InstallShard
			if err != rpc.OK {
				return
			}
			err = oldClerk.DeleteShard(shard, new.Num) // 3 DeleteShard
		}(shard, oldGid, newGid)
	}
	wg.Wait()

	// 所有迁移完成，更新配置
	sck.updateConfig(new)

}

// Return the current configuration
// 返回当前的配置，负责从kvsrv读取配置
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	for {
		val, _, err := sck.Get(configKey)
		if err == rpc.OK {
			return shardcfg.FromString(val)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
