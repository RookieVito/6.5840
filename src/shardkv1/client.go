package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"sync"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.
	cfg *shardcfg.ShardConfig
	mu  sync.Mutex
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)

	// 找到初始化配置
	ck.cfg = ck.sck.Query()

	// 初始化shard group的clerk
	for gid, srvs := range ck.cfg.Groups {
		ck.rcks[gid] = shardgrp.MakeClerk(clnt, srvs)
	}
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		ck.mu.Lock()
		shard := shardcfg.Key2Shard(key) // key -> shard
		gid := ck.cfg.Shards[shard]      // shard -> group
		grpCk, ok := ck.GetClerk(gid)    // group -> group.clerk
		ck.mu.Unlock()
		if ok {
			value, version, err := grpCk.Get(key)
			if err == rpc.OK || err == rpc.ErrNoKey {
				return value, version, err
			}
			switch err {
			case rpc.OK:
				return value, version, err
			case rpc.ErrNoKey:
				return value, version, err
			default:
				// err == rpc.ErrWrongGroup || err == rpc.ErrMaybe
				ck.refreshConfig()
			}
		} else {
			// 没有找到group对应的clerk，说明配置应该更新
			ck.refreshConfig()
		}
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	maybeLost := false
	for {
		ck.mu.Lock()
		shard := shardcfg.Key2Shard(key) // key -> shard
		gid := ck.cfg.Shards[shard]      // shard -> group
		grpCk, ok := ck.GetClerk(gid)    // group -> group.clerk
		ck.mu.Unlock()
		var err rpc.Err
		if ok {
			// 找到了 group的clerk
			var ambiguous bool
			err, ambiguous = grpCk.Put(key, value, version)
			maybeLost = maybeLost || ambiguous
			switch err {
			case rpc.ErrMaybe:
				return err
			case rpc.ErrVersion:
				if maybeLost {
					return rpc.ErrMaybe
				}
				return err
			case rpc.OK:
				return err
			default:
				// err == rpc.ErrWrongGroup
				ck.refreshConfig()
			}
		} else {
			// err == rpc.ErrWrongGroup || ok == false
			// 组错误或者没有找到group对应的clerk，说明配置应该更新
			ck.refreshConfig()
		}
	}
}

func (ck *Clerk) refreshConfig() {

	cfg := ck.sck.Query()
	// fmt.Println("shard clerk: refreshConfig: ", cfg.String())
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.cfg = cfg
	for gid, srvs := range ck.cfg.Groups {
		if _, exists := ck.rcks[gid]; !exists {
			ck.rcks[gid] = shardgrp.MakeClerk(ck.clnt, srvs)
		}
	}
}
