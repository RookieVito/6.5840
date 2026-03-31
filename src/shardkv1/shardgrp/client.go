package shardgrp

import (
	"fmt"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	// 超时时间
	TIMEOUT = 10 * time.Second
	// 发送最小间隔
	SENDINTERVAL = 50 * time.Millisecond
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
	mu sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	start := time.Now()
	for {
		if time.Since(start) > TIMEOUT {
			// 分片组可能已经被controller迁出了，或者网络异常导致一直无法联系到leader了，返回错误
			return "", 0, rpc.ErrWrongGroup
		}

		reply := rpc.GetReply{}
		ck.mu.Lock()
		leader := ck.leader
		ck.mu.Unlock()
		ok := ck.Call(ck.servers[leader], "KVServer.Get", &args, &reply)

		if ok && reply.Err != rpc.ErrWrongLeader {
			// 网络良好，且leader正确
			// rpc.OK rpc.ErrNoKey rpc.ErrWrongGroup
			switch reply.Err {
			case rpc.ErrWrongGroup:
				return reply.Value, reply.Version, reply.Err
			case rpc.ErrNoKey:
				return "", 0, reply.Err
			case rpc.OK:
				return reply.Value, reply.Version, reply.Err
			default:
				// 不应该出现的错误
				fmt.Printf("unexpected error: %v\n", reply.Err)
				return "", 0, reply.Err
			}
		} else {
			// 1. 网络不良好，重试
			// 2. 网络良好，但leader错误，重试
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
			time.Sleep(SENDINTERVAL)
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) (rpc.Err, bool) {
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	start := time.Now()
	maybeLost := false
	for {
		if time.Since(start) > TIMEOUT {
			return rpc.ErrWrongGroup, maybeLost
		}
		reply := rpc.PutReply{}
		ck.mu.Lock()
		leader := ck.leader
		ck.mu.Unlock()
		ok := ck.Call(ck.servers[leader], "KVServer.Put", &args, &reply)

		if ok && reply.Err != rpc.ErrWrongLeader {
			// 网络良好，且leader正确
			// rpc.OK rpc.ErrVersion rpc.ErrNoKey rpc.ErrWrongGroup
			switch reply.Err {
			case rpc.ErrVersion:
				if maybeLost {
					return rpc.ErrMaybe, true
				}
				return rpc.ErrVersion, false
			case rpc.ErrWrongGroup:
				return rpc.ErrWrongGroup, maybeLost
			case rpc.OK:
				return rpc.OK, false
			default:
				// 不应该出现的错误
				fmt.Printf("unexpected error: %v\n", reply.Err)
				return reply.Err, maybeLost
			}
		}

		// 1. 网络不良好，重试
		// 2. 网络良好，但leader错误，重试
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
		if !ok {
			maybeLost = true
		}
		time.Sleep(SENDINTERVAL)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}
	for {
		reply := shardrpc.FreezeShardReply{}
		ck.mu.Lock()
		leader := ck.leader
		ck.mu.Unlock()
		ok := ck.Call(ck.servers[leader], "KVServer.FreezeShard", &args, &reply)
		// fmt.Println("leader:", ck.servers[ck.leader])
		if ok {
			// 网络良好
			if reply.Err == rpc.ErrWrongLeader {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % len(ck.servers)
				ck.mu.Unlock()
				continue
			}
			// rpc.OK rpc.ErrWrongGroup
			if reply.Err == rpc.OK {
				return reply.State, reply.Err
			}

			if reply.Err == rpc.ErrWrongGroup {
				return reply.State, reply.Err
			}
		} else {
			// 网络不可靠，换节点重试
			time.Sleep(SENDINTERVAL)
		}
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {

	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}
	for {
		reply := shardrpc.InstallShardReply{}
		ck.mu.Lock()
		leader := ck.leader
		ck.mu.Unlock()
		ok := ck.Call(ck.servers[leader], "KVServer.InstallShard", &args, &reply)
		// fmt.Printf("InstallShard: ok=%v server=%v reply=%v\n", ok, ck.servers[ck.leader], reply)
		if ok {
			if reply.Err == rpc.OK {
				return reply.Err
			}
			if reply.Err == rpc.ErrWrongGroup {
				return reply.Err
			}
			if reply.Err == rpc.ErrWrongLeader {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % len(ck.servers)
				ck.mu.Unlock()
				continue
			}
		} else {
			time.Sleep(SENDINTERVAL)
		}
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}
	for {
		reply := shardrpc.DeleteShardReply{}
		ck.mu.Lock()
		leader := ck.leader
		ck.mu.Unlock()
		ok := ck.Call(ck.servers[leader], "KVServer.DeleteShard", &args, &reply)
		// fmt.Printf("DeleteShard: ok=%v server=%v reply=%v\n", ok, ck.servers[ck.leader], reply)
		if ok {
			if reply.Err == rpc.OK {
				return reply.Err
			}
			if reply.Err == rpc.ErrWrongGroup {
				return reply.Err
			}

			if reply.Err == rpc.ErrWrongLeader {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % len(ck.servers)
				ck.mu.Unlock()
				continue
			}
		} else {
			time.Sleep(SENDINTERVAL)
		}
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
	}
}
