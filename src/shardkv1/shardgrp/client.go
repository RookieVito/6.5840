package shardgrp

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
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
		if time.Since(start) > 5*time.Second {
			return "", 0, rpc.ErrWrongGroup
		}
		reply := rpc.GetReply{}
		ck.mu.Lock()
		leader := ck.leader
		ck.mu.Unlock()
		ok := ck.Call(ck.servers[leader], "KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrWrongLeader {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % len(ck.servers)
				ck.mu.Unlock()
				continue
			} else {
				if reply.Err == rpc.ErrNoKey {
					// 没有该键值对
					return "", 0, rpc.ErrNoKey
				}
				if reply.Err == rpc.OK {
					// Get成功，正常返回
					return reply.Value, reply.Version, reply.Err
				}
				if reply.Err == rpc.ErrWrongGroup {
					return reply.Value, reply.Version, reply.Err
				}
			}
		} else {
			// 当前leader的网络不可靠问题，可能已经不是leader了，换其他的服务器试试
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	count := 1
	start := time.Now()
	for {
		if time.Since(start) > 5*time.Second {
			return rpc.ErrWrongGroup
		}
		reply := rpc.PutReply{}
		ck.mu.Lock()
		leader := ck.leader
		ck.mu.Unlock()
		ok := ck.Call(ck.servers[leader], "KVServer.Put", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrWrongLeader {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % len(ck.servers)
				ck.mu.Unlock()
				count++
				continue
			} else {

				if reply.Err == rpc.OK {
					return rpc.OK
				}

				if reply.Err == rpc.ErrVersion && count == 1 {
					return rpc.ErrVersion
				}

				if reply.Err == rpc.ErrVersion && count != 1 {
					//如果是重发，errmaybe
					return rpc.ErrMaybe
				}

				if reply.Err == rpc.ErrNoKey {
					return rpc.ErrNoKey
				}

				if reply.Err == rpc.ErrMaybe {
					return rpc.ErrMaybe
				}

				if reply.Err == rpc.ErrWrongGroup {
					return rpc.ErrWrongGroup
				}
			}
		} else {
			count++
			ck.mu.Lock()
			ck.leader = (ck.leader + 1) % len(ck.servers)
			ck.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
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
			time.Sleep(10 * time.Millisecond)
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
			if reply.Err == rpc.ErrWrongLeader {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % len(ck.servers)
				ck.mu.Unlock()
				continue
			}
		} else {
			time.Sleep(10 * time.Millisecond)
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

			if reply.Err == rpc.ErrWrongLeader {
				ck.mu.Lock()
				ck.leader = (ck.leader + 1) % len(ck.servers)
				ck.mu.Unlock()
				continue
			}
		} else {
			time.Sleep(10 * time.Millisecond)
		}
		ck.mu.Lock()
		ck.leader = (ck.leader + 1) % len(ck.servers)
		ck.mu.Unlock()
	}
}
