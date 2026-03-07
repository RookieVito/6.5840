package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
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
	for {
		reply := rpc.GetReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrWrongLeader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
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
			}
		} else {
			// 当前leader的网络不可靠问题，可能已经不是leader了，换其他的服务器试试
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
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
	for {
		reply := rpc.PutReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		if ok {
			if reply.Err == rpc.ErrWrongLeader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
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
			}
		} else {
			count++
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
