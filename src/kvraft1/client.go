package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
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

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	count := 1
	for {
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
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
