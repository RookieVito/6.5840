package shardgrp

import (
	"bytes"
	"fmt"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type ValueEntry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// data  map[string]ValueEntry
	data map[shardcfg.Tshid]map[string]ValueEntry

	mu sync.RWMutex

	num    shardcfg.Tnum           // 当前最大配置号
	frozen map[shardcfg.Tshid]bool // 管理是否被某分片被冻结
}

func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		var reply rpc.GetReply
		kv.getOp(&args, &reply)
		return reply
	case rpc.PutArgs:
		var reply rpc.PutReply
		kv.putOp(&args, &reply)
		return reply
	case shardrpc.DeleteShardArgs:
		var reply shardrpc.DeleteShardReply
		kv.deleteShardOp(&args, &reply)
		return reply
	case shardrpc.InstallShardArgs:
		var reply shardrpc.InstallShardReply
		kv.installShardOp(&args, &reply)
		return reply
	case shardrpc.FreezeShardArgs:
		var reply shardrpc.FreezeShardReply
		kv.freezeShardOp(&args, &reply)
		return reply
	default:
		fmt.Println(kv.me, " args's type is error")
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data) // 将kvmap制作为快照返回
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvmap map[shardcfg.Tshid]map[string]ValueEntry
	if d.Decode(&kvmap) != nil {
		fmt.Println("KVstorage restore failed: data is error snapshot len:", len(data))
	} else {
		kv.data = kvmap
	}
}

func (kv *KVServer) getOp(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.data[shardcfg.Key2Shard(args.Key)][args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = val.Value
	reply.Version = val.Version
	reply.Err = rpc.OK
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.OK {
		reply.Err = rep.(rpc.GetReply).Err
		reply.Value = rep.(rpc.GetReply).Value
		reply.Version = rep.(rpc.GetReply).Version
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) putOp(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	val, ok := kv.data[shardcfg.Key2Shard(args.Key)][args.Key]
	if !ok {
		// 没有这个key
		if args.Version != 0 {
			// 版本不对，之前有这个KV，但是被删除了，所以没有key，而且版本不是0
			reply.Err = rpc.ErrVersion
			return
		}

		kv.data[shardcfg.Key2Shard(args.Key)][args.Key] = ValueEntry{
			args.Value,
			args.Version + 1,
		}
		reply.Err = rpc.OK
		return
	}
	if val.Version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	kv.data[shardcfg.Key2Shard(args.Key)][args.Key] = ValueEntry{
		args.Value,
		args.Version + 1,
	}
	reply.Err = rpc.OK
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.OK {
		reply.Err = rep.(rpc.PutReply).Err
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) freezeShardOp(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {

	// 压缩保存分片数据
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	sharedData := kv.data[args.Shard]
	if sharedData == nil {
		sharedData = make(map[string]ValueEntry)
	}
	e.Encode(kv.data[args.Shard]) // 将kvmap制作为快照返回
	reply.State = w.Bytes()
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.OK {
		reply.Err = rep.(shardrpc.FreezeShardReply).Err
		reply.Num = rep.(shardrpc.FreezeShardReply).Num
		reply.State = rep.(shardrpc.FreezeShardReply).State
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) installShardOp(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {

	// 安装分片数据
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(args.State)
	d := labgob.NewDecoder(r)
	var kvmap map[string]ValueEntry
	if d.Decode(&kvmap) != nil {
		fmt.Println("KVstorage restore failed: data is error snapshot len:", len(args.State))
	} else {
		if kvmap == nil {
			kvmap = make(map[string]ValueEntry)
		}
		kv.data[args.Shard] = kvmap
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.OK {
		reply.Err = rep.(shardrpc.InstallShardReply).Err
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) deleteShardOp(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	delete(kv.data, args.Shard) // 直接删除整个 shard 的数据
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.OK {
		reply.Err = rep.(shardrpc.DeleteShardReply).Err
	} else {
		reply.Err = err
	}
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.data = make(map[shardcfg.Tshid]map[string]ValueEntry)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
