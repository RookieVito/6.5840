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

	frozen   map[shardcfg.Tshid]bool // 管理是否被某分片被冻结
	shardNum map[shardcfg.Tshid]shardcfg.Tnum
	// shardCfgNum shardcfg.Tnum           // 当前正在执行的配置版本
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
		// fmt.Println("Err", reply.Err)
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
	e.Encode(kv.frozen)
	// e.Encode(kv.shardCfgNum)
	e.Encode(kv.shardNum)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvmap map[shardcfg.Tshid]map[string]ValueEntry
	var frozen map[shardcfg.Tshid]bool
	// var shardCfgNum shardcfg.Tnum
	var shardNum map[shardcfg.Tshid]shardcfg.Tnum
	if d.Decode(&kvmap) != nil || d.Decode(&frozen) != nil || d.Decode(&shardNum) != nil {
		fmt.Println("KVstorage restore failed: data is error snapshot len:", len(data))
	} else {
		kv.data = kvmap
		kv.frozen = frozen
		kv.shardNum = shardNum
	}
}

func (kv *KVServer) ownsShardLocked(shard shardcfg.Tshid) bool {
	_, ok := kv.data[shard]
	return ok && !kv.frozen[shard]
}

func (kv *KVServer) getOp(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	shard := shardcfg.Key2Shard(args.Key)

	if !kv.ownsShardLocked(shard) {
		// 分片未安装或已冻结，说明当前分组不负责该分片。
		reply.Err = rpc.ErrWrongGroup
		return
	}

	if val, ok := kv.data[shard][args.Key]; !ok {
		// 没有这个key
		reply.Err = rpc.ErrNoKey
	} else {
		// 有这个key，返回值和版本号
		reply.Value = val.Value
		reply.Version = val.Version
		reply.Err = rpc.OK
	}
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

	shard := shardcfg.Key2Shard(args.Key)

	// 只有当前负责且未冻结的 shard 才能接受读写。
	if !kv.ownsShardLocked(shard) {
		reply.Err = rpc.ErrWrongGroup
		return
	}

	val, ok := kv.data[shard][args.Key]
	if !ok {
		// 没有这个key
		if args.Version != 0 {
			// 版本不对，之前有这个KV，但是被删除了，所以没有key，而且版本不是0
			reply.Err = rpc.ErrVersion
			return
		}
		if kv.data[shard] == nil {
			kv.data[shard] = make(map[string]ValueEntry)
		}
		kv.data[shard][args.Key] = ValueEntry{
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

	if kv.data[shard] == nil {
		kv.data[shard] = make(map[string]ValueEntry)
	}
	kv.data[shard][args.Key] = ValueEntry{
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
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := args.Shard
	if args.Num < kv.shardNum[shard] {
		// 过期的冻结请求，之前已经冻结过了
		reply.Err = rpc.ErrWrongGroup
		return
	}

	// 冻结不改变配置版本号，安装和删除分片时才更新配置版本号
	kv.frozen[shard] = true

	if _, exists := kv.data[args.Shard]; !exists {
		// 如果分片不存在，建立一个空分片
		kv.data[args.Shard] = make(map[string]ValueEntry)
	}

	// 将分片数据制作为状态返回
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data[args.Shard])

	reply.State = w.Bytes()
	reply.Num = args.Num
	reply.Err = rpc.OK
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// fmt.Println("KVServer:FreezeShard")
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.OK {
		reply.Err = rep.(shardrpc.FreezeShardReply).Err
		reply.Num = rep.(shardrpc.FreezeShardReply).Num
		reply.State = rep.(shardrpc.FreezeShardReply).State
	} else {
		reply.Err = err
	}
}

// 只要配置版本号匹配，就安装分片，
func (kv *KVServer) installShardOp(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := args.Shard

	if args.Num < kv.shardNum[shard] {
		// 过期的安装请求，之前已经安装过了
		reply.Err = rpc.ErrWrongGroup
	} else if args.Num == kv.shardNum[shard] {
		// 之前安装过了，响应丢失，controller重试
		// 检查是否安装成功
		if _, exists := kv.data[shard]; exists && !kv.frozen[shard] {
			reply.Err = rpc.OK
		} else {
			// 没有安装成功，不应该出现的错误，因为版本号已经更新了，说明之前安装成功了
			reply.Err = rpc.ErrWrongGroup
		}
	} else {
		// args.Num > kv.shardCfgNum+1：太新的安装请求不可能出现
		// changeConfigTo只接受比当前配置版本号大1的配置更新
		// 所以，controller不可能发出一个版本号大于kv.shardCfgNum+1的安装请求

		// 正常安装
		var kvmap map[string]ValueEntry
		if args.State == nil || len(args.State) == 0 {
			// 空状态（新加入的组），初始化空 map
			kvmap = make(map[string]ValueEntry)
		} else {
			r := bytes.NewBuffer(args.State)
			d := labgob.NewDecoder(r)
			if d.Decode(&kvmap) != nil {
				fmt.Println("installShardOp: decode failed, shard:", args.Shard)
				reply.Err = rpc.ErrWrongGroup
				return
			}
			if kvmap == nil {
				kvmap = make(map[string]ValueEntry)
			}
		}

		kv.data[args.Shard] = kvmap   // 安装分片
		kv.shardNum[shard] = args.Num // 更新配置号
		kv.frozen[shard] = false      // 该分片解冻，配置更新后可以Get/Put
		reply.Err = rpc.OK
	}
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// fmt.Println("KVServer:Install Shard")
	err, rep := kv.rsm.Submit(*args)
	if err == rpc.OK {
		reply.Err = rep.(shardrpc.InstallShardReply).Err
	} else {
		reply.Err = err
	}
}

func (kv *KVServer) deleteShardOp(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := args.Shard

	if args.Num < kv.shardNum[shard] {
		// 过期的删除请求，之前已经删除过了
		reply.Err = rpc.ErrWrongGroup
	} else if args.Num == kv.shardNum[shard] {
		// 之前删除过了，响应丢失，controller重试
		reply.Err = rpc.OK
	} else {
		// args.Num > kv.shardCfgNum+1：太新的删除请求不可能出现
		// changeConfigTo只接受比当前配置版本号大1的配置更新
		// 所以，controller不可能发出一个版本号大于kv.shardCfgNum+1的删除请求

		if kv.frozen[shard] == false {
			// 没有冻结，说明之前的freeze请求丢失了，controller重试，返回错误让controller重试freeze
			reply.Err = rpc.ErrWrongGroup
		} else {
			// 正常删除
			delete(kv.data, shard)
			kv.shardNum[shard] = args.Num
			reply.Err = rpc.OK
		}
	}
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// fmt.Println("KVServer:Delete Shard")
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
	kv.frozen = make(map[shardcfg.Tshid]bool)
	kv.shardNum = make(map[shardcfg.Tshid]shardcfg.Tnum)
	if gid == shardcfg.Gid1 && persister.RaftStateSize() == 0 && persister.SnapshotSize() == 0 {
		// Config #1 assigns all shards to Gid1, so a brand-new Gid1 starts
		// out owning empty maps for every shard.
		for shard := shardcfg.Tshid(0); shard < shardcfg.NShards; shard++ {
			kv.data[shard] = make(map[string]ValueEntry)
			kv.shardNum[shard] = shardcfg.NumFirst
		}
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
