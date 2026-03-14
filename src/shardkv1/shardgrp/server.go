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

	frozen      map[shardcfg.Tshid]bool // 管理是否被某分片被冻结
	shardCfgNum shardcfg.Tnum           // 当前正在执行的配置版本
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
	e.Encode(kv.shardCfgNum)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var kvmap map[shardcfg.Tshid]map[string]ValueEntry
	var frozen map[shardcfg.Tshid]bool
	var shardCfgNum shardcfg.Tnum
	if d.Decode(&kvmap) != nil || d.Decode(&frozen) != nil || d.Decode(&shardCfgNum) != nil {
		fmt.Println("KVstorage restore failed: data is error snapshot len:", len(data))
	} else {
		kv.data = kvmap
		kv.frozen = frozen
		kv.shardCfgNum = shardCfgNum
	}
}

func (kv *KVServer) getOp(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	shard := shardcfg.Key2Shard(args.Key)

	// 判断分片是否冻结
	if kv.frozen[shard] {
		reply.Err = rpc.ErrWrongGroup
		return
	}

	// 分片为空，说明没有这个key
	shardData, exists := kv.data[shard]
	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	}

	val, ok := shardData[args.Key]
	if !ok {
		// 分片中没有数据
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

	shard := shardcfg.Key2Shard(args.Key)

	// 判断分片是否冻结，如果冻结：1. 这个分片要被转移。 2. 这个分片不由自己负责
	if kv.frozen[shard] {
		reply.Err = rpc.ErrWrongGroup
		return
	}

	// 如果shard不存在，创建
	_, exists := kv.data[shard]
	if !exists {
		kv.data[shard] = make(map[string]ValueEntry)
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

	if args.Num < kv.shardCfgNum {
		// args.Num < kv.shardCfgNum+1 网络中过期的请求
		fmt.Println("freezeshardOp err :args.Num < kv.shardCfgNum+1")
		reply.Err = rpc.ErrWrongGroup
		return
	}

	// 冻结分片
	kv.frozen[shard] = true

	// 序列化该分片数据
	if _, exists := kv.data[args.Shard]; !exists {
		// 如果不存在，建立一个空分片
		kv.data[args.Shard] = make(map[string]ValueEntry)
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	shardData := kv.data[args.Shard]
	if shardData == nil {
		shardData = make(map[string]ValueEntry)
	}
	e.Encode(shardData)

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
// 0. 处理时，如果args.Num = kv.shardCfgNum + 1，说明是相邻的配置
// 1. 处理时，如果是args.Num = kv.shardCfgNum，说明之前install的返回可能丢失，controller重试，直接返回ok
// 2. state是空的，那么只更新版本号，这是一个广播
// 3.
func (kv *KVServer) installShardOp(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := args.Shard

	// 版本会在第一次install的时候被设置为最新的版本号，如果一个配置中有多个分片迁移过来，
	// 那么，应该允许修改
	if args.Num < kv.shardCfgNum {
		// 旧的rpc，丢弃
		fmt.Println("install err ErrWrongGroup:", args.Num, " != ", kv.shardCfgNum)
		reply.Err = rpc.ErrWrongGroup
		return
	}

	if _, exists := kv.data[shard]; args.Num == kv.shardCfgNum && exists && !kv.frozen[shard] {
		// 如果配置版本相同，分片存在（之前安装分片成功，响应丢失）
		reply.Err = rpc.OK
		return
	}

	// args.Num == kv.shardCfgNum+1

	// 反序列化分片数据
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

	kv.data[args.Shard] = kvmap // 安装分片
	kv.shardCfgNum = args.Num   // 更新配置号
	kv.frozen[shard] = false    // 该分片解冻，可以Get/Put

	reply.Err = rpc.OK
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

	if args.Num < kv.shardCfgNum {
		// args.Num > kv.shardCfgNum+1：太新的删除请求，不可能，除非changConfigTo没有对freeze的返回值进行判断
		// args.Num < kv.shardCfgNum+1: 过期的删除请求，之前已经删除过了
	}

	if kv.frozen[shard] == true {
		// 删除之前必须被冻结
		delete(kv.data, shard)
		kv.shardCfgNum = args.Num // 配置版本更新
	}

	reply.Err = rpc.OK
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

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	kv.frozen = make(map[shardcfg.Tshid]bool)
	kv.shardCfgNum = 1

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
