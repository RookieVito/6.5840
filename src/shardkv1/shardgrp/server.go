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

	shardNum map[shardcfg.Tshid]shardcfg.Tnum // 为每个 shard管理一个版本号
	frozen   map[shardcfg.Tshid]bool          // 管理是否被某分片被冻结
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
	var shardNum map[shardcfg.Tshid]shardcfg.Tnum
	if d.Decode(&kvmap) != nil || d.Decode(&frozen) != nil || d.Decode(&shardNum) != nil {
		fmt.Println("KVstorage restore failed: data is error snapshot len:", len(data))
	} else {
		kv.data = kvmap
		kv.frozen = frozen
		kv.shardNum = shardNum
	}
}

func (kv *KVServer) getOp(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	shard := shardcfg.Key2Shard(args.Key)

	if kv.frozen[shard] {
		reply.Err = rpc.ErrWrongGroup
		return
	}

	shardData, exists := kv.data[shard]
	if !exists {
		reply.Err = rpc.ErrWrongGroup
		return
	}

	val, ok := shardData[args.Key]
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

	shard := shardcfg.Key2Shard(args.Key)
	if kv.frozen[shard] {
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
	currentNum := kv.shardNum[shard]
	// 配置号必须匹配：只处理比当前配置号更新的冻结请求
	if args.Num < currentNum {
		// 已经处理过更新的配置，拒绝旧请求
		reply.Err = rpc.ErrWrongGroup
		return
	}

	// 幂等：已冻结且配置号相同，直接返回已有数据
	if kv.frozen[shard] && args.Num == currentNum {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		shardData := kv.data[shard]
		if shardData == nil {
			shardData = make(map[string]ValueEntry)
		}
		e.Encode(shardData)
		reply.State = w.Bytes()
		reply.Num = args.Num
		reply.Err = rpc.OK
		return
	}

	// 标记该分片为冻结状态，后续 Get/Put 将拒绝服务
	kv.frozen[shard] = true
	kv.shardNum[shard] = args.Num

	// 序列化该分片数据
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

func (kv *KVServer) installShardOp(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := args.Shard
	currentNum := kv.shardNum[shard]

	// 配置号必须匹配
	if args.Num < currentNum {
		reply.Err = rpc.ErrWrongGroup
		return
	}

	// 幂等：已安装且配置号相同，直接返回成功
	if _, exists := kv.data[shard]; exists && !kv.frozen[shard] && args.Num == currentNum {
		reply.Err = rpc.OK
		return
	}

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

	kv.data[args.Shard] = kvmap
	delete(kv.frozen, args.Shard) // 解冻该分片，允许 Get/Put 服务
	kv.shardNum[shard] = args.Num // 更新配置号

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
	currentNum := kv.shardNum[shard] // 只看该 shard 自己的配置号

	if args.Num > currentNum {
		// 未来的删除请求，拒绝
		reply.Err = rpc.ErrWrongGroup
		return
	}

	delete(kv.data, shard)
	delete(kv.frozen, shard)
	delete(kv.shardNum, shard) // 清除该 shard 的配置号记录

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
	kv.shardNum = make(map[shardcfg.Tshid]shardcfg.Tnum)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
