package kvraft

import (
	"bytes"
	"fmt"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type ValueEntry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	data map[string]ValueEntry
	mu   sync.RWMutex
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
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
	var kvmap map[string]ValueEntry
	if d.Decode(&kvmap) != nil {
		fmt.Println("KVstorage restore failed: data is error snapshot len:", len(data))
	} else {
		kv.data = kvmap
	}
}

func (kv *KVServer) getOp(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.data[args.Key]
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
	val, ok := kv.data[args.Key]
	if !ok {
		// 没有这个key
		if args.Version != 0 {
			// 版本不对，之前有这个KV，但是被删除了，所以没有key，而且版本不是0
			reply.Err = rpc.ErrVersion
			return
		}

		kv.data[args.Key] = ValueEntry{
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

	kv.data[args.Key] = ValueEntry{
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}
	kv.data = make(map[string]ValueEntry)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
