package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type valueVersion struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]*valueVersion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]*valueVersion),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	vv, ok := kv.data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = vv.value
	reply.Version = vv.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	vv, ok := kv.data[args.Key]
	if !ok {
		// Key does not exist
		if args.Version == 0 {
			// Create new entry with version 1
			kv.data[args.Key] = &valueVersion{
				value:   args.Value,
				version: 1,
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return
	}

	// Key exists
	if vv.version != args.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	// Version matches, update value and increment version
	kv.data[args.Key] = &valueVersion{
		value:   args.Value,
		version: vv.version + 1,
	}
	reply.Err = rpc.OK
}



// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
