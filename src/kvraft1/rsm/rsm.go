package rsm

import (
	"fmt"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	Me  int
	Id  uint64 // 唯一id
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	nextId      uint64 // 唯一自增Id，用于唯一标识每个Op
	pending     map[uint64]chan struct{}
	lastApplied int
	results     map[uint64]any
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		nextId:       0,
		pending:      make(map[uint64]chan struct{}),
		results:      make(map[uint64]any),
		lastApplied:  0,
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.reader()

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.SnapshotValid {
			if err := rsm.ingestSnap(msg.Snapshot, msg.SnapshotIndex); err != "" {
				fmt.Println(err)
			}
			continue
		}

		if !msg.CommandValid {
			continue
		}
		rsm.mu.Lock()
		if msg.CommandIndex != rsm.lastApplied+1 {
			fmt.Println("rsm apply out of order")
		}
		rsm.lastApplied = msg.CommandIndex
		rsm.mu.Unlock()

		op := msg.Command.(Op)
		result := rsm.sm.DoOp(op.Req)

		// 判断是否需要创建快照
		if rsm.maxraftstate != -1 && float32(rsm.rf.PersistBytes()) > float32(rsm.maxraftstate)*0.9 {
			snapshot := rsm.sm.Snapshot()
			rsm.rf.Snapshot(msg.CommandIndex, snapshot)
		}

		// 只通知自己 submit 的请求
		if op.Me != rsm.me {
			continue
		}

		rsm.mu.Lock()
		ch, ok := rsm.pending[op.Id]
		if ok {
			rsm.results[op.Id] = result
			delete(rsm.pending, op.Id) // 先删，防止Submit超时时重复操作
		}
		rsm.mu.Unlock()

		if ok {
			// 用非阻塞发送，防止Submit已超时退出导致永久阻塞
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
}

func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	rsm.mu.Lock()
	id := rsm.nextId
	rsm.nextId++
	op := Op{Me: rsm.me, Id: id, Req: req}
	ch := make(chan struct{}, 1) // 用带缓冲的channel，防止reader阻塞
	rsm.pending[id] = ch
	rsm.mu.Unlock()

	_, submitTerm, submitIsLeader := rsm.Raft().Start(op)

	if !submitIsLeader {
		// 清理pending
		rsm.mu.Lock()
		delete(rsm.pending, id)
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	start := time.Now()
	for {
		select {
		case <-ch:
			rsm.mu.Lock()
			result, ok := rsm.results[id]
			delete(rsm.results, id)
			rsm.mu.Unlock()
			if ok {
				return rpc.OK, result
			}
			return rpc.ErrWrongLeader, nil
		default:
		}

		// term 变更说明 leadership 变了，这条日志大概率丢失
		curTerm, _ := rsm.Raft().GetState()
		if curTerm != submitTerm || time.Since(start) > 10*time.Second {
			rsm.mu.Lock()
			result, ok := rsm.results[id]
			delete(rsm.results, id)
			delete(rsm.pending, id)
			rsm.mu.Unlock()
			if ok {
				return rpc.OK, result
			}
			return rpc.ErrWrongLeader, nil
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (rsm *RSM) ingestSnap(snapshot []byte, index int) string {
	// fmt.Printf("[server %d] ingestSnap index=%d\n", rsm.me, index)
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if snapshot == nil {
		return "nil snapshot"
	}
	rsm.sm.Restore(snapshot)
	rsm.lastApplied = index
	return ""
}
