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
		err_msg := ""
		if msg.SnapshotValid {
			// install snapshot
			err_msg = rsm.ingestSnap(msg.Snapshot, msg.SnapshotIndex)
		} else if msg.CommandValid {
			if msg.CommandIndex != rsm.lastApplied+1 {
				// 判断是不是应该apply的log entry, cmd执行的顺序应该与log的记录顺序一致
				err_msg = "rsm apply out of order"
			}
			rsm.lastApplied = msg.CommandIndex

			result := rsm.sm.DoOp(msg.Command.(Op).Req)

			// 1.判断是否需要快照
			if rsm.maxraftstate != -1 && float32(rsm.rf.PersistBytes()) > float32(rsm.maxraftstate)*0.9 {
				// 创建快照
				snapshot := rsm.sm.Snapshot() // 获取kvServer的snapshot
				rsm.rf.Snapshot(msg.CommandIndex, snapshot)
			} else {
				// Ignore other types of ApplyMsg
			}

			// 2.如果是自己submit的cmd，需要自己唤醒需要的submit线程
			if msg.Command.(Op).Me != rsm.me {
				// 如果submitServer不是自己，说明自己不是leader，仅执行
				continue
			}

			rsm.mu.Lock()
			if ch, ok := rsm.pending[msg.Command.(Op).Id]; ok {
				rsm.results[msg.Command.(Op).Id] = result
				rsm.mu.Unlock()
				ch <- struct{}{}
				continue
			}
			rsm.mu.Unlock()

		} else {
			// Ignore other types of ApplyMsg.
		}
		if err_msg != "" {
			fmt.Println(err_msg)
		}
	}
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	rsm.mu.Lock()
	id := rsm.nextId
	op := Op{Me: rsm.me, Id: id, Req: req}
	ch := make(chan struct{})
	rsm.pending[rsm.nextId] = ch
	rsm.nextId++
	rsm.mu.Unlock()

	_, submitTerm, submitIsLeader := rsm.Raft().Start(op)

	if !submitIsLeader {
		return rpc.ErrWrongLeader, nil
	}

	start := time.Now()
	for {
		// 判断是否term没变
		// 判断ch是否有通知
		// 如果超时10秒，删除map中id对应的元素

		curTerm, _ := rsm.Raft().GetState()
		rsm.mu.Lock()
		if time.Since(start) >= 10*time.Second || curTerm != submitTerm {
			reply, ok := rsm.results[id]
			delete(rsm.results, id)
			if ch, ok := rsm.pending[id]; ok {
				close(ch)
				delete(rsm.pending, id)
			}

			if ok {
				rsm.mu.Unlock()
				return rpc.OK, reply
			} else {
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		}
		rsm.mu.Unlock()

		select {
		case _, ok := <-ch:
			rsm.mu.Lock()
			reply, ok := rsm.results[id]

			delete(rsm.results, id)
			if ch, ok := rsm.pending[id]; ok {
				close(ch)
				delete(rsm.pending, id)
			}
			rsm.mu.Unlock()
			if ok {
				return rpc.OK, reply
			} else {
				return rpc.ErrWrongLeader, nil
			}

		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rsm *RSM) ingestSnap(snapshot []byte, index int) string {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if snapshot == nil {
		return "nil snapshot"
	}
	rsm.sm.Restore(snapshot)
	rsm.lastApplied = index
	return ""
}
