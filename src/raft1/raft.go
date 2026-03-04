package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// ServerState 表示 Raft 服务器的角色状态
type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// Raft 超时配置常量
const (
	// 心跳间隔：Leader 定期发送心跳的时间间隔
	HeartbeatInterval = 100 * time.Millisecond

	// Ticker 检查间隔：ticker 循环检查的睡眠时间
	TickerInterval = 50 * time.Millisecond

	// 选举超时范围：服务器在未收到心跳时等待的最小/最大时间
	// Raft 论文建议：election timeout >> heartbeat interval
	// 这里设置为 500-1000ms，心跳间隔 100ms
	ElectionTimeoutMin = 500 * time.Millisecond
	ElectionTimeoutMax = 1000 * time.Millisecond
)

// LogEntry 表示 Raft 日志条目
type LogEntry struct {
	Command interface{} // 日志命令
	Term    int         // 收到该条目时的 leader term
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// 持久化状态 (Persistent state on all servers)
	currentTerm int        // 服务器已知的最新 term（首次初始化为 0）
	votedFor    int        // 当前 term 收到选票的 candidateId（如果没有则为 -1）
	log         []LogEntry // 日志条目数组

	// 易失性状态（所有服务器）
	commitIndex int // 已提交的最高日志条目索引（初始值为 0）
	lastApplied int // 已应用到状态机的最高日志条目索引（初始值为 0）

	// 易失性状态（仅在领导者上）
	nextIndex  []int // 对于每个服务器，要发送的下一个日志条目索引
	matchIndex []int // 对于每个服务器，已复制的最高日志条目索引

	// 额外状态
	state             ServerState           // 服务器角色（Follower/Candidate/Leader）
	applyCh           chan raftapi.ApplyMsg // 提交消息通道
	snapshot          []byte                // 快照数据（3D）
	lastIncludedIndex int                   // 快照包含的最后日志索引（3D）
	lastIncludedTerm  int                   // 快照包含的最后日志 term（3D）

	// 选举超时相关
	electionTimeout time.Time // 下一次选举超时时间
	votesReceived   int       // 当前选举中收到的选票数
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1 // 新任期内还未投票
	rf.votesReceived = 0
	rf.persist()
	// 重置选举计时器
	rf.resetElectionTimeout()
}

// becomeCandidate 将服务器转换为 Candidate 状态
func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.votesReceived = 0
	rf.resetElectionTimeout()
}

// becomeLeader 将服务器转换为 Leader 状态
// TODO: 日志压缩处理
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.votesReceived = 0
	// 初始化 leader 的 nextIndex 和 matchIndex
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + rf.lastIncludedIndex
		rf.matchIndex[i] = 0
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var snapshotIndex int
	var snapshotTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil ||
		d.Decode(&snapshotIndex) != nil || d.Decode(&snapshotTerm) != nil {
		// error...
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = make([]LogEntry, len(log))
		copy(rf.log, log)
		rf.lastIncludedIndex = snapshotIndex
		rf.lastIncludedTerm = snapshotTerm
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// GetState 返回当前 term 和是否是 leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. 检查 index 是否有效
	if index <= rf.lastIncludedIndex {
		return // 快照已过时或重复
	}

	// 2. index是否在日志范围内
	if index > rf.lastIncludedIndex+len(rf.log)-1 {
		return // index 超出日志范围
	}

	//
	if index > rf.commitIndex {
		return
	}

	// 3. 计算要保留的日志起始位置
	logIndex := index - rf.lastIncludedIndex // log 数组中的索引
	term := rf.log[logIndex].Term

	// 4. 更新快照状态
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	rf.snapshot = snapshot

	//5. 日志压缩
	rf.log = append([]LogEntry{{Command: nil, Term: term}}, rf.log[logIndex+1:]...)

	rf.persist()
}

// RequestVoteArgs RequestVote RPC 参数结构体
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply RequestVote RPC 响应结构体
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// 1. 任期检查
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// 2. 发现更高任期，转为 Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	reply.Term = rf.currentTerm

	// 3. 检查是否已经投票
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
		return
	}

	// 4. 检查候选人的日志是否至少和自己一样新（log up-to-date check）
	lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
	lastLogTerm := rf.lastIncludedTerm
	if len(rf.log) > 1 {
		lastLogTerm = rf.log[lastLogIndex-rf.lastIncludedIndex].Term
	}

	// 候选者的日志至少和自己一样新
	// 判断依据：首先判断term，如果term更大则投票，其次如果term相同则比较index，index更大则投票
	logIsUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		// 候选人最后日志的 term 更大，日志更新
		logIsUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		// term 相同，但候选人的日志至少一样长
		logIsUpToDate = true
	}

	if !logIsUpToDate {
		reply.VoteGranted = false
		return
	}

	// 5. 投票给该候选人
	rf.votedFor = args.CandidateId
	rf.persist()
	rf.resetElectionTimeout() // 重置选举定时器
	reply.VoteGranted = true
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs AppendEntries RPC 参数结构体
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

// AppendEntriesReply AppendEntries RPC 响应结构体
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching PrevLogIndex/Term

	// 用于快速匹配冲突的优化字段
	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

// AppendEntries AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	// 1. 任期检查
	if args.Term < rf.currentTerm {
		// Leader 任期过时
		reply.Term = rf.currentTerm
		return
	}

	// 2. 发现更高或相等任期，转为 Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else {
		//args.Term == rf.currentTerm
		if rf.state != Follower {
			rf.state = Follower
		}
	}
	reply.Term = rf.currentTerm

	// 3. 重置选举计时器（收到有效的 Leader 消息）
	rf.resetElectionTimeout()

	// 4. 日志一致性检查
	// if args.PrevLogIndex >= len(rf.log) {
	// 	// Follower 日志太短
	// 	reply.XTerm = -1
	// 	reply.XIndex = len(rf.log) + rf.lastIncludedIndex
	// 	reply.XLen = len(rf.log) + rf.lastIncludedIndex
	// 	return
	// }

	// if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	// 日志不匹配
	// 	reply.XTerm = rf.log[args.PrevLogIndex].Term

	// 	// 找到该任期的第一个索引
	// 	conflictIndex := args.PrevLogIndex
	// 	for conflictIndex > 0 && rf.log[conflictIndex-1].Term == reply.XTerm {
	// 		conflictIndex--
	// 	}
	// 	reply.XIndex = conflictIndex
	// 	return
	// }
	if args.PrevLogIndex < rf.lastIncludedIndex {
		// PrevLogIndex已经在快照中
		reply.XIndex = len(rf.log) - 1 + rf.lastIncludedIndex
		reply.XTerm = rf.log[len(rf.log)-1].Term
		reply.Success = false
		return
	}

	prevLogIndexInLogArray := args.PrevLogIndex - rf.lastIncludedIndex
	if prevLogIndexInLogArray >= len(rf.log) {
		// 日志过于落后
		reply.XTerm = -1
		reply.XIndex = rf.lastIncludedIndex + len(rf.log)
		reply.XLen = rf.lastIncludedIndex + len(rf.log)
		return
	}

	if rf.log[prevLogIndexInLogArray].Term != args.PrevLogTerm {
		// 日志不匹配
		reply.XTerm = rf.log[prevLogIndexInLogArray].Term
		conflictIndex := prevLogIndexInLogArray
		for conflictIndex > 0 && rf.log[conflictIndex-1].Term == reply.XTerm {
			conflictIndex--
		}
		reply.XIndex = conflictIndex + rf.lastIncludedIndex
		return
	}

	// 5. 日志匹配成功，追加新日志
	nextlogIndexInLogArray := args.PrevLogIndex + 1 - rf.lastIncludedIndex
	entryIndex := 0

	// 找到第一个冲突点
	for nextlogIndexInLogArray < len(rf.log) && entryIndex < len(args.Entries) {
		if rf.log[nextlogIndexInLogArray].Term != args.Entries[entryIndex].Term {
			// 删除从这里开始的所有日志
			rf.log = rf.log[:nextlogIndexInLogArray]
			rf.persist()
			break
		}
		nextlogIndexInLogArray++
		entryIndex++
	}

	// 追加剩余的新日志
	if entryIndex < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[entryIndex:]...)
		rf.persist()
	}

	// 6. 更新 commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastIncludedIndex+len(rf.log)-1)
	}

	reply.Success = true
}

func (rf *Raft) apply() {
	for {
		// rf.mu.Lock()
		// if rf.lastApplied < rf.commitIndex && rf.commitIndex >= 0 {

		// 	msgs := []raftapi.ApplyMsg{}
		// 	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		// 		msgs = append(msgs, raftapi.ApplyMsg{CommandValid: true, Command: rf.log[i-rf.lastIncludedIndex].Command, CommandIndex: i})
		// 	}
		// 	rf.lastApplied = rf.commitIndex
		// 	rf.mu.Unlock()

		// 	for i := range msgs {
		// 		rf.applyCh <- msgs[i]
		// 	}

		// } else {
		// 	rf.mu.Unlock()
		// }
		// time.Sleep(5 * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
		rf.mu.Lock()
		// 优先检查是否有待发送的快照
		// lastApplied < lastIncludedIndex 说明 InstallSnapshot 安装了新快照
		if rf.lastApplied < rf.lastIncludedIndex {
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.lastApplied = rf.lastIncludedIndex
			rf.mu.Unlock()
			rf.applyCh <- msg // 此时不持锁，不会死锁
			continue
		}

		// 发送普通的Command
		if rf.lastApplied >= rf.commitIndex {
			// 没有需要发送的command
			rf.mu.Unlock()
			continue
		}

		// 收集需要apply的Command
		msgs := []raftapi.ApplyMsg{}
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[i-rf.lastIncludedIndex].Command,
				CommandIndex: i,
			})
		}
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
}

// sendAppendEntries sends an AppendEntries RPC to a server.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// InstallSnapshotArgs InstallSnapshot RPC 参数结构体 (3D)
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply InstallSnapshot RPC 响应结构体 (3D)
type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot InstallSnapshot RPC handler (3D)
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	// 1. 任期检查
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	// 2. 发现更高任期，转为 Follower
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	} else {
		if rf.state != Follower {
			rf.state = Follower
		}
	}
	reply.Term = rf.currentTerm

	// 3. 重置选举计时器
	rf.resetElectionTimeout()

	// 4. 检查快照是否过时
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		// 已经有更新或相同的快照
		rf.mu.Unlock()
		return
	}

	// 计算快照LastIncludedIndex在当前 log 中的位置
	logIndex := args.LastIncludedIndex - rf.lastIncludedIndex

	// 5. 安装快照
	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// 6. 更新日志
	// 如果现有日志中有与快照冲突的条目，删除整个日志
	// 如果现有日志在快照之后有条目，保留这些条目

	// 情况1: 快照完全覆盖了所有日志
	if logIndex >= len(rf.log) {
		// 丢弃所有日志，只保留新哨兵
		rf.log = []LogEntry{{Command: nil, Term: args.LastIncludedTerm}}
	} else {
		// 情况2: 日志中还有快照之后的条目
		// 检查快照边界处的日志是否匹配
		if rf.log[logIndex].Term == args.LastIncludedTerm {
			// 匹配，保留快照之后的日志
			newLog := make([]LogEntry, 0, len(rf.log)-logIndex)
			newLog = append(newLog, LogEntry{Command: nil, Term: args.LastIncludedTerm}) // 新哨兵
			newLog = append(newLog, rf.log[logIndex+1:]...)
			rf.log = newLog
		} else {
			// 不匹配，丢弃所有日志
			rf.log = []LogEntry{{Command: nil, Term: args.LastIncludedTerm}}
		}
	}

	// 7. 更新状态机的 commitIndex 和 lastApplied
	// 快照已经包含了所有 <= lastIncludedIndex 的日志
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	rf.lastApplied = 0
	// 8. 持久化状态
	rf.persist()
	rf.mu.Unlock()
}

// 处理发送InstallSnapshot的Reply
func (rf *Raft) handleInstallSnapshotReply(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 检查任期
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	// 确保仍然是 Leader 且任期未变
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}

	// 更新 nextIndex 和 matchIndex
	// 快照成功安装，说明 follower 至少有了 lastIncludedIndex 的日志
	if rf.matchIndex[server] < args.LastIncludedIndex {
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1
	}
}

// sendInstallSnapshot sends an InstallSnapshot RPC to a server (3D).
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return index, term, false
	}
	term = rf.currentTerm
	index = len(rf.log) + rf.lastIncludedIndex // 新的entry的索引
	rf.log = append(rf.log, LogEntry{Command: command, Term: term})
	rf.persist()
	rf.mu.Unlock()
	// go rf.broadcastAppendEntries(),由peerLoop定期感知发送

	return index, term, true
}

// resetElectionTimeout 重置选举超时时间为一个随机的未来时间点
func (rf *Raft) resetElectionTimeout() {
	// 在 ElectionTimeoutMin 到 ElectionTimeoutMax 之间随机选择
	randDuration := ElectionTimeoutMin +
		time.Duration(rand.Int63()%int64(ElectionTimeoutMax-ElectionTimeoutMin))
	rf.electionTimeout = time.Now().Add(randDuration)
}

// 决定发送 AppendEntries 还是 InstallSnapshot
// 暂时只发送AppendEntries
func (rf *Raft) sendAppendOrSnapshotOnce(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	nextIdx := rf.nextIndex[server]

	if nextIdx <= rf.lastIncludedIndex {
		// 发送 InstallSnapshot RPC
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.snapshot,
		}
		reply := &InstallSnapshotReply{}
		rf.mu.Unlock()

		// 发送 RPC
		ok := rf.sendInstallSnapshot(server, args, reply)
		if !ok {
			return
		}

		// 处理响应
		rf.handleInstallSnapshotReply(server, args, reply)
	} else {
		// 发送AppendEntries RPC
		prevLogIndex := nextIdx - 1
		var prevLogTerm int
		if prevLogIndex == rf.lastIncludedIndex {
			prevLogIndex = rf.lastIncludedIndex
			prevLogTerm = rf.lastIncludedTerm
		} else {
			inlogIndex := prevLogIndex - rf.lastIncludedIndex // 计算prevLogIndex在log中的索引
			prevLogTerm = rf.log[inlogIndex].Term
		}

		// 准备发送的日志条目
		logArrayStartIndex := nextIdx - rf.lastIncludedIndex          // 计算复制日志的起始索引
		entries := make([]LogEntry, len(rf.log[logArrayStartIndex:])) // 准备日志数据
		copy(entries, rf.log[logArrayStartIndex:])

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		reply := &AppendEntriesReply{}
		rf.mu.Unlock()

		// 发送RPC
		ok := rf.sendAppendEntries(server, args, reply)

		if !ok {
			return
		}
		// 处理响应
		rf.handleAppendEntriesReply(server, args, reply)
	}
}

// updateCommitIndex 尝试更新 commitIndex
// TODO: 考虑日志压缩的情况
func (rf *Raft) updateCommitIndex() {
	// 从 commitIndex+1 开始向后查找
	for i := rf.commitIndex + 1; i < rf.lastIncludedIndex+len(rf.log); i++ {
		// 只能提交当前任期的日志条目（Raft 论文 Figure 8 的要求）
		if rf.log[i-rf.lastIncludedIndex].Term != rf.currentTerm {
			continue
		}

		// 统计有多少个服务器已经复制了这条日志
		count := 1 // leader 自己
		for k := 0; k < len(rf.peers); k++ {
			if k != rf.me && rf.matchIndex[k] >= i {
				count++
			}
		}

		// 如果过半，更新 commitIndex
		if count > len(rf.peers)/2 {
			rf.commitIndex = i
		}
	}
}

// handleRequestVoteReply 处理 RequestVote RPC 的响应
func (rf *Raft) handleRequestVoteReply(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. 检查reply中的term
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}

	// 2. 确保仍然是candidate且term未变
	if rf.state != Candidate || args.Term != rf.currentTerm {
		// 已经成为了leader或者成为了follower
		return
	}

	// 3. 处理投票结果
	if reply.VoteGranted {
		rf.votesReceived++
		//4. 检查是否获得过半选票
		if rf.votesReceived > len(rf.peers)/2 {
			rf.becomeLeader()
		}
	}
}

// handleAppendEntriesReply 处理 AppendEntries RPC 的响应
func (rf *Raft) handleAppendEntriesReply(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 检查reply中的term
	if reply.Term > rf.currentTerm {
		// 发现更高任期，转为 Follower
		rf.becomeFollower(reply.Term)
		return
	}

	// 确保仍然时leader且term未变
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}

	if reply.Success {
		// 更新 nextIndex 和 matchIndex
		matchIndex := args.PrevLogIndex + len(args.Entries)
		if rf.matchIndex[server] < matchIndex {
			rf.matchIndex[server] = matchIndex
			rf.nextIndex[server] = rf.matchIndex[server] + 1
		}

		// 尝试更新 commitIndex
		rf.updateCommitIndex()
	} else {

		// // 确保这是基于当前 nextIndex 发出的请求，防止是一个过期的reply
		// if args.PrevLogIndex+1 == rf.nextIndex[server] && rf.nextIndex[server] > 1 {
		// 	rf.nextIndex[server]--
		// }
		if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XLen
		} else {
			// 情况一和二：存在冲突term
			// 在leader的日志中查找是否有XTerm
			lastXTermIndex := -1
			for i := len(rf.log) - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					lastXTermIndex = i
					break
				}
				// 如果遇到更小的 term，说明 leader 没有 XTerm
				if rf.log[i].Term < reply.XTerm {
					break
				}
			}

			if lastXTermIndex == -1 {
				// 情况一：leader没有XTerm，设置可能为lastIncludedIndex，触发发送Snapshot
				rf.nextIndex[server] = max(reply.XIndex, rf.lastIncludedIndex)
			} else {
				// 情况二：leader有XTerm，从XTerm的下一个Term的第一个日志开始复制
				rf.nextIndex[server] = max(lastXTermIndex+rf.lastIncludedIndex+1, rf.lastIncludedIndex+1)
			}
		}
	}
}

func (rf *Raft) ticker() {
	for true {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Follower:
			rf.followerTicker()
		case Candidate:
			rf.candidateTicker()
		case Leader:
			rf.leaderTicker()
		}
	}
}

func (rf *Raft) followerTicker() {
	for true {
		rf.mu.Lock()
		if rf.state != Follower {
			rf.mu.Unlock()
			return
		}

		if time.Now().After(rf.electionTimeout) {
			// 判断是否超时，超时则转换为Candidate
			rf.becomeCandidate()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(TickerInterval)
	}
}

func (rf *Raft) candidateTicker() {
	for {
		rf.mu.Lock()
		if rf.state != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// 开启一轮选举，会阻塞直到选举结束或超时
		rf.startElection()
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state != Candidate {
		rf.mu.Unlock()
		return
	}

	// 1. 增加任期并投票给自己
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesReceived = 1 // 自己的一票
	rf.persist()

	// 2. 重置选举超时
	rf.resetElectionTimeout()

	// 3. 准备requestVote参数
	currentTerm := rf.currentTerm
	candidateId := rf.me
	lastLogIndex := rf.lastIncludedIndex + len(rf.log) - 1
	lastLogTerm := rf.lastIncludedTerm
	if len(rf.log) > 1 {
		lastLogTerm = rf.log[lastLogIndex-rf.lastIncludedIndex].Term
	}
	serverNum := len(rf.peers)
	rf.mu.Unlock()

	for i := 0; i < serverNum; i++ {
		if rf.me == i {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         currentTerm,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				rf.handleRequestVoteReply(server, args, reply)
			}
		}(i)
	}

	// 等待选举结果或超时
	for {
		time.Sleep(TickerInterval)
		rf.mu.Lock()
		// 检查身份
		if rf.state != Candidate {
			rf.mu.Unlock()
			return
		}

		// 检查任期是否变化
		if rf.currentTerm != currentTerm {
			rf.mu.Unlock()
			return
		}

		if time.Now().After(rf.electionTimeout) {
			// 选举超时，重新开始选举
			rf.mu.Unlock()
			return // 返回到 candidateTicker 重新开始选举
		}

		rf.mu.Unlock()
	}
}

// 向所有服务器发送 AppendEntries RPC（心跳或日志复制）
func (rf *Raft) broadcastAppendEntries() {
	// 向每个peer发送AppendEntries
	for peer := 0; peer < len(rf.peers); peer++ {
		if peer != rf.me {
			go rf.sendAppendOrSnapshotOnce(peer)
		}
	}
}

func (rf *Raft) leaderTicker() {
	// 为每个 follower 启动一个独立的协程，负责发送心跳或日志复制
	for i := range len(rf.peers) {
		if rf.me == i {
			continue
		}
		go rf.peerLoop(i)
	}

	// 维持leader ticker，仅身份转变时退出
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(TickerInterval)
	}
}

// 为每个follower维护一个独立的协程，负责发送心跳或日志复制
// 第一次进入该循环的的时候初始化lastCommitIndex和lastAppendEntriesSent
// 分别记录上次发送时leader的commitIndex和上次发送AppendEntries的时间
// 如果commitIndex有更新，立即发送AppendEntries
// 否则如果心跳间隔到达，发送AppendEntries
// func (rf *Raft) peerLoop(peer int) {
// 	lastHeartbeat := time.Now()
// 	lastCommitIndex := rf.commitIndex // 初始化时获取一次

// 	for {
// 		rf.mu.Lock()
// 		if rf.state != Leader {
// 			rf.mu.Unlock()
// 			return
// 		}

// 		commitChanged := rf.commitIndex != lastCommitIndex             // commitIndex 变化
// 		heartbeatDue := time.Since(lastHeartbeat) >= HeartbeatInterval // 心跳超时

// 		currentCommitIndex := rf.commitIndex
// 		rf.mu.Unlock()

// 		if commitChanged || heartbeatDue {
// 			go rf.sendAppendOrSnapshotOnce(peer)
// 			lastCommitIndex = currentCommitIndex
// 			lastHeartbeat = time.Now()
// 		}

//			time.Sleep(TickerInterval) // 统一在循环末尾
//		}
//	}
//
// 每隔心跳发送
func (rf *Raft) peerLoop(peer int) {
	lastLogLen := 0
	lastSendTime := time.Time{}
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		currentLogLen := len(rf.log)
		logChanged := currentLogLen != lastLogLen
		heartbeatDue := time.Since(lastSendTime) >= HeartbeatInterval

		if logChanged || heartbeatDue {
			lastLogLen = currentLogLen
			rf.mu.Unlock()
			lastSendTime = time.Now()
			go rf.sendAppendOrSnapshotOnce(peer)
		} else {
			rf.mu.Unlock()
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 初始化持久化状态 (3A, 3B, 3C)
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{
		{Command: nil, Term: 0}, // 哨兵
	}

	// 初始化易失性状态（所有服务器）
	rf.commitIndex = 0
	rf.lastApplied = 0

	// 初始化易失性状态（仅在领导者上）
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// 初始化额外状态
	rf.state = Follower
	rf.applyCh = applyCh
	rf.snapshot = nil
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// 初始化选举超时时间
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// 检查是否有持久化的快照
	if len(persister.ReadSnapshot()) > 0 {
		rf.snapshot = persister.ReadSnapshot()
		//如果有快照，说明commitIndex至少也是lastIncludeIndex
		rf.commitIndex = rf.lastIncludedIndex
		// rf.lastApplied（0） < rf.lastIncludedIndex，说明有快照需要安装，apply会执行应用快照
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply()

	return rf
}
