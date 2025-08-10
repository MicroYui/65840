package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// 算法浓缩摘要
// - 选举安全：在一个特定的任期内最多可以选出一个 Leader
// - Leader Append-Only：Leader 从不覆盖或删除其日志中的条目；它只追加新条目
// - 日志匹配：如果两个日志包含一个具有相同索引和任期的条目，那么这两个日志的所有条目到给定索引都是相同的
// - Leader 的完整性：如果一个日志条目在某一任期中被 committed，那么该条目将出现在所有更高编号任期的 Leader 的日志中
// - 状态机安全：如果一个服务器在其状态机上应用了一个给定索引的日志条目，那么其他服务器将永远不会为同一索引应用不用的日志条目

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有服务器上的持久性状态
	// 在响应 RPC 之前，在稳定的存储上更新（换言之，任何可能改变这三个字段的操作都需要持久化）
	currentTerm int        // 服务器看到的最新任期（首次启动时初始化为0，单调增加）
	votedFor    int        // 当前任期内获得选票的 Candidate ID（如果没有则为空）
	log         []LogEntry // 日志条目；每个条目包含状态机的命令，以及 Leader 收到条目的任期（第一个索引是1）

	// 所有服务器上的易失性状态
	commitIndex int // 已知被提交的最高日志条目的索引（初始化为0，单调增加）
	lastApplied int // 已应用于状态机的最高日志条目的索引（初始化为0，单调增加）

	// Leader 上的易失性状态
	// 选举后重新初始化
	nextIndex  []int // 对于每个服务器，要发送给该服务器的下一个日志条目的索引（初始化为 Leader 的最后一个日志索引+1）
	matchIndex []int // 对于每个服务器，已知在服务器上复制的最高日志条目索引（初始化为0，单调增加）

	// 新增的用于实现实验的字段
	state        RaftState             // 服务器当前所处的状态
	electionTime time.Time             // 用于记录当前服务器的选举超时时间
	applyCh      chan raftapi.ApplyMsg // 用于向应用层提交信息
	applyCond    *sync.Cond            // 用于通知向应用层提交信息
	votesGranted int                   // 用于记录自身的票数

	// 引入快照后需要持久化的字段
	lastIncludedIndex int // 快照所包含的最后一条日志条目的 index
	lastIncludedTerm  int // 快照所包含的最后一条日志条目的 term
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// 将 absIndex 转换为 rf.log 切片中的相对索引
func (rf *Raft) toSliceIndex(absIndex int) int {
	return absIndex - rf.lastIncludedIndex
}

// 将 sliceIndex 转换为每个日志唯一的索引
func (rf *Raft) toAbsIndex(sliceIndex int) int {
	return sliceIndex + rf.lastIncludedIndex
}

// 获取服务器最新的日志的 index 和 term
func (rf *Raft) getLastIndexAndTerm() (int, int) {
	// index 是日志的长度 -1，因为最开始就有一个空的日志
	// 原来的 index 在引入快照后都变成了 sliceIndex，所以需要 toAbsIndex
	index := len(rf.log) - 1
	lastAbsIndex := rf.toAbsIndex(index)
	// term 是最新的日志的任期，因为前面保持了一致，这里直接索引即可（如第一条日志是log[1]）
	// 但是 term 仍然是最后一个条目的 term
	term := rf.log[index].Term
	return lastAbsIndex, term
}

// 服务器用于重置选举时间
func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	timeout := time.Duration(350+rand.Int63()%150) * time.Millisecond
	rf.electionTime = t.Add(timeout)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.currentTerm, rf.state == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	currentSnapshot := rf.persister.ReadSnapshot()
	rf.persister.Save(raftstate, currentSnapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		log.Fatal("failed to read persist\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

		// 如果我们是从一个包含快照的状态中恢复的，
		// 必须确保 commitIndex 和 lastApplied 至少和快照同步。
		// 这可以防止节点重启后，重新应用已经被快照包含了的日志。
		if rf.lastIncludedIndex > 0 {
			rf.commitIndex = max(rf.commitIndex, rf.lastIncludedIndex)
			rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
		}
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

// 每台服务器独立进行快照，只覆盖日志中已提交的条目
// raft 除了将其状态写入快照，还包含元数据：
// 1. 状态机应用的最后一个条目以及这个条目的 term，这些是为了支持 AppendEntries 一致性检查
// 2. 日志中最后的索引，这是为了面对集群成员的变化
// 3. 一旦服务器完成写入快照，它可以删除所有的日志条目，直到最后包含的索引，以及任何先前的快照
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果要快照的索引不比我们已有的快照新，则忽略
	if index <= rf.lastIncludedIndex {
		return
	}

	// 计算要保留的日志的起始相对索引
	sliceIndex := rf.toSliceIndex(index)

	// 更新快照元数据
	rf.lastIncludedTerm = rf.log[sliceIndex].Term

	// 创建一个新的日志切片
	// 第一个元素是新的 dummy entry，它的 Term 就是快照最后条目的 Term
	newLog := make([]LogEntry, 1)
	newLog[0].Term = rf.lastIncludedTerm

	// 将 index 之后的所有条目追加到新日志中
	newLog = append(newLog, rf.log[sliceIndex+1:]...)

	rf.log = newLog
	rf.lastIncludedIndex = index

	// 确保 commitIndex 和 lastApplied 不会小于快照的 index，防止它们指向已经被丢弃的日志
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	// 在进行快照后需要将新的状态进行持久化
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, snapshot)
}

type InstallSnapshotArgs struct {
	Term              int    // Leader 的任期号
	LeaderId          int    // 以便于 Follower 重定向请求
	LastIncludedIndex int    // 快照会替换所有的条目，直到并包括这个 index
	LastIncludedTerm  int    // 快照中包含的最后日志条目的任期号
	Offset            int    // 快照在快照文件中位置的字节偏移量（在此实验中不需要）
	Data              []byte // 快照分块的原始字节，从 offset 开始
	Done              bool   // 如果这是最后一个分块则为 true（在此实验中不需要）
}

type InstallSnapshotReply struct {
	Term int // 当前任期号，便于 Leader 更新自己
}

// 由 Leader 调用，向 Follower 发送快照的分块。Leader 总是按顺序发送分块
// 1. 如果 term < currentTerm 立即回复
// 2. 如果是第一个块（offset 为 0），创建一个新的快照
// 3. 在指定偏移量写入数据
// 4. 如果 done == false，则回复并等待更多数据
// 5. 保存快照文件，丢弃具有较小索引的已存或部分快照
// 6. 如果现存的日志条目与快照中最后包含的日志条目具有相同的 index 和 term，则保留其后的日志条目并进行回复
// 7. 丢弃整个日志
// 8. 使用快照内容重置状态机（并加载快照的集群配置）
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// 1. 如果 term < currentTerm 立即回复
	if args.Term < rf.currentTerm {
		return
	}

	// 对于所有服务器 2
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		// 这里需要将投票置为 -1，因为在这一任期内，看到了更高的任期，意味着进入新的任期，之前的所有投票都作废了
		rf.votedFor = -1
		// 改变了 currentTerm 和 votedFor，需要持久化
		rf.persist()
	}

	// 对于 Candidates 3
	if rf.state == Candidate && rf.currentTerm == args.Term {
		rf.state = Follower
		// 注意：这里不需要改变 voteFor，因为如果是 Candidate，其在这个任期已经将票投给了自己，如果变为 -1.意味着它又可以投票了
	}

	// 只要收到 InstallSnapshot 消息，都意味着需要重启计时器
	rf.resetElectionTimer()

	// 2 3 4 因为实验要求不需要所以忽略

	// 5. 保存快照文件，丢弃具有较小索引的已存或部分快照
	if args.LastIncludedIndex < rf.lastIncludedIndex {
		return
	}

	lastLogIndex, _ := rf.getLastIndexAndTerm()
	// 6. 如果现存的日志条目与快照中最后包含的日志条目具有相同的 index 和 term，则保留其后的日志条目并进行回复
	if lastLogIndex > args.LastIncludedIndex && rf.log[rf.toSliceIndex(args.LastIncludedIndex)].Term == args.LastIncludedTerm {
		newLog := make([]LogEntry, 1)
		// newLog[0] 是 dummy entry，可以为空或者存储元数据
		newLog = append(newLog, rf.log[rf.toSliceIndex(args.LastIncludedIndex)+1:]...)
		rf.log = newLog
	} else { // 7. 丢弃整个日志
		rf.log = make([]LogEntry, 1) // 只留下一个空的 dummy entry
	}

	// 更新快照元数据
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.log[0].Term = rf.lastIncludedTerm // 更新 dummy entry 的 term

	// 更新 commit 索引
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)

	// 将 Raft 状态和快照数据一起持久化
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, args.Data)

	rf.applyCond.Broadcast()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// 对于服务器的规则
// 对于所有服务器
// 1. 如果 commitIndex > lastApplied：增加 lastApplied，将 log[lastApplied] 应用于状态机
// 2. 如果 RPC 请求或响应包含任期 T > currentTerm：设置 currentTerm = T，转换为 Follower

// 对于 Followers
// 1. 对 Candidate 和 Leader 的 RPC 作出回应
// 2. 如果选举超时，没有收到现任 Leader 的 AppendEntries RPC，也没有给 Candidate 投票：转换为 Candidate

// 对于 Candidates
// 1. 在转换为 Candidate 时，开始选举
//   1.1. 递增 currentTerm
//   1.2. 重置选举定时器
//   1.3. 向所有其他服务器发送 RequestVote RPCs
// 2. 如果获得大多数服务器的投票：成为领导者
// 3. 如果收到来自新领导的 AppendEntries RPC：转换为 Follower
// 4. 如果选举超时：开始新的选举

// 对于 Leaders
// 1. 关于选举：向每个服务器发送初始的空 AppendEntries RPC（心跳）；在空闲也重复，以防止选举超时
// 2. 如果收到来自客户端的命令：将条目追加到本地日志，在条目应用于状态机后作出响应
// 3. 如果一个 Leader 的最后一个日志索引 >= nextIndex：发送 AppendEntries RPC 包含从 nextIndex 开始的日志条目
//   3.1. 如果成功：为 Follower 更新 nextIndex 和 matchIndex
//   3.2. 如果 AppendEntries 因为日志不一致而失败：递减 NextIndex 并重试
// 4. 如果存在一个 N，使得 N > commitIndex，大多数的 matchIndex[i] >= N，并且 log[N].Term == currentTerm：设置 commitIndex = N

type AppendEntriesArgs struct {
	Term         int        // 领导任期
	LeaderId     int        // 使 Candidate 可以为客户端重定向
	PrevLogIndex int        // 紧接在新日志之前的日志条目的索引
	PrevLogTerm  int        // 紧邻新日志条目之前的日志条目的任期
	Entries      []LogEntry // 需要被保存的日志条目（做心跳使用时，内容为空；为了提高效率可一次性发送多个）
	LeaderCommit int        // Leader 的已知已提交的最高的日志条目的索引
}

type AppendEntriesReply struct {
	Term    int  // 当前任期，Leader 会更新自己的任期
	Success bool // 如果 Follower 所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配，则为真

	// 实验提到在拒绝信息中增加内容，即在 AppendEntriesReply 中
	XTerm  int // 冲突条目的 term（如果有的话）
	Xindex int // 冲突 term 的第一个条目的 index（如果有的话）
	Xlen   int // 日志长度
}

// AppendEntries 由 Leader 调用以复制日志条目；也作为心跳使用
// 注意：如果是 Leader 调用则说明 rf 为 Follower 或 Candidate，args 为 Leader 发送来的消息，reply 为回复的消息
// 1. 如果 term < currentTerm，则返回 false
// 2. 如果日志在 prevLogIndex 处不包含 term 与 prevLogTerm 匹配的条目，则返回 false
// 3. 如果一个现有的条目与一个新的条目相冲突（相同的索引但不同的任期），删除现有的条目和后面所有的条目
// 4. 添加日志中任何尚未出现的新条目
// 5. 如果 leaderCommit > commitIndex，设置 commitIndex = min(leaderCommit, 最后一个新条目的索引)

// 日志匹配属性：
// 1. 如果不同日志中的两个条目具有相同的索引和任期，那么它们存储的是同一个命令
// 2. 如果不同日志中的两个条目具有相同的索引和任期，那么日志中的所有前面的条目都是相同的
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false       // 先将回复设置为 true
	reply.Term = rf.currentTerm // 先将回复的 Term 设置为 rf 的 currentTerm

	if args.Term < rf.currentTerm { // AppendEntries 1
		return
	}

	// 对于所有服务器 2
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		// 这里需要将投票置为 -1，因为在这一任期内，看到了更高的任期，意味着进入新的任期，之前的所有投票都作废了
		rf.votedFor = -1
		// 改变了 currentTerm 和 votedFor，需要持久化
		rf.persist()
	}

	// 对于 Candidates 3
	if rf.state == Candidate && rf.currentTerm == args.Term {
		rf.state = Follower
		// 注意：这里不需要改变 voteFor，因为如果是 Candidate，其在这个任期已经将票投给了自己，如果变为 -1.意味着它又可以投票了
	}

	// 只要收到 AppendEntries 消息，都意味着需要重启计时器
	rf.resetElectionTimer()

	if args.PrevLogIndex < rf.lastIncludedIndex {
		return
	}

	// 上面是身份转变，开始对日志进行判断
	// AppendEntries 2：如果日志在 prevLogIndex 处不包含 term 与 prevLogTerm 匹配的条目，则返回 false
	lastLogIndex, _ := rf.getLastIndexAndTerm()
	// 首先 prevLogIndex > lastLogIndex，则少数据，直接返回 false
	if lastLogIndex < args.PrevLogIndex {
		// 少数据了，对应 Leader 的：Follower 的日志太短 -> nextIndex = XLen
		// 给少数据的情况标识为 XTerm = -1
		reply.XTerm = -1
		reply.Xindex = -1
		// 现在 log 的长度不能直接 len() 获取
		reply.Xlen = lastLogIndex + 1
		return
	}

	// 剩下表示索引可以定位，需要比较 term 是否匹配
	// 这里 log 的索引也需要修改
	prevLogTerm := rf.log[rf.toSliceIndex(args.PrevLogIndex)].Term
	if prevLogTerm != args.PrevLogTerm {
		xTerm := prevLogTerm
		// xIndex 在访问 log 时也应该变成绝对索引
		var firstIndexOfXTerm int
		for xIndex := args.PrevLogIndex; xIndex >= rf.lastIncludedIndex; xIndex-- {
			// 注意这里是 -1，因为先要找到不一样的，上一个才是 xterm 的第一个
			if rf.log[rf.toSliceIndex(xIndex)].Term != xTerm {
				firstIndexOfXTerm = xIndex + 1
				break
			}
		}
		if firstIndexOfXTerm == 0 { // 如果没找到
			firstIndexOfXTerm = rf.lastIncludedIndex
		}
		reply.Xindex = firstIndexOfXTerm
		reply.XTerm = xTerm
		// 现在 log 的长度不能直接 len() 获取
		reply.Xlen = lastLogIndex + 1
		return
	}

	// 找到第一个不匹配的条目
	firstMismatch := -1
	for i, entry := range args.Entries {
		// 原来的 index 变成绝对索引，在 log 索引时需要转变成为 sliceIndex
		absIndex := args.PrevLogIndex + 1 + i
		if absIndex > lastLogIndex || rf.log[rf.toSliceIndex(absIndex)].Term != entry.Term {
			firstMismatch = i
			break
		}
	}

	// 如果有不匹配的条目，或者有新条目需要追加
	if firstMismatch != -1 {
		// 这里对 log 进行索引，所以需要转换
		sliceIndexToCut := rf.toSliceIndex(args.PrevLogIndex + 1 + firstMismatch)
		// AppendEntries 3：如果一个现有的条目与一个新的条目相冲突（相同的索引但不同的任期），删除现有的条目和后面所有的条目
		rf.log = rf.log[:sliceIndexToCut]
		// AppendEntries 4：添加日志中任何尚未出现的新条目
		rf.log = append(rf.log, args.Entries[firstMismatch:]...)
		// 改变了 log，需要持久化
		rf.persist()
	}

	// AppendEntries 5：如果 leaderCommit > commitIndex，设置 commitIndex = min(leaderCommit, 最后一个新条目的索引)
	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex, _ = rf.getLastIndexAndTerm()
		rf.commitIndex = min(args.LeaderCommit, lastLogIndex)
		// 当 follower 的 commitIndex 更新后，需要唤醒 applier goroutine 来应用日志
		rf.applyCond.Broadcast()
	}

	// 只要能发送 AppendEntries 的 term >= me，并且能走到最后一步，将回复设置为 true
	reply.Success = true

}

func min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // 候选人的任期号
	CandidateId  int // 请求选票的 Candidate Id
	LastLogIndex int // Candidate 最后日志条目的索引
	LastLogTerm  int // Candidate 最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // 当前任期号，Candidate 会更新自己的任期号
	VoteGranted bool // true 表示 Candidate 获得了选票
}

// RequestVote Candidate 为收集选票而调用
// 注意：这是 Candidate 发送给所有节点的信息，因此 rf 为被发送的节点
// 1. 如果 term < currentTerm，则返回 false
// 2. 如果 votedFor 是 null 或 candidateId，并且 Candidate 的日志至少与接收人的日志一样新，则投票
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm // 当前任期号

	if args.Term < rf.currentTerm { // RequestVote 1
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm { // 对于所有服务器 2
		rf.state = Follower
		rf.currentTerm = args.Term
		// 1. 如果之前是 Candidate 或 Leader，那么在身份转换后自然需要清空投票
		// 2. 如果是 Follower，在到达新任期之后也需要清空投票
		rf.votedFor = -1
		// 如果是 Follower，它收到了一个来自更高任期 T+n 的投票请求。这说明有节点已经开始为 T+n 任期竞选了，因此需要放弃之前的计时器
		rf.resetElectionTimer()
		// 改变了 currentTerm 和 votedFor，需要持久化
		rf.persist()
	}

	// RequestVote 2：用于判断是否投票
	// 判断 candidate 日志是否至少与接收人的日志一样新
	// 1. Candidate 当前 term 已经大于接收人
	// 2. 与 Candidate 任期相等，但是 index >= 接收人
	lastLogIndex, lastLogTerm := rf.getLastIndexAndTerm()
	isUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && isUpToDate {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		// 这里注意：如果投出了赞成票，意味着 Follower 愿意等 Candidate，所以需要重置计时器
		rf.resetElectionTimer()
		// 改变了 votedFor，需要持久化
		rf.persist()
		return
	} else {
		reply.VoteGranted = false
		return
	}
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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	// 如果不是 Leader，直接返回 false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}

	// Leader 2：如果收到来自客户端的命令：将条目追加到本地日志，在条目应用于状态机后作出响应
	// 上层传下来新的 command
	// 1. 将新的日志条目 append 到 log 中
	// 2. 将 AppendEntries 广播出去
	// 3. 返回新的 index，并将 isLeader 改为 true
	newEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newEntry)
	// 改变了 log，需要持久化
	rf.persist()
	rf.sendBroadcast()
	index, term = rf.getLastIndexAndTerm()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		// 如果是 Leader，就发送心跳消息或日志同步
		if rf.state == Leader {
			rf.sendBroadcast()
		}

		// 如果 Follower 发现超时了
		if (rf.state == Follower || rf.state == Candidate) && time.Now().After(rf.electionTime) {
			rf.startElection()
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		ms := 50
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// 发送心跳消息或进行日志同步

// 如果 Follower 的日志于 Leader 的不一致，在下一个 AppendEntries RPC 中，AppendEntries 一致性检查将失败
// 在拒绝之后，Leader 会递减 nextIndex 并重试 AppendEntries RPC
func (rf *Raft) sendBroadcast() {
	leaderTerm := rf.currentTerm
	// 给每一个服务器发送
	for server := range rf.peers {
		// 遇到自己跳过
		if server == rf.me {
			continue
		}
		// 进行非阻塞的 RPC 访问
		go func(server int) {
			rf.mu.Lock()

			// 如果一个 Follower 需要的日志已经被 Leader 快照了
			// 这时候直接将快照发过去让 Follower 复制即可
			if rf.nextIndex[server] <= rf.lastIncludedIndex {
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.persister.ReadSnapshot(),
				}
				rf.mu.Unlock()

				reply := InstallSnapshotReply{}
				if rf.sendInstallSnapshot(server, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// 处理回复前，再次检查自己是否还是当初那个 Leader
					if rf.state != Leader || rf.currentTerm != args.Term {
						return
					}

					// 如果 term 比自己大，直接转变身份变为 Follower
					if reply.Term > rf.currentTerm {
						rf.state = Follower
						rf.votedFor = -1
						rf.currentTerm = reply.Term
						rf.resetElectionTimer()
						// 改变了 currentTerm 和 votedFor，需要持久化
						rf.persist()
					} else { // 快照发送成功，更新 nextIndex 和 matchIndex
						rf.matchIndex[server] = max(rf.matchIndex[server], rf.lastIncludedIndex)
						rf.nextIndex[server] = max(rf.nextIndex[server], rf.lastIncludedIndex+1)
					}

				}
				return
			}

			prevLogIndex := rf.nextIndex[server] - 1
			// 这里对 log 进行访问，Leader 传来的是绝对索引，需要进行索引转换
			prevLogTerm := rf.log[rf.toSliceIndex(prevLogIndex)].Term

			// Leaders rule 3
			var entries []LogEntry
			lastLogIndex, _ := rf.getLastIndexAndTerm()
			if lastLogIndex >= rf.nextIndex[server] {
				// 这里对 log 进行访问，Leader 传来的是绝对索引，需要进行索引转换
				sliceStart := rf.toSliceIndex(rf.nextIndex[server])
				entries = make([]LogEntry, len(rf.log[sliceStart:]))
				copy(entries, rf.log[sliceStart:])
			}

			// 构建心跳信息
			args := &AppendEntriesArgs{
				Term:         leaderTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			if rf.sendAppendEntries(server, args, reply) {
				// 只需检查返回的 Term 是否比自己大即可
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 处理回复前，再次检查自己是否还是当初那个 Leader
				if rf.state != Leader || rf.currentTerm != leaderTerm {
					return
				}

				// 根据回复判断心跳消息发送结果
				if reply.Success {
					// Leaders rule 3.1
					// 因为不一定把所有数据发完，因此需要根据发送的计算新的 index
					// 亦或者是普通心跳信息，这时候本来就是空的，这样会导致数值改变
					matchIndex := prevLogIndex + len(entries)
					nextIndex := matchIndex + 1
					// 同理，可能有多个数据片段返回，如果小的后回来会覆盖，取 max 可以防止此类情况
					rf.nextIndex[server] = max(rf.nextIndex[server], nextIndex)
					rf.matchIndex[server] = max(rf.matchIndex[server], matchIndex)
				} else {
					// 因为自身任期过低而被拒绝
					if reply.Term > rf.currentTerm {
						rf.state = Follower
						rf.votedFor = -1
						rf.currentTerm = reply.Term
						rf.resetElectionTimer()
						// 改变了 currentTerm 和 votedFor，需要持久化
						rf.persist()
					} else { // 数据没有匹配上，需要递减 nextIndex 并重试，Leaders rule 3.2
						// if rf.nextIndex[server] > 1 {
						// 	rf.nextIndex[server]--
						// }
						// Follower 的日志太短
						if reply.XTerm == -1 {
							rf.nextIndex[server] = reply.Xlen
						} else {
							// 检查有无 xTerm
							lastLogIndex, _ = rf.getLastIndexAndTerm()
							haveXTerm := false
							lastIndexOfTerm := -1
							for index := lastLogIndex; index > 0; index-- {
								// 必须先检查 index 是否已经越过快照边界
								if index <= rf.lastIncludedIndex {
									break
								}
								// 这里的 index 是绝对索引，所以需要转换
								if rf.log[rf.toSliceIndex(index)].Term == reply.XTerm {
									haveXTerm = true
									lastIndexOfTerm = index
									break
								}
							}
							if haveXTerm {
								rf.nextIndex[server] = lastIndexOfTerm + 1
							} else {
								rf.nextIndex[server] = reply.Xindex
							}
						}
					}
				}
				// Leaders rule 4
				lastLogIndex, _ = rf.getLastIndexAndTerm()
				// 从最大值开始往下找，找到最大的已经 commit 的 N
				for N := lastLogIndex; N > rf.commitIndex; N-- {
					// 这里的 index 是绝对索引，所以需要转换
					// Leader 只能提交自己当前 Term 的日志条目
					if rf.log[rf.toSliceIndex(N)].Term != rf.currentTerm {
						break
					}
					count := 1
					for peer := range rf.peers {
						if peer != rf.me && rf.matchIndex[peer] >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 {
						rf.commitIndex = N
						rf.applyCond.Broadcast()
						break
					}
				}
			}
		}(server)
	}
}

// 启动选举
// 当领导者第一次上台时，它将所有的 nextIndex 初始化为其日志中的最后一条的索引
func (rf *Raft) startElection() {
	// 对于 Candidates 1
	// 要选举前先更新自己的信息
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	// 改变了 currentTerm 和 votedFor，需要持久化
	rf.persist()
	electionTerm := rf.currentTerm
	// 用于记录票数，这里修改为地址更合适
	rf.votesGranted = 1
	// 构建 requestVote 信息
	lastLogIndex, lastLogTerm := rf.getLastIndexAndTerm()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 在协程期间，有可能状态发生改变，需要先检查
				if rf.state != Candidate || rf.currentTerm != electionTerm {
					return
				}

				// 检查返回消息的任期号
				if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.resetElectionTimer()
					// 改变了 currentTerm 和 votedFor，需要持久化
					rf.persist()
					return
				}

				// 看是否给自己投票了
				if reply.VoteGranted {
					rf.votesGranted++
					if rf.votesGranted > len(rf.peers)/2 {
						rf.state = Leader

						// 如果当选了 Leader，需要先初始化 nextIndex 和 matchIndex
						lastLogIndex, _ := rf.getLastIndexAndTerm()
						for peer := range rf.peers {
							rf.nextIndex[peer] = lastLogIndex + 1
							rf.matchIndex[peer] = 0
						}

						// 中选了需要立即广播消息
						rf.sendBroadcast()
					}
				}
			}
		}(server)
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

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	// 这里在初始化时就已经先放一个空的 logEntry，len = 1
	rf.log = make([]LogEntry, 1)
	// dummy 条目，index 0
	rf.log[0] = LogEntry{Term: 0}
	// 初始化快照相关字段，初始时没有快照，均为 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.resetElectionTimer()
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.killed() == false {
		// 当状态机（lastApplied）落后于 Raft 日志的快照点（lastIncludedIndex）时，
		// 必须先应用快照。
		if rf.lastApplied < rf.lastIncludedIndex {
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.persister.ReadSnapshot(),
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}

			// 发送消息时必须解锁，以防死锁
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()

			// 更新 lastApplied，使用 max 以防在解锁期间状态发生变化
			rf.lastApplied = max(rf.lastApplied, rf.lastIncludedIndex)
			continue // 重新开始循环，再次评估状态
		}

		if rf.commitIndex > rf.lastApplied {
			lastApplied := rf.lastApplied
			commitIndex := rf.commitIndex
			entriesToApply := make([]raftapi.ApplyMsg, commitIndex-lastApplied)
			// 这里的 lastApplied 是绝对索引，因此需要在访问 log 时转换
			for i := lastApplied + 1; i <= commitIndex; i++ {
				entriesToApply[i-(lastApplied+1)] = raftapi.ApplyMsg{
					CommandValid: true,
					CommandIndex: i,
					Command:      rf.log[rf.toSliceIndex(i)].Command,
				}
			}
			// 使用 max 防止乱序提交（例如，如果状态机apply慢，commitIndex可能回退）
			// 确保 lastApplied 不会意外地变小
			rf.lastApplied = max(rf.lastApplied, commitIndex)
			// 通道本身是原子性的，需要先解锁
			rf.mu.Unlock()
			for _, msg := range entriesToApply {
				rf.applyCh <- msg
			}
			rf.mu.Lock()
		} else { // 当 Leader 更新了 commitIndex 后唤醒 Wait 函数
			rf.applyCond.Wait()
		}
	}
}
