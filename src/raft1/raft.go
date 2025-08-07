package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	// 在响应 RPC 之前，在稳定的存储上更新
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

// 获取服务器最新的日志的 index 和 term
func (rf *Raft) getLastIndexAndTerm() (int, int) {
	// index 是日志的长度 -1，因为最开始就有一个空的日志
	index := len(rf.log) - 1
	// term 是最新的日志的任期，因为前面保持了一致，这里直接索引即可（如第一条日志是log[1]）
	term := rf.log[index].Term
	return index, term
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	}

	// 对于 Candidates 3
	if rf.state == Candidate {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// 只要收到 AppendEntries 消息，都意味着需要重启计时器
	rf.resetElectionTimer()

	// 上面是身份转变，开始对日志进行判断
	// AppendEntries 2：如果日志在 prevLogIndex 处不包含 term 与 prevLogTerm 匹配的条目，则返回 false
	lastLogIndex, _ := rf.getLastIndexAndTerm()
	// 首先 prevLogIndex > lastLogIndex，则少数据，直接返回 false
	if lastLogIndex < args.PrevLogIndex {
		return
	}
	// 剩下表示索引可以定位，需要比较 term 是否匹配
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return
	}

	// AppendEntries 3：如果一个现有的条目与一个新的条目相冲突（相同的索引但不同的任期），删除现有的条目和后面所有的条目
	// 经过上面，剩下的只剩数据相同或接收人数据更多，无论怎样都需要进行数据截断
	rf.log = rf.log[:args.PrevLogIndex+1]

	// AppendEntries 4：添加日志中任何尚未出现的新条目
	rf.log = append(rf.log, args.Entries...)

	// 只要能发送 AppendEntries 的 term >= me，并且能走到最后一步，将回复设置为 true
	reply.Success = true

	// AppendEntries 5：如果 leaderCommit > commitIndex，设置 commitIndex = min(leaderCommit, 最后一个新条目的索引)
	if args.LeaderCommit > rf.commitIndex {
		// 最后一个新条目的索引是 prevLogIndex 加上新条目的数量
		lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
		rf.commitIndex = min(args.LeaderCommit, lastNewEntryIndex)
		// 当 follower 的 commitIndex 更新后，需要唤醒 applier goroutine 来应用日志
		rf.applyCond.Broadcast()
	}

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
	// 给每一个服务器发送
	for server := range rf.peers {
		// 遇到自己跳过
		if server == rf.me {
			continue
		}
		// 进行非阻塞的 RPC 访问
		go func(server int) {
			rf.mu.Lock()
			prevLogIndex := rf.nextIndex[server] - 1
			prevLogTerm := rf.log[prevLogIndex].Term

			// Leaders rule 3
			var entries []LogEntry
			lastLogIndex, _ := rf.getLastIndexAndTerm()
			if lastLogIndex >= rf.nextIndex[server] {
				entries = make([]LogEntry, len(rf.log[rf.nextIndex[server]:]))
				copy(entries, rf.log[rf.nextIndex[server]:])
			}

			// 构建心跳信息
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
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
					} else { // 数据没有匹配上，需要递减 nextIndex 并重试，Leaders rule 3.2
						if rf.nextIndex[server] > 1 {
							rf.nextIndex[server]--
						}
					}
				}
				// Leaders rule 4
				lastLogIndex, _ = rf.getLastIndexAndTerm()
				// 从最大值开始往下找，找到最大的已经 commit 的 N
				for N := lastLogIndex; N > rf.commitIndex; N-- {
					// Leader 只能提交自己当前 Term 的日志条目
					if rf.log[N].Term != rf.currentTerm {
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
	// 用于记录票数，这里修改为地址更合适
	votesCount := 1
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

				// 检查返回消息的任期号
				if reply.Term > rf.currentTerm {
					rf.state = Follower
					rf.votedFor = -1
					rf.currentTerm = reply.Term
					rf.resetElectionTimer()
					return
				}

				// 看是否给自己投票了
				if reply.VoteGranted {
					votesCount++
					if votesCount > len(rf.peers)/2 {
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
		if rf.commitIndex > rf.lastApplied {
			lastApplied := rf.lastApplied
			commitIndex := rf.commitIndex
			entriesToApply := make([]raftapi.ApplyMsg, commitIndex-lastApplied)
			for i := lastApplied + 1; i <= commitIndex; i++ {
				entriesToApply[i-(lastApplied+1)] = raftapi.ApplyMsg{
					CommandValid: true,
					CommandIndex: i,
					Command:      rf.log[i].Command,
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
