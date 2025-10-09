package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"bytes"
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

const (
	// heartbeatInterval controls how often leaders send empty AppendEntries RPCs.
	heartbeatInterval = 120 * time.Millisecond
	// electionTimeoutMin/Max bound the randomized election timeout.
	electionTimeoutMin = 650 * time.Millisecond
	electionTimeoutMax = 950 * time.Millisecond
)

// func init() {
// 	rand.Seed(time.Now().UnixNano())
// }

type raftState int

const (
	stateFollower raftState = iota
	stateCandidate
	stateLeader
)

// LogEntry holds a single log entry; 3A uses only the Term field.
type LogEntry struct {
	Term    int
	Command interface{}
}

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

	currentTerm int
	votedFor    int

	state raftState

	log         []LogEntry
	commitIndex int
	lastApplied int

	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte

	nextIndex  []int
	matchIndex []int

	electionDeadline time.Time
	lastHeartbeat    time.Time

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == stateLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) encodeState() []byte {
	var buffer bytes.Buffer
	encoder := labgob.NewEncoder(&buffer)
	if err := encoder.Encode(rf.currentTerm); err != nil {
		return nil
	}
	if err := encoder.Encode(rf.votedFor); err != nil {
		return nil
	}
	if err := encoder.Encode(rf.lastIncludedIndex); err != nil {
		return nil
	}
	if err := encoder.Encode(rf.lastIncludedTerm); err != nil {
		return nil
	}
	if err := encoder.Encode(rf.log); err != nil {
		return nil
	}
	return buffer.Bytes()
}

func (rf *Raft) persist() {
	state := rf.encodeState()
	if state == nil {
		return
	}
	if rf.persister != nil {
		rf.persister.Save(state, rf.snapshot)
	}
}

func (rf *Raft) saveStateAndSnapshot(snapshot []byte) {
	if snapshot != nil {
		rf.snapshot = make([]byte, len(snapshot))
		copy(rf.snapshot, snapshot)
	} else {
		rf.snapshot = nil
	}
	rf.persist()
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
	var lastIncludedIndex int
	var lastIncludedTerm int
	var logEntries []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&logEntries) != nil {
		return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

	if len(logEntries) == 0 {
		rf.log = []LogEntry{{Term: rf.lastIncludedTerm}}
	} else {
		rf.log = logEntries
	}

	if len(rf.log) == 0 {
		rf.log = []LogEntry{{Term: rf.lastIncludedTerm}}
	}

	lastIdx := rf.lastLogIndex()
	if rf.commitIndex > lastIdx {
		rf.commitIndex = lastIdx
	}
	if rf.lastApplied > rf.commitIndex {
		rf.lastApplied = rf.commitIndex
	}
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
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
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.commitIndex || index > rf.lastLogIndex() {
		return
	}

	term := rf.termAt(index)
	sliceIdx := index - rf.lastIncludedIndex
	var newLog []LogEntry
	if sliceIdx+1 < len(rf.log) {
		newLog = append([]LogEntry{{Term: term}}, rf.log[sliceIdx+1:]...)
	} else {
		newLog = []LogEntry{{Term: term}}
	}
	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term

	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.saveStateAndSnapshot(snapshot)
	rf.applyCond.Broadcast()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
		reply.Term = rf.currentTerm
	}

	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.resetElectionLocked()
		DPrintf("[R%d] vote granted to %d for term=%d (candidate lastIdx=%d lastTerm=%d myLastIdx=%d myLastTerm=%d)", rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.lastLogIndex(), rf.lastLogTerm())
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

// AppendEntriesArgs captures the leader's AppendEntries RPC (heartbeat for 3A).
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply captures the follower's response to AppendEntries.
type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term int
}

// AppendEntries handles heartbeats and (later) log replication from leaders.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = rf.lastLogIndex() + 1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
	}

	rf.state = stateFollower
	persistNeeded := false
	if rf.votedFor != args.LeaderId {
		rf.votedFor = args.LeaderId
		persistNeeded = true
	}
	rf.resetElectionLocked()

	if args.PrevLogIndex < rf.lastIncludedIndex {
		if persistNeeded {
			rf.persist()
		}
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.lastIncludedIndex + 1
		return
	}

	if args.PrevLogIndex > rf.lastLogIndex() {
		if persistNeeded {
			rf.persist()
		}
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.lastLogIndex() + 1
		return
	}

	if args.PrevLogIndex >= 0 && rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		if persistNeeded {
			rf.persist()
		}
		conflictTerm := rf.termAt(args.PrevLogIndex)
		reply.XTerm = conflictTerm
		reply.XIndex = rf.firstIndexOfTerm(conflictTerm)
		reply.XLen = rf.lastLogIndex() + 1
		return
	}

	newIndex := args.PrevLogIndex + 1
	logUpdated := false
	for i, entry := range args.Entries {
		if newIndex <= rf.lastLogIndex() {
			if rf.termAt(newIndex) != entry.Term {
				rf.truncateLog(newIndex - 1)
				rf.log = append(rf.log, args.Entries[i:]...)
				logUpdated = true
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			logUpdated = true
			break
		}
		newIndex++
	}
	if logUpdated {
		persistNeeded = true
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.lastLogIndex())
		rf.applyCond.Broadcast()
	}

	if persistNeeded {
		rf.persist()
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = rf.lastLogIndex() + 1

	DPrintf("[R%d] AppendEntries from L%d term=%d prevIdx=%d entries=%d success", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, len(args.Entries))
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}

	if args.Term > rf.currentTerm {
		rf.becomeFollowerLocked(args.Term)
		reply.Term = rf.currentTerm
	}

	rf.state = stateFollower
	rf.resetElectionLocked()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	snapshotCopy := append([]byte(nil), args.Snapshot...)

	if args.LastIncludedIndex < rf.lastLogIndex() {
		sliceIdx := rf.sliceIndex(args.LastIncludedIndex)
		if sliceIdx < 0 {
			sliceIdx = 0
		}
		if sliceIdx+1 < len(rf.log) {
			rf.log = append([]LogEntry{{Term: args.LastIncludedTerm}}, rf.log[sliceIdx+1:]...)
		} else {
			rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
		}
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if rf.commitIndex < rf.lastIncludedIndex {
		rf.commitIndex = rf.lastIncludedIndex
	}
	if rf.lastApplied < rf.lastIncludedIndex {
		rf.lastApplied = rf.lastIncludedIndex
	}

	rf.saveStateAndSnapshot(snapshotCopy)
	rf.applyCond.Broadcast()
	rf.mu.Unlock()

	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshotCopy,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.applyCh <- msg
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	if rf.state != stateLeader {
		return -1, term, false
	}

	entry := LogEntry{
		Term:    term,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	index := rf.lastLogIndex()

	rf.persist()

	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	rf.updateCommitIndexLocked()

	DPrintf("[R%d] Start command term=%d index=%d", rf.me, term, index)

	go rf.broadcastHeartbeats()

	return index, term, true
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
	rf.applyCond.Broadcast()
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
		state := rf.state
		timeout := time.Now().After(rf.electionDeadline)
		sendHeartbeat := state == stateLeader && time.Since(rf.lastHeartbeat) >= heartbeatInterval
		if sendHeartbeat {
			rf.lastHeartbeat = time.Now()
		}
		shouldStartElection := timeout && state != stateLeader
		rf.mu.Unlock()

		if shouldStartElection {
			rf.startElection()
		}
		if sendHeartbeat {
			rf.broadcastHeartbeats()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.state = stateFollower
	rf.log = []LogEntry{{Term: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1
	}
	rf.lastHeartbeat = time.Now()
	rf.resetElectionLocked()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	lastIndex := rf.lastLogIndex() + 1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lastIndex
		rf.matchIndex[i] = rf.lastIncludedIndex
	}
	rf.matchIndex[rf.me] = rf.lastLogIndex()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()

	return rf
}

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) sliceIndex(index int) int {
	return index - rf.lastIncludedIndex
}

func (rf *Raft) termAt(index int) int {
	sliceIdx := rf.sliceIndex(index)
	if sliceIdx < 0 || sliceIdx >= len(rf.log) {
		return -1
	}
	return rf.log[sliceIdx].Term
}

func (rf *Raft) firstIndexOfTerm(term int) int {
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Term == term {
			return rf.lastIncludedIndex + i
		}
	}
	return -1
}

func (rf *Raft) lastIndexOfTerm(term int) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term == term {
			return rf.lastIncludedIndex + i
		}
	}
	return -1
}

func (rf *Raft) truncateLog(index int) {
	if index < rf.lastIncludedIndex {
		rf.log = []LogEntry{{Term: rf.lastIncludedTerm}}
		return
	}
	sliceIdx := rf.sliceIndex(index)
	if sliceIdx+1 < len(rf.log) {
		rf.log = rf.log[:sliceIdx+1]
	}
}

func (rf *Raft) copyLogFrom(index int) []LogEntry {
	sliceIdx := rf.sliceIndex(index)
	if sliceIdx < 0 {
		sliceIdx = 0
	}
	if sliceIdx == 0 && len(rf.log) > 0 {
		sliceIdx = 1
	}
	if sliceIdx >= len(rf.log) {
		return nil
	}
	entries := make([]LogEntry, len(rf.log)-sliceIdx)
	copy(entries, rf.log[sliceIdx:])
	return entries
}

func (rf *Raft) logEntry(index int) LogEntry {
	sliceIdx := rf.sliceIndex(index)
	if sliceIdx < 0 || sliceIdx >= len(rf.log) {
		return LogEntry{}
	}
	return rf.log[sliceIdx]
}

func (rf *Raft) resetElectionLocked() {
	delay := electionTimeoutMin
	delay += time.Duration(rand.Int63n(int64(electionTimeoutMax - electionTimeoutMin)))
	rf.electionDeadline = time.Now().Add(delay)
}

func (rf *Raft) isLogUpToDate(lastIdx int, lastTerm int) bool {
	myTerm := rf.lastLogTerm()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIdx >= rf.lastLogIndex()
}

func (rf *Raft) becomeFollowerLocked(term int) {
	rf.state = stateFollower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.resetElectionLocked()
	rf.persist()
}

func (rf *Raft) becomeLeaderLocked() {
	rf.state = stateLeader
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	rf.resetElectionLocked()
	lastIndex := rf.lastLogIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex + 1
		rf.matchIndex[i] = rf.lastIncludedIndex
	}
	rf.matchIndex[rf.me] = lastIndex
	rf.persist()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == stateLeader {
		rf.mu.Unlock()
		return
	}

	rf.state = stateCandidate
	rf.currentTerm++
	termStarted := rf.currentTerm
	rf.votedFor = rf.me
	rf.persist()
	lastLogIndex := rf.lastLogIndex()
	lastLogTerm := rf.lastLogTerm()
	rf.resetElectionLocked()
	rf.mu.Unlock()

	votes := 1
	majority := len(rf.peers)/2 + 1

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         termStarted,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if termStarted != rf.currentTerm || rf.state != stateCandidate {
				if reply.Term > rf.currentTerm {
					rf.becomeFollowerLocked(reply.Term)
				}
				return
			}

			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				return
			}

			if reply.VoteGranted {
				votes++
				if votes >= majority {
					rf.becomeLeaderLocked()
					go rf.broadcastHeartbeats()
				}
			}
		}(peer)
	}
}

func (rf *Raft) broadcastHeartbeats() {
	rf.mu.Lock()
	if rf.state != stateLeader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == me {
			continue
		}
		go rf.replicateToPeer(peer, term)
	}
}

func (rf *Raft) replicateToPeer(server int, term int) {
	for {
		rf.mu.Lock()
		if rf.state != stateLeader || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		if rf.nextIndex[server] <= rf.lastIncludedIndex {
			if rf.snapshot == nil {
				rf.nextIndex[server] = rf.lastIncludedIndex + 1
				rf.mu.Unlock()
				continue
			}
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Snapshot:          append([]byte(nil), rf.snapshot...),
			}
			rf.mu.Unlock()

			reply := &InstallSnapshotReply{}
			if !rf.sendInstallSnapshot(server, args, reply) {
				return
			}

			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.becomeFollowerLocked(reply.Term)
				rf.mu.Unlock()
				return
			}
			if rf.state != stateLeader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			rf.matchIndex[server] = rf.lastIncludedIndex
			rf.nextIndex[server] = rf.lastIncludedIndex + 1
			rf.mu.Unlock()
			continue
		}

		nextIdx := rf.nextIndex[server]
		prevIdx := nextIdx - 1
		prevTerm := rf.termAt(prevIdx)
		entries := rf.copyLogFrom(nextIdx)
		leaderCommit := rf.commitIndex
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIdx,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		}
		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		reply := &AppendEntriesReply{}
		if !rf.sendAppendEntries(server, args, reply) {
			return
		}

		rf.mu.Lock()

		if reply.Term > rf.currentTerm {
			rf.becomeFollowerLocked(reply.Term)
			rf.mu.Unlock()
			return
		}

		if rf.state != stateLeader || currentTerm != rf.currentTerm || rf.currentTerm != term {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			if rf.matchIndex[server] < rf.lastIncludedIndex {
				rf.matchIndex[server] = rf.lastIncludedIndex
			}
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			rf.updateCommitIndexLocked()
			DPrintf("[R%d] replicate to %d success upto %d", rf.me, server, rf.matchIndex[server])
			rf.mu.Unlock()
			return
		}

		if reply.XTerm == -1 {
			rf.nextIndex[server] = reply.XLen
		} else {
			lastIdx := rf.lastIndexOfTerm(reply.XTerm)
			if lastIdx >= 0 {
				rf.nextIndex[server] = lastIdx + 1
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
		}
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
		DPrintf("[R%d] replicate to %d failed, adjust nextIndex to %d (XTerm=%d XIndex=%d XLen=%d)", rf.me, server, rf.nextIndex[server], reply.XTerm, reply.XIndex, reply.XLen)
		rf.mu.Unlock()
	}
}

func (rf *Raft) applier() {
	defer close(rf.applyCh)
	for {
		rf.mu.Lock()
		for !rf.killed() && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		rf.lastApplied++
		index := rf.lastApplied
		entry := rf.logEntry(index)
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: index,
		}
		rf.mu.Unlock()

		if rf.killed() {
			return
		}

		rf.applyCh <- msg
	}
}

func (rf *Raft) updateCommitIndexLocked() {
	lastIdx := rf.lastLogIndex()
	for idx := lastIdx; idx > rf.commitIndex; idx-- {
		if rf.termAt(idx) != rf.currentTerm {
			continue
		}
		count := 1
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= idx {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = idx
			rf.applyCond.Broadcast()
			snapshot := make([]int, len(rf.matchIndex))
			copy(snapshot, rf.matchIndex)
			DPrintf("[R%d] commit index advanced to %d in term %d (match=%v)", rf.me, rf.commitIndex, rf.currentTerm, snapshot)
			break
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
