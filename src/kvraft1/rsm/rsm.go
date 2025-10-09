package rsm

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Me identifies which server submitted the Op. Tests and Submit use this to
	// match locally originated entries once they commit.
	Me int
	// Id is a per-server unique identifier that lets us disambiguate Ops
	// originating from the same server.
	Id uint64
	// Req carries the state machine specific request (e.g., Inc, Null).
	Req any
}

type submitResult struct {
	op    Op
	reply any
	err   rpc.Err
}

type pending struct {
	op   Op
	term int
	ch   chan submitResult
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
	waiters      map[int]*pending
	ready        map[int]submitResult
	lastApplied  int
	nextId       uint64
	doneCh       chan struct{}
	stopOnce     sync.Once
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
		waiters:      make(map[int]*pending),
		ready:        make(map[int]submitResult),
		doneCh:       make(chan struct{}),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.applyLoop()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	op := Op{
		Me:  rsm.me,
		Id:  atomic.AddUint64(&rsm.nextId, 1),
		Req: req,
	}

	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan submitResult, 1)

	rsm.mu.Lock()
	// Replace any stale waiter occupying this index (should not happen, but be defensive).
	if old, ok := rsm.waiters[index]; ok {
		delete(rsm.waiters, index)
		// Wake stale waiter so it can exit.
		old.ch <- submitResult{err: rpc.ErrWrongLeader}
	}
	rsm.waiters[index] = &pending{
		op:   op,
		term: term,
		ch:   ch,
	}

	if res, ok := rsm.ready[index]; ok {
		delete(rsm.waiters, index)
		delete(rsm.ready, index)
		rsm.mu.Unlock()
		if res.op.Id == op.Id {
			return res.err, res.reply
		}
		return rpc.ErrWrongLeader, nil
	}
	doneCh := rsm.doneCh
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	rsm.mu.Unlock()

	for {
		select {
		case res := <-ch:
			if res.err != rpc.OK {
				return res.err, nil
			}
			return res.err, res.reply
		case <-doneCh:
			rsm.mu.Lock()
			if cur, ok := rsm.waiters[index]; ok && cur.op.Id == op.Id {
				delete(rsm.waiters, index)
			}
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		case <-ticker.C:
			if curTerm, isLeader := rsm.rf.GetState(); !isLeader || curTerm > term {
				rsm.mu.Lock()
				if cur, ok := rsm.waiters[index]; ok && cur.op.Id == op.Id {
					delete(rsm.waiters, index)
				}
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}

func (rsm *RSM) applyLoop() {
	defer rsm.shutdownWaiters()
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}

			if !rsm.shouldApply(msg.CommandIndex) {
				continue
			}

			reply := rsm.sm.DoOp(op.Req)
			rsm.finishApply(msg.CommandIndex, op, reply, rpc.OK)
		} else if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
			rsm.mu.Lock()
			if msg.SnapshotIndex > rsm.lastApplied {
				rsm.lastApplied = msg.SnapshotIndex
			}
			// Drop waiters for commands older than the snapshot.
			for idx, waiter := range rsm.waiters {
				if idx <= msg.SnapshotIndex {
					delete(rsm.waiters, idx)
					waiter.ch <- submitResult{err: rpc.ErrWrongLeader}
				}
			}
			for idx := range rsm.ready {
				if idx <= msg.SnapshotIndex {
					delete(rsm.ready, idx)
				}
			}
			rsm.mu.Unlock()
		}
	}
}

func (rsm *RSM) shouldApply(index int) bool {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	if index <= rsm.lastApplied {
		return false
	}
	rsm.lastApplied = index
	return true
}

func (rsm *RSM) finishApply(index int, op Op, reply any, err rpc.Err) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if waiter, ok := rsm.waiters[index]; ok {
		delete(rsm.waiters, index)
		if waiter.op.Id == op.Id {
			waiter.ch <- submitResult{op: op, reply: reply, err: err}
		} else {
			waiter.ch <- submitResult{op: op, err: rpc.ErrWrongLeader}
		}
		return
	}

	// Store result for the originating leader in case apply precedes waiter registration.
	if op.Me == rsm.me {
		rsm.ready[index] = submitResult{op: op, reply: reply, err: err}
	}
}

func (rsm *RSM) shutdownWaiters() {
	rsm.stopOnce.Do(func() { close(rsm.doneCh) })

	rsm.mu.Lock()
	waiters := make([]*pending, 0, len(rsm.waiters))
	for idx, waiter := range rsm.waiters {
		waiters = append(waiters, waiter)
		delete(rsm.waiters, idx)
	}
	rsm.ready = make(map[int]submitResult)
	rsm.mu.Unlock()

	for _, waiter := range waiters {
		waiter.ch <- submitResult{err: rpc.ErrWrongLeader}
	}
}
