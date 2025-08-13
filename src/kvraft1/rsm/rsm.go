package rsm

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me     int   // 当前服务器的 id
	Id     int64 // 当前 op 的 id
	Req    any   // 实际的指令
	Result any   // DoOp() 执行的结果
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
	waitChs map[int]chan Op // submit 用于接收 reader 的信息
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
		waitChs:      make(map[int]chan Op),
	}
	// 从持久化存储中读取并恢复快照
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rsm.sm.Restore(snapshot)
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
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
	// your code here
	id := nrand()
	op := Op{
		Me:  rsm.me,
		Id:  id,
		Req: req,
	}
	index, _, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}
	ch := make(chan Op, 1)

	rsm.mu.Lock()
	rsm.waitChs[index] = ch
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.waitChs, index)
		DPrintf("RSM %d: Cleaned up channel for index %d", rsm.me, index)
		rsm.mu.Unlock()
	}()

	select {
	case result := <-ch:
		DPrintf("RSM %d: Woke up for index %d. Original Op: (Id: %d, Me: %d). Received Op: (Id: %d, Me: %d)", rsm.me, index, id, rsm.me, result.Id, result.Me)
		if result.Id != id || result.Me != rsm.me {
			DPrintf("RSM %d: Op Mismatch at index %d! Returning ErrWrongLeader.", rsm.me, index)
			return rpc.ErrWrongLeader, nil
		}
		DPrintf("RSM %d: Op Match at index %d. Returning OK.", rsm.me, index)
		return rpc.OK, result.Result
	case <-time.After(2000 * time.Millisecond):
		DPrintf("RSM %d: Timed out waiting for Op (%d) at index %d.", rsm.me, id, index)
		return rpc.ErrMaybe, nil
	}
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid && !msg.SnapshotValid {
			index := msg.CommandIndex
			op, ok := msg.Command.(Op)
			if !ok {
				DPrintf("RSM %d: Reader ERROR! Cannot cast msg.Command to Op type at index %d. Command is: %+v", rsm.me, index, msg.Command)
				continue
			}

			DPrintf("RSM %d: Reader received ApplyMsg for index %d with Op (Id: %d, Me: %d)", rsm.me, index, op.Id, op.Me)

			result := rsm.sm.DoOp(op.Req)
			op.Result = result
			rsm.mu.Lock()
			ch, exists := rsm.waitChs[index]
			if exists {
				DPrintf("RSM %d: Reader found channel for index %d. Sending result for Op (Id: %d)...", rsm.me, index, op.Id)
				// 使用非阻塞发送，以防Submit goroutine已经超时并放弃了channel
				select {
				case ch <- op:
				default:
					DPrintf("RSM %d: Reader failed to send on channel for index %d (receiver gone)", rsm.me, index)
				}
			} else {
				DPrintf("RSM %d: Reader found NO channel for index %d. Op (Id: %d) might have timed out.", rsm.me, index, op.Id)
			}
			rsm.mu.Unlock()

			// 检查是否需要生成快照
			// 1. maxraftstate != -1
			// 2. 当前存储的数据长度已经超过 maxraftstate
			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() >= rsm.maxraftstate {
				DPrintf("RSM %d: Raft state size %d >= maxraftstate %d. Creating snapshot.", rsm.me, rsm.rf.PersistBytes(), rsm.maxraftstate)
				snapshotData := rsm.sm.Snapshot()
				rsm.rf.Snapshot(index, snapshotData)
			}
		} else if !msg.CommandValid && msg.SnapshotValid { // Leader 发送来 installSnapshot
			// 直接读取发送来的快照即可
			rsm.sm.Restore(msg.Snapshot)
			DPrintf("RSM %d: State machine restored from snapshot.", rsm.me)
		}
	}
}
