package rsm

import (
	"log"
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

// 和 rsm 有关的流程

// 在执行 operation 工作流时
// server 调用 rsm Submit(req) 函数，rsm 等待指令被 server 执行
// rsm 调用 raft Start(cmd) 函数，raft 在 log 中添加新的 command，开始共识过程
// raft 通过 applyCh 发送 command message(SnapshotValid = false, CommitValid = true) 给 rsm
// rsm 得知 raft 形成共识后调用 server DoOp(opreq) 函数
// server 返回 DoOp return opres 给 rsm
// rsm 得到 server 的处理结果后，返回 Submit return res 给 server

var useRaftStateMachine bool // to plug in another raft besided raft1

// 实验指导提到 Submit() 应该将每个客户端操作与唯一标识符一起包装在一个 Op 结构中
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Req any // 客户端的原始请求
	Id  int // 唯一的标识符
	Me  int // 最初提交此操作的服务器
}

// reader 和 Submit() 沟通的通道
type OpResult struct {
	Result any // DoOp() 执行的结果
	Op     Op  // 被提交的操作
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
	waitChans map[int]chan OpResult // 暂时记录所有 submit 的事件

	nextOpId int // 为每一个操作生成一个唯一标识符
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
		waitChans:    make(map[int]chan OpResult),
		nextOpId:     0,
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
	// 获取当前的 term，作为基准
	term, isLeader := rsm.rf.GetState()
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	rsm.mu.Lock()
	rsm.nextOpId++
	id := rsm.nextOpId
	rsm.mu.Unlock()

	// 创建带有唯一 id 的 op
	op := Op{
		Req: req,
		Id:  id,
		Me:  rsm.me,
	}

	index, startTerm, isLeader := rsm.rf.Start(op)

	if !isLeader || term != startTerm {
		return rpc.ErrWrongLeader, nil
	}

	DPrintf("RSM %d: Submit op (%d, %d), got index %d, isLeader %v", rsm.me, op.Me, op.Id, index, isLeader)

	ch := make(chan OpResult, 1)

	rsm.mu.Lock()
	rsm.waitChans[index] = ch
	rsm.mu.Unlock()

	DPrintf("RSM %d: Waiting for op (%d, %d) at index %d", rsm.me, op.Me, op.Id, index)

	// 使用 defer 来确保 channel 最终会被清理，这比在每个返回路径都写一遍更健壮
	defer func() {
		rsm.mu.Lock()
		// 检查 map 中的 channel 是否还是我们创建的那个，防止误删
		if rsm.waitChans[index] == ch {
			delete(rsm.waitChans, index)
		}
		rsm.mu.Unlock()
	}()

	select {
	case opResult := <-ch:
		DPrintf("RSM %d: Woke up for op (%d, %d) at index %d. Got op (%d, %d)", rsm.me, op.Me, op.Id, index, opResult.Op.Me, opResult.Op.Id)
		if opResult.Op.Id != op.Id || opResult.Op.Me != rsm.me {
			// ID 不匹配，这意味着我们的操作被覆盖了，我们已经不是 Leader。
			DPrintf("RSM %d: Op Mismatch! MyOp:(%d,%d), GotOp:(%d,%d). Returning ErrWrongLeader.", rsm.me, op.Me, op.Id, opResult.Op.Me, opResult.Op.Id)
			return rpc.ErrWrongLeader, nil
		}
		DPrintf("RSM %d: Op Match for (%d, %d). Returning OK.", rsm.me, op.Me, op.Id)
		// ID 匹配，操作成功。
		return rpc.OK, opResult.Result

	case <-time.After(2000 * time.Millisecond):
		DPrintf("RSM %d: Timed out for op (%d, %d) at index %d", rsm.me, op.Me, op.Id, index)
		return rpc.ErrWrongLeader, nil
	}
}

// 用于接收 raft 形成共识的操作
// 并将操作发送给 server 进行处理
func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		// 在执行操作工作流中，看 commandvalid
		if msg.CommandValid {
			// 确保 command 是 Op 类型
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}

			DPrintf("RSM %d: Reader got ApplyMsg for index %d with op (%d, %d)", rsm.me, msg.CommandIndex, op.Me, op.Id)

			result := rsm.sm.DoOp(op.Req)

			_, isLeader := rsm.rf.GetState()

			rsm.mu.Lock()
			ch, exists := rsm.waitChans[msg.CommandIndex]

			if exists && isLeader {
				DPrintf("RSM %d: Reader sending op (%d, %d) to channel for index %d", rsm.me, op.Me, op.Id, msg.CommandIndex)
				select {
				case ch <- OpResult{Result: result, Op: op}:
				default:
					DPrintf("RSM %d: Reader failed to send on channel for index %d (receiver gone)", rsm.me, msg.CommandIndex)
				}
			}

			rsm.mu.Unlock()
		}
	}
}
