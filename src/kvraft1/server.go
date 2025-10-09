package kvraft

import (
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	mu    sync.Mutex
	store map[string]struct {
		value   string
		version rpc.Tversion
	}
	lastResult map[int64]clientResult
}

type clientResult struct {
	requestId uint64
	reply     any
}

func (kv *KVServer) duplicateReplyLocked(clientId int64, requestId uint64) (any, bool) {
	rec, ok := kv.lastResult[clientId]
	if !ok {
		return nil, false
	}
	if requestId <= rec.requestId {
		return rec.reply, true
	}
	return nil, false
}

func (kv *KVServer) recordReplyLocked(clientId int64, requestId uint64, reply any) {
	rec, ok := kv.lastResult[clientId]
	if !ok || requestId >= rec.requestId {
		kv.lastResult[clientId] = clientResult{requestId: requestId, reply: reply}
	}
}

func (kv *KVServer) DoOp(req any) any {
	switch v := req.(type) {
	case rpc.GetArgs:
		return kv.applyGet(v)
	case rpc.PutArgs:
		return kv.applyPut(v)
	default:
		return nil
	}
}

func (kv *KVServer) applyGet(args rpc.GetArgs) rpc.GetReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if reply, ok := kv.duplicateReplyLocked(args.ClientId, args.RequestId); ok {
		return reply.(rpc.GetReply)
	}

	entry, ok := kv.store[args.Key]
	if !ok {
		reply := rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrNoKey}
		kv.recordReplyLocked(args.ClientId, args.RequestId, reply)
		return reply
	}
	reply := rpc.GetReply{Value: entry.value, Version: entry.version, Err: rpc.OK}
	kv.recordReplyLocked(args.ClientId, args.RequestId, reply)
	return reply
}

func (kv *KVServer) applyPut(args rpc.PutArgs) rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if reply, ok := kv.duplicateReplyLocked(args.ClientId, args.RequestId); ok {
		return reply.(rpc.PutReply)
	}

	entry, ok := kv.store[args.Key]
	if ok {
		if entry.version != args.Version {
			reply := rpc.PutReply{Err: rpc.ErrVersion}
			kv.recordReplyLocked(args.ClientId, args.RequestId, reply)
			return reply
		}
		entry.value = args.Value
		entry.version++
		kv.store[args.Key] = entry
		reply := rpc.PutReply{Err: rpc.OK}
		kv.recordReplyLocked(args.ClientId, args.RequestId, reply)
		return reply
	}

	if args.Version != 0 {
		reply := rpc.PutReply{Err: rpc.ErrNoKey}
		kv.recordReplyLocked(args.ClientId, args.RequestId, reply)
		return reply
	}
	kv.store[args.Key] = struct {
		value   string
		version rpc.Tversion
	}{value: args.Value, version: 1}
	reply := rpc.PutReply{Err: rpc.OK}
	kv.recordReplyLocked(args.ClientId, args.RequestId, reply)
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	return nil
}

func (kv *KVServer) Restore(data []byte) {
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(rpc.PutReply)
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me: me,
		store: make(map[string]struct {
			value   string
			version rpc.Tversion
		}),
		lastResult: make(map[int64]clientResult),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []tester.IService{kv, kv.rsm.Raft()}
}
