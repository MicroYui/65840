package kvraft

import (
	"bytes"
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
	isGet     bool
	getReply  rpc.GetReply
	putReply  rpc.PutReply
}

type snapshotEntry struct {
	Value   string
	Version rpc.Tversion
}

type snapshotClientResult struct {
	RequestId uint64
	IsGet     bool
	GetReply  rpc.GetReply
	PutReply  rpc.PutReply
}

type snapshotState struct {
	Store      map[string]snapshotEntry
	LastResult map[int64]snapshotClientResult
}

func (kv *KVServer) cachedGetLocked(clientId int64, requestId uint64) (rpc.GetReply, bool) {
	rec, ok := kv.lastResult[clientId]
	if !ok || rec.requestId != requestId || !rec.isGet {
		return rpc.GetReply{}, false
	}
	return rec.getReply, true
}

func (kv *KVServer) cachedPutLocked(clientId int64, requestId uint64) (rpc.PutReply, bool) {
	rec, ok := kv.lastResult[clientId]
	if !ok || rec.requestId != requestId || rec.isGet {
		return rpc.PutReply{}, false
	}
	return rec.putReply, true
}

func (kv *KVServer) storeGetLocked(clientId int64, requestId uint64, reply rpc.GetReply) {
	rec, ok := kv.lastResult[clientId]
	if !ok || requestId >= rec.requestId {
		kv.lastResult[clientId] = clientResult{
			requestId: requestId,
			isGet:     true,
			getReply:  reply,
		}
	}
}

func (kv *KVServer) storePutLocked(clientId int64, requestId uint64, reply rpc.PutReply) {
	rec, ok := kv.lastResult[clientId]
	if !ok || requestId >= rec.requestId {
		kv.lastResult[clientId] = clientResult{
			requestId: requestId,
			isGet:     false,
			putReply:  reply,
		}
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

	if reply, ok := kv.cachedGetLocked(args.ClientId, args.RequestId); ok {
		return reply
	}

	entry, ok := kv.store[args.Key]
	if !ok {
		reply := rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrNoKey}
		kv.storeGetLocked(args.ClientId, args.RequestId, reply)
		return reply
	}
	reply := rpc.GetReply{Value: entry.value, Version: entry.version, Err: rpc.OK}
	kv.storeGetLocked(args.ClientId, args.RequestId, reply)
	return reply
}

func (kv *KVServer) applyPut(args rpc.PutArgs) rpc.PutReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if reply, ok := kv.cachedPutLocked(args.ClientId, args.RequestId); ok {
		return reply
	}

	entry, ok := kv.store[args.Key]
	if ok {
		if entry.version != args.Version {
			reply := rpc.PutReply{Err: rpc.ErrVersion}
			kv.storePutLocked(args.ClientId, args.RequestId, reply)
			return reply
		}
		entry.value = args.Value
		entry.version++
		kv.store[args.Key] = entry
		reply := rpc.PutReply{Err: rpc.OK}
		kv.storePutLocked(args.ClientId, args.RequestId, reply)
		return reply
	}

	if args.Version != 0 {
		reply := rpc.PutReply{Err: rpc.ErrNoKey}
		kv.storePutLocked(args.ClientId, args.RequestId, reply)
		return reply
	}
	kv.store[args.Key] = struct {
		value   string
		version rpc.Tversion
	}{value: args.Value, version: 1}
	reply := rpc.PutReply{Err: rpc.OK}
	kv.storePutLocked(args.ClientId, args.RequestId, reply)
	return reply
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	state := snapshotState{
		Store:      make(map[string]snapshotEntry, len(kv.store)),
		LastResult: make(map[int64]snapshotClientResult, len(kv.lastResult)),
	}
	for k, v := range kv.store {
		state.Store[k] = snapshotEntry{Value: v.value, Version: v.version}
	}
	for id, res := range kv.lastResult {
		state.LastResult[id] = snapshotClientResult{
			RequestId: res.requestId,
			IsGet:     res.isGet,
			GetReply:  res.getReply,
			PutReply:  res.putReply,
		}
	}

	var buf bytes.Buffer
	enc := labgob.NewEncoder(&buf)
	if err := enc.Encode(state); err != nil {
		return nil
	}
	return buf.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}

	var state snapshotState
	buf := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(buf)
	if err := dec.Decode(&state); err != nil {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.store = make(map[string]struct {
		value   string
		version rpc.Tversion
	}, len(state.Store))
	for k, v := range state.Store {
		kv.store[k] = struct {
			value   string
			version rpc.Tversion
		}{value: v.Value, version: v.Version}
	}

	kv.lastResult = make(map[int64]clientResult, len(state.LastResult))
	for id, res := range state.LastResult {
		kv.lastResult[id] = clientResult{
			requestId: res.RequestId,
			isGet:     res.IsGet,
			getReply:  res.GetReply,
			putReply:  res.PutReply,
		}
	}
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
	labgob.Register(snapshotState{})
	labgob.Register(snapshotEntry{})
	labgob.Register(snapshotClientResult{})

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
