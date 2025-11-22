package shardgrp

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const debugShardgrpServer = false

func srvLog(format string, args ...any) {
	if !debugShardgrpServer {
		return
	}
	log.Printf("[shardgrp-server] "+format, args...)
}

type clientResult struct {
	requestId uint64
	isGet     bool
	getReply  rpc.GetReply
	putReply  rpc.PutReply
}

type kvEntry struct {
	value   string
	version rpc.Tversion
}

type shardState struct {
	owned      bool
	frozen     bool
	num        shardcfg.Tnum
	store      map[string]kvEntry
	lastResult map[int64]clientResult
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

type snapshotShardState struct {
	Owned      bool
	Frozen     bool
	Num        shardcfg.Tnum
	Store      map[string]snapshotEntry
	LastResult map[int64]snapshotClientResult
}

type snapshotState struct {
	Shards map[shardcfg.Tshid]snapshotShardState
}

type transferState struct {
	Store      map[string]snapshotEntry
	LastResult map[int64]snapshotClientResult
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	mu     sync.Mutex
	shards map[shardcfg.Tshid]*shardState
}

func newShardState(owned bool) *shardState {
	return &shardState{
		owned:      owned,
		store:      make(map[string]kvEntry),
		lastResult: make(map[int64]clientResult),
	}
}

func (kv *KVServer) ensureShardLocked(sh shardcfg.Tshid) *shardState {
	if kv.shards == nil {
		kv.shards = make(map[shardcfg.Tshid]*shardState)
	}
	st, ok := kv.shards[sh]
	if !ok {
		st = newShardState(false)
		kv.shards[sh] = st
	}
	return st
}

func (kv *KVServer) cachedPutLocked(st *shardState, clientId int64, requestId uint64) (rpc.PutReply, bool) {
	rec, ok := st.lastResult[clientId]
	if !ok || rec.requestId != requestId || rec.isGet {
		return rpc.PutReply{}, false
	}
	return rec.putReply, true
}

func (kv *KVServer) cachedGetLocked(st *shardState, clientId int64, requestId uint64) (rpc.GetReply, bool) {
	rec, ok := st.lastResult[clientId]
	if !ok || rec.requestId != requestId || !rec.isGet {
		return rpc.GetReply{}, false
	}
	return rec.getReply, true
}

func (kv *KVServer) storeGetLocked(st *shardState, clientId int64, requestId uint64, reply rpc.GetReply) {
	rec, ok := st.lastResult[clientId]
	if !ok || requestId >= rec.requestId {
		st.lastResult[clientId] = clientResult{
			requestId: requestId,
			isGet:     true,
			getReply:  reply,
		}
	}
}

func (kv *KVServer) storePutLocked(st *shardState, clientId int64, requestId uint64, reply rpc.PutReply) {
	rec, ok := st.lastResult[clientId]
	if !ok || requestId >= rec.requestId {
		st.lastResult[clientId] = clientResult{
			requestId: requestId,
			isGet:     false,
			putReply:  reply,
		}
	}
}

func (kv *KVServer) applyGet(args rpc.GetArgs) rpc.GetReply {
	sh := shardcfg.Key2Shard(args.Key)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	st := kv.ensureShardLocked(sh)
	if !st.owned {
		return rpc.GetReply{Err: rpc.ErrWrongGroup}
	}

	if reply, ok := kv.cachedGetLocked(st, args.ClientId, args.RequestId); ok {
		return reply
	}

	entry, ok := st.store[args.Key]
	if !ok {
		reply := rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrNoKey}
		kv.storeGetLocked(st, args.ClientId, args.RequestId, reply)
		return reply
	}
	reply := rpc.GetReply{Value: entry.value, Version: entry.version, Err: rpc.OK}
	kv.storeGetLocked(st, args.ClientId, args.RequestId, reply)
	return reply
}

func (kv *KVServer) applyPut(args rpc.PutArgs) rpc.PutReply {
	sh := shardcfg.Key2Shard(args.Key)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	st := kv.ensureShardLocked(sh)
	if !st.owned || st.frozen {
		reply := rpc.PutReply{Err: rpc.ErrWrongGroup}
		kv.storePutLocked(st, args.ClientId, args.RequestId, reply)
		return reply
	}

	if reply, ok := kv.cachedPutLocked(st, args.ClientId, args.RequestId); ok {
		return reply
	}

	entry, ok := st.store[args.Key]
	if ok {
		if entry.version != args.Version {
			reply := rpc.PutReply{Err: rpc.ErrVersion}
			kv.storePutLocked(st, args.ClientId, args.RequestId, reply)
			return reply
		}
		entry.value = args.Value
		entry.version++
		st.store[args.Key] = entry
		reply := rpc.PutReply{Err: rpc.OK}
		kv.storePutLocked(st, args.ClientId, args.RequestId, reply)
		return reply
	}

	if args.Version != 0 {
		reply := rpc.PutReply{Err: rpc.ErrNoKey}
		kv.storePutLocked(st, args.ClientId, args.RequestId, reply)
		return reply
	}

	st.store[args.Key] = kvEntry{
		value:   args.Value,
		version: 1,
	}
	reply := rpc.PutReply{Err: rpc.OK}
	kv.storePutLocked(st, args.ClientId, args.RequestId, reply)
	return reply
}

func copyTransferState(st *shardState) transferState {
	ts := transferState{
		Store:      make(map[string]snapshotEntry, len(st.store)),
		LastResult: make(map[int64]snapshotClientResult, len(st.lastResult)),
	}
	for k, v := range st.store {
		ts.Store[k] = snapshotEntry{Value: v.value, Version: v.version}
	}
	for id, res := range st.lastResult {
		ts.LastResult[id] = snapshotClientResult{
			RequestId: res.requestId,
			IsGet:     res.isGet,
			GetReply:  res.getReply,
			PutReply:  res.putReply,
		}
	}
	return ts
}

func encodeTransferState(ts transferState) []byte {
	var buf bytes.Buffer
	enc := labgob.NewEncoder(&buf)
	if err := enc.Encode(ts); err != nil {
		return nil
	}
	return buf.Bytes()
}

func decodeTransferState(data []byte) (transferState, bool) {
	if len(data) == 0 {
		return transferState{
			Store:      make(map[string]snapshotEntry),
			LastResult: make(map[int64]snapshotClientResult),
		}, true
	}
	var ts transferState
	dec := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&ts); err != nil {
		return transferState{}, false
	}
	if ts.Store == nil {
		ts.Store = make(map[string]snapshotEntry)
	}
	if ts.LastResult == nil {
		ts.LastResult = make(map[int64]snapshotClientResult)
	}
	return ts, true
}

func (kv *KVServer) applyFreeze(args shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	st := kv.ensureShardLocked(args.Shard)
	if args.Num < st.num || !st.owned {
		srvLog("gid=%d freeze shard=%d num=%d owned=%v reject currentNum=%d", kv.gid, args.Shard, args.Num, st.owned, st.num)
		return shardrpc.FreezeShardReply{Err: rpc.ErrWrongGroup, Num: st.num}
	}

	if args.Num > st.num {
		srvLog("gid=%d freeze shard=%d advancing num %d->%d", kv.gid, args.Shard, st.num, args.Num)
		st.num = args.Num
	}
	st.frozen = true

	state := copyTransferState(st)
	return shardrpc.FreezeShardReply{
		State: encodeTransferState(state),
		Num:   st.num,
		Err:   rpc.OK,
	}
}

func (kv *KVServer) applyInstall(args shardrpc.InstallShardArgs) shardrpc.InstallShardReply {
	ts, ok := decodeTransferState(args.State)
	if !ok {
		return shardrpc.InstallShardReply{Err: rpc.ErrMaybe}
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	st := kv.ensureShardLocked(args.Shard)
	if args.Num < st.num {
		srvLog("gid=%d install shard=%d num=%d < currentNum=%d reject", kv.gid, args.Shard, args.Num, st.num)
		return shardrpc.InstallShardReply{Err: rpc.ErrWrongGroup}
	}

	if args.Num == st.num && st.owned && !st.frozen {
		srvLog("gid=%d install shard=%d num=%d already installed", kv.gid, args.Shard, args.Num)
		return shardrpc.InstallShardReply{Err: rpc.OK}
	}

	if args.Num > st.num {
		srvLog("gid=%d install shard=%d advancing num %d->%d", kv.gid, args.Shard, st.num, args.Num)
		st.num = args.Num
	}

	if st.store == nil {
		st.store = make(map[string]kvEntry, len(ts.Store))
	}
	for k, v := range ts.Store {
		curr, ok := st.store[k]
		if !ok || curr.version <= v.Version {
			st.store[k] = kvEntry{value: v.Value, version: v.Version}
		}
	}

	if st.lastResult == nil {
		st.lastResult = make(map[int64]clientResult, len(ts.LastResult))
	}
	for id, res := range ts.LastResult {
		curr, ok := st.lastResult[id]
		if ok && curr.requestId > res.RequestId {
			continue
		}
		st.lastResult[id] = clientResult{
			requestId: res.RequestId,
			isGet:     res.IsGet,
			getReply:  res.GetReply,
			putReply:  res.PutReply,
		}
	}

	st.owned = true
	st.frozen = false
	srvLog("gid=%d install shard=%d complete keys=%d", kv.gid, args.Shard, len(st.store))

	return shardrpc.InstallShardReply{Err: rpc.OK}
}

func (kv *KVServer) applyDelete(args shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	st := kv.ensureShardLocked(args.Shard)
	if args.Num < st.num {
		srvLog("gid=%d delete shard=%d num=%d < currentNum=%d reject", kv.gid, args.Shard, args.Num, st.num)
		return shardrpc.DeleteShardReply{Err: rpc.ErrWrongGroup}
	}

	if args.Num > st.num {
		srvLog("gid=%d delete shard=%d advancing num %d->%d", kv.gid, args.Shard, st.num, args.Num)
		st.num = args.Num
	}

	st.owned = false
	st.frozen = false
	st.store = make(map[string]kvEntry)
	st.lastResult = make(map[int64]clientResult)
	srvLog("gid=%d delete shard=%d complete", kv.gid, args.Shard)

	return shardrpc.DeleteShardReply{Err: rpc.OK}
}

func (kv *KVServer) DoOp(req any) any {
	switch v := req.(type) {
	case rpc.GetArgs:
		return kv.applyGet(v)
	case rpc.PutArgs:
		return kv.applyPut(v)
	case shardrpc.FreezeShardArgs:
		return kv.applyFreeze(v)
	case shardrpc.InstallShardArgs:
		return kv.applyInstall(v)
	case shardrpc.DeleteShardArgs:
		return kv.applyDelete(v)
	default:
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	state := snapshotState{
		Shards: make(map[shardcfg.Tshid]snapshotShardState, len(kv.shards)),
	}
	for sh, st := range kv.shards {
		shSnapshot := snapshotShardState{
			Owned:      st.owned,
			Frozen:     st.frozen,
			Num:        st.num,
			Store:      make(map[string]snapshotEntry, len(st.store)),
			LastResult: make(map[int64]snapshotClientResult, len(st.lastResult)),
		}
		for k, v := range st.store {
			shSnapshot.Store[k] = snapshotEntry{Value: v.value, Version: v.version}
		}
		for id, res := range st.lastResult {
			if res.isGet {
				continue
			}
			shSnapshot.LastResult[id] = snapshotClientResult{
				RequestId: res.requestId,
				IsGet:     false,
				PutReply:  res.putReply,
			}
		}
		state.Shards[sh] = shSnapshot
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
	dec := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := dec.Decode(&state); err != nil {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.shards = make(map[shardcfg.Tshid]*shardState, len(state.Shards))
	for sh, ss := range state.Shards {
		st := &shardState{
			owned:      ss.Owned,
			frozen:     ss.Frozen,
			num:        ss.Num,
			store:      make(map[string]kvEntry, len(ss.Store)),
			lastResult: make(map[int64]clientResult, len(ss.LastResult)),
		}
		for k, v := range ss.Store {
			st.store[k] = kvEntry{value: v.Value, version: v.Version}
		}
		for id, res := range ss.LastResult {
			st.lastResult[id] = clientResult{
				requestId: res.RequestId,
				isGet:     res.IsGet,
				getReply:  res.GetReply,
				putReply:  res.PutReply,
			}
		}
		kv.shards[sh] = st
	}
	for i := 0; i < shardcfg.NShards; i++ {
		sh := shardcfg.Tshid(i)
		if _, ok := kv.shards[sh]; !ok {
			kv.shards[sh] = newShardState(false)
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

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = res.(shardrpc.DeleteShardReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) initShards() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.shards == nil {
		kv.shards = make(map[shardcfg.Tshid]*shardState, shardcfg.NShards)
	}
	for i := 0; i < shardcfg.NShards; i++ {
		sh := shardcfg.Tshid(i)
		if _, ok := kv.shards[sh]; !ok {
			kv.shards[sh] = newShardState(kv.gid == shardcfg.Gid1)
		}
	}
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(snapshotState{})
	labgob.Register(snapshotShardState{})
	labgob.Register(snapshotEntry{})
	labgob.Register(snapshotClientResult{})
	labgob.Register(transferState{})

	kv := &KVServer{
		gid:    gid,
		me:     me,
		shards: make(map[shardcfg.Tshid]*shardState, shardcfg.NShards),
	}
	kv.initShards()
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []tester.IService{kv, kv.rsm.Raft()}
}
