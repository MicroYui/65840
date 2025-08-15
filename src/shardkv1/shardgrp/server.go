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
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type ValueWithVersion struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu            sync.Mutex
	db            map[string]ValueWithVersion
	processedReqs map[int64]int
	cachedReplies map[int64]any
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		// 检查重复请求
		if lastSeqNum, ok := kv.processedReqs[args.ClerkId]; ok && args.SeqNum <= lastSeqNum {
			return kv.cachedReplies[args.ClerkId]
		}

		value, ok := kv.db[args.Key]
		var reply rpc.GetReply
		if ok {
			reply.Value = value.Value
			reply.Version = value.Version
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}

		// 更新去重信息和缓存
		kv.processedReqs[args.ClerkId] = args.SeqNum
		kv.cachedReplies[args.ClerkId] = reply
		return reply

	case rpc.PutArgs:
		// 检查重复请求
		if lastSeqNum, ok := kv.processedReqs[args.ClerkId]; ok && args.SeqNum <= lastSeqNum {
			return kv.cachedReplies[args.ClerkId]
		}

		value, ok := kv.db[args.Key]
		var reply rpc.PutReply
		if !ok {
			if args.Version == 0 {
				kv.db[args.Key] = ValueWithVersion{
					Value:   args.Value,
					Version: 1,
				}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrMaybe
			}
		} else {
			if args.Version == value.Version {
				kv.db[args.Key] = ValueWithVersion{
					Value:   args.Value,
					Version: args.Version + 1,
				}
				reply.Err = rpc.OK
			} else {
				reply.Err = rpc.ErrVersion
			}
		}

		// 更新去重信息和缓存
		kv.processedReqs[args.ClerkId] = args.SeqNum
		kv.cachedReplies[args.ClerkId] = reply
		return reply
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.db) != nil ||
		e.Encode(kv.processedReqs) != nil ||
		e.Encode(kv.cachedReplies) != nil {
		log.Fatalf("KVServer %d: failed to encode state for snapshot", kv.me)
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]ValueWithVersion
	var processedReqs map[int64]int
	var cachedReplies map[int64]any
	if d.Decode(&db) != nil ||
		d.Decode(&processedReqs) != nil ||
		d.Decode(&cachedReplies) != nil {
		log.Fatalf("KVServer %d: failed to restore from snapshot", kv.me)
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.db = db
	kv.processedReqs = processedReqs
	kv.cachedReplies = cachedReplies
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	reply.Err = err

	if err == rpc.OK {
		getReply, ok := result.(rpc.GetReply)
		if ok {
			reply.Value = getReply.Value
			reply.Version = getReply.Version
			reply.Err = getReply.Err
		} else {
			reply.Err = rpc.ErrWrongLeader
		}
	}
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	err, result := kv.rsm.Submit(*args)
	reply.Err = err

	if err == rpc.OK {
		putReply, ok := result.(rpc.PutReply)
		if ok {
			reply.Err = putReply.Err
		} else {
			reply.Err = rpc.ErrWrongLeader
		}
	}
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
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
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
	labgob.Register(rpc.GetReply{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(ValueWithVersion{})

	kv := &KVServer{gid: gid, me: me}

	kv.db = make(map[string]ValueWithVersion)
	kv.processedReqs = make(map[int64]int)
	kv.cachedReplies = make(map[int64]any)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
