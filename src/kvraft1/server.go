package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
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

	// Your definitions here.
	mu sync.Mutex                  // lock
	db map[string]ValueWithVersion // kv database
	// 两个 map 用于请求去重和缓存答复
	processedReqs map[int64]int
	cachedReplies map[int64]any
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		if lastSeqNum, ok := kv.processedReqs[args.ClerkId]; ok && args.SeqNum <= lastSeqNum {
			// 如果是重复请求，直接返回缓存的答复
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

		kv.processedReqs[args.ClerkId] = args.SeqNum
		kv.cachedReplies[args.ClerkId] = reply

		return reply
	case rpc.PutArgs:
		if lastSeqNum, ok := kv.processedReqs[args.ClerkId]; ok && args.SeqNum <= lastSeqNum {
			// 如果是重复请求，直接返回缓存的答复
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
				reply.Err = rpc.ErrNoKey
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
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
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
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(rpc.PutReply{})

	kv := &KVServer{me: me}
	kv.db = make(map[string]ValueWithVersion)
	kv.processedReqs = make(map[int64]int)
	kv.cachedReplies = make(map[int64]any)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
