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

// 组内一个分片的状态。
type ShardStatus int

const (
	Serving ShardStatus = iota // 服务中
	Frozen                     // 已冻结
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
	mu sync.Mutex

	shardsDB    map[shardcfg.Tshid]map[string]ValueWithVersion // 用于分片的 kv 数据库
	shardStates map[shardcfg.Tshid]ShardStatus                 // 记录每一个分片当前的状态
	shardNums   map[shardcfg.Tshid]shardcfg.Tnum               // 记录每一个分片的版本号

	processedReqs map[int64]int
	cachedReplies map[int64]any
}

func (kv *KVServer) isWrongGroup(shardID shardcfg.Tshid) bool {
	// 查找此分片在其对应位置是否有状态
	_, ok := kv.shardStates[shardID]
	DPrintf("[GID %d Srv %d] Checking shard %d ownership: %v", kv.gid, kv.me, shardID, ok)
	return !ok
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		shardID := shardcfg.Key2Shard(args.Key)
		if kv.isWrongGroup(shardID) || kv.shardStates[shardID] == Frozen {
			// 如果分片不存在，或状态不是 Serving，则拒绝操作
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
		DPrintf("[GID %d Srv %d] DoOp: Processing Get for Key '%s'", kv.gid, kv.me, args.Key)
		// 检查重复请求
		if lastSeqNum, ok := kv.processedReqs[args.ClerkId]; ok && args.SeqNum <= lastSeqNum {
			DPrintf("[GID %d Srv %d] DoOp: Duplicate Get from Clerk %d (Seq %d <= %d)", kv.gid, kv.me, args.ClerkId, args.SeqNum, lastSeqNum)
			return kv.cachedReplies[args.ClerkId]
		}
		// 需要先计算分片 id
		// shardID := shardcfg.Key2Shard(args.Key)
		shardData := kv.shardsDB[shardID]
		value, ok := shardData[args.Key]
		var reply rpc.GetReply
		if ok {
			reply.Value = value.Value
			reply.Version = value.Version
			reply.Err = rpc.OK
			DPrintf("[GID %d Srv %d] DoOp: Get for Key '%s' -> OK", kv.gid, kv.me, args.Key)
		} else {
			reply.Err = rpc.ErrNoKey
			DPrintf("[GID %d Srv %d] DoOp: Get for Key '%s' -> ErrNoKey", kv.gid, kv.me, args.Key)
		}

		// 更新去重信息和缓存
		kv.processedReqs[args.ClerkId] = args.SeqNum
		kv.cachedReplies[args.ClerkId] = reply
		return reply

	case rpc.PutArgs:
		shardID := shardcfg.Key2Shard(args.Key)
		if kv.isWrongGroup(shardID) || kv.shardStates[shardID] == Frozen {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
		DPrintf("[GID %d Srv %d] DoOp: Processing Put for Key '%s'", kv.gid, kv.me, args.Key)
		// 检查重复请求
		if lastSeqNum, ok := kv.processedReqs[args.ClerkId]; ok && args.SeqNum <= lastSeqNum {
			DPrintf("[GID %d Srv %d] DoOp: Duplicate Put from Clerk %d (Seq %d <= %d)", kv.gid, kv.me, args.ClerkId, args.SeqNum, lastSeqNum)
			return kv.cachedReplies[args.ClerkId]
		}

		// 需要先计算分片 id
		// shardID := shardcfg.Key2Shard(args.Key)
		shardData := kv.shardsDB[shardID]
		value, ok := shardData[args.Key]
		var reply rpc.PutReply
		if !ok {
			if args.Version == 0 {
				shardData[args.Key] = ValueWithVersion{
					Value:   args.Value,
					Version: 1,
				}
				reply.Err = rpc.OK
				DPrintf("[GID %d Srv %d] DoOp: Put for new Key '%s' -> OK", kv.gid, kv.me, args.Key)
			} else {
				reply.Err = rpc.ErrMaybe
				DPrintf("[GID %d Srv %d] DoOp: Put for new Key '%s' with Version %d -> ErrMaybe", kv.gid, kv.me, args.Key, args.Version)
			}
		} else {
			if args.Version == value.Version {
				shardData[args.Key] = ValueWithVersion{
					Value:   args.Value,
					Version: args.Version + 1,
				}
				reply.Err = rpc.OK
				DPrintf("[GID %d Srv %d] DoOp: Put for existing Key '%s' (Version %d) -> OK", kv.gid, kv.me, args.Key, args.Version)
			} else {
				reply.Err = rpc.ErrVersion
				DPrintf("[GID %d Srv %d] DoOp: Put for existing Key '%s' -> ErrVersion (Req: %d, Has: %d)", kv.gid, kv.me, args.Key, args.Version, value.Version)
			}
		}

		// 更新去重信息和缓存
		kv.processedReqs[args.ClerkId] = args.SeqNum
		kv.cachedReplies[args.ClerkId] = reply
		return reply
	case shardrpc.FreezeShardArgs:
		DPrintf("[GID %d Srv %d] DoOp: Processing FreezeShard for Shard %d (Num %d). Current Num: %d", kv.gid, kv.me, args.Shard, args.Num, kv.shardNums[args.Shard])
		var reply shardrpc.FreezeShardReply
		if args.Num >= kv.shardNums[args.Shard] {
			kv.shardStates[args.Shard] = Frozen
			kv.shardNums[args.Shard] = args.Num
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.shardsDB[args.Shard])
			reply.State = w.Bytes()
			reply.Err = rpc.OK
			DPrintf("[GID %d Srv %d] DoOp: Froze Shard %d for Num %d -> OK", kv.gid, kv.me, args.Shard, args.Num)
		} else {
			reply.Err = rpc.ErrMaybe
		}
		reply.Num = kv.shardNums[args.Shard]
		return reply
	case shardrpc.InstallShardArgs:
		DPrintf("[GID %d Srv %d] DoOp: Processing InstallShard for Shard %d (Num %d). Current Num: %d", kv.gid, kv.me, args.Shard, args.Num, kv.shardNums[args.Shard])
		var reply shardrpc.InstallShardReply
		if args.Num >= kv.shardNums[args.Shard] {
			r := bytes.NewBuffer(args.State)
			d := labgob.NewDecoder(r)
			var shardData map[string]ValueWithVersion
			if d.Decode(&shardData) == nil {
				kv.shardsDB[args.Shard] = shardData
				kv.shardStates[args.Shard] = Serving
				kv.shardNums[args.Shard] = args.Num
				reply.Err = rpc.OK
				DPrintf("[GID %d Srv %d] DoOp: Installed Shard %d for Num %d -> OK", kv.gid, kv.me, args.Shard, args.Num)
			} else {
				log.Fatal("install shard decode error")
			}
		} else {
			reply.Err = rpc.ErrMaybe
		}
		return reply
	case shardrpc.DeleteShardArgs:
		DPrintf("[GID %d Srv %d] DoOp: Processing DeleteShard for Shard %d (Num %d). Current Num: %d", kv.gid, kv.me, args.Shard, args.Num, kv.shardNums[args.Shard])
		var reply shardrpc.DeleteShardReply
		if args.Num >= kv.shardNums[args.Shard] {
			delete(kv.shardsDB, args.Shard)
			delete(kv.shardStates, args.Shard)
			delete(kv.shardNums, args.Shard)
			reply.Err = rpc.OK
			DPrintf("[GID %d Srv %d] DoOp: Deleted Shard %d for Num %d -> OK", kv.gid, kv.me, args.Shard, args.Num)
		} else {
			reply.Err = rpc.ErrMaybe
		}
		return reply
	}
	DPrintf("[GID %d Srv %d] DoOp: Received unknown request type %T", kv.gid, kv.me, req)
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.shardsDB) != nil ||
		e.Encode(kv.shardStates) != nil ||
		e.Encode(kv.shardNums) != nil {
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
	var shardsDB map[shardcfg.Tshid]map[string]ValueWithVersion
	var shardStates map[shardcfg.Tshid]ShardStatus
	var shardNums map[shardcfg.Tshid]shardcfg.Tnum
	if d.Decode(&shardsDB) != nil ||
		d.Decode(&shardStates) != nil ||
		d.Decode(&shardNums) != nil {
		log.Fatalf("KVServer %d: failed to restore from snapshot", kv.me)
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.shardsDB = shardsDB
	kv.shardStates = shardStates
	kv.shardNums = shardNums
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	// 先检查分片是否在组内
	kv.mu.Lock()
	DPrintf("[GID %d Srv %d] RPC: Received Get for Key '%s'", kv.gid, kv.me, args.Key)
	shardId := shardcfg.Key2Shard(args.Key)
	if kv.isWrongGroup(shardId) || kv.shardStates[shardId] == Frozen {
		reply.Err = rpc.ErrWrongGroup
		DPrintf("[GID %d Srv %d] RPC Get: Key '%s' (Shard %d) -> ErrWrongGroup", kv.gid, kv.me, args.Key, shardId)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf("[GID %d Srv %d] RPC Get: Submitting Get for Key '%s' to RSM", kv.gid, kv.me, args.Key)
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
	DPrintf("[GID %d Srv %d] RPC Get: Key '%s' completed with Err: %s", kv.gid, kv.me, args.Key, reply.Err)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	// Put 还需要检查分片是否处于 frozen 状态
	kv.mu.Lock()
	DPrintf("[GID %d Srv %d] RPC: Received Put for Key '%s'", kv.gid, kv.me, args.Key)
	shardId := shardcfg.Key2Shard(args.Key)
	if kv.isWrongGroup(shardId) || kv.shardStates[shardId] == Frozen {
		reply.Err = rpc.ErrWrongGroup
		DPrintf("[GID %d Srv %d] RPC Put: Key '%s' (Shard %d) -> ErrWrongGroup (isWrongGroup: %v, state: %v)", kv.gid, kv.me, args.Key, shardId, kv.isWrongGroup(shardId), kv.shardStates[shardId])
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	DPrintf("[GID %d Srv %d] RPC Put: Submitting Put for Key '%s' to RSM", kv.gid, kv.me, args.Key)
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
	DPrintf("[GID %d Srv %d] RPC Put: Key '%s' completed with Err: %s", kv.gid, kv.me, args.Key, reply.Err)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	DPrintf("[GID %d Srv %d] RPC: Received FreezeShard for Shard %d (Num %d)", kv.gid, kv.me, args.Shard, args.Num)
	err, result := kv.rsm.Submit(*args)
	reply.Err = err

	if err == rpc.OK {
		freezeReply, ok := result.(shardrpc.FreezeShardReply)
		if ok {
			reply.Num = freezeReply.Num
			reply.State = freezeReply.State
			reply.Err = freezeReply.Err
		} else {
			reply.Err = rpc.ErrWrongLeader
		}
	}

	DPrintf("[GID %d Srv %d] RPC FreezeShard: Shard %d completed with Err: %s", kv.gid, kv.me, args.Shard, reply.Err)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	DPrintf("[GID %d Srv %d] RPC: Received InstallShard for Shard %d (Num %d)", kv.gid, kv.me, args.Shard, args.Num)
	err, result := kv.rsm.Submit(*args)
	reply.Err = err

	if err == rpc.OK {
		freezeReply, ok := result.(shardrpc.InstallShardReply)
		if ok {
			reply.Err = freezeReply.Err
		} else {
			reply.Err = rpc.ErrWrongLeader
		}
	}
	DPrintf("[GID %d Srv %d] RPC InstallShard: Shard %d completed with Err: %s", kv.gid, kv.me, args.Shard, reply.Err)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	DPrintf("[GID %d Srv %d] RPC: Received DeleteShard for Shard %d (Num %d)", kv.gid, kv.me, args.Shard, args.Num)
	err, result := kv.rsm.Submit(*args)
	reply.Err = err

	if err == rpc.OK {
		freezeReply, ok := result.(shardrpc.DeleteShardReply)
		if ok {
			reply.Err = freezeReply.Err
		} else {
			reply.Err = rpc.ErrWrongLeader
		}
	}
	DPrintf("[GID %d Srv %d] RPC DeleteShard: Shard %d completed with Err: %s", kv.gid, kv.me, args.Shard, reply.Err)
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
	kv.rsm.Raft().Kill()
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
	labgob.Register(shardrpc.FreezeShardReply{})
	labgob.Register(shardrpc.InstallShardReply{})
	labgob.Register(shardrpc.DeleteShardReply{})
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(ValueWithVersion{})

	kv := &KVServer{gid: gid, me: me}

	kv.shardsDB = make(map[shardcfg.Tshid]map[string]ValueWithVersion)
	kv.shardStates = make(map[shardcfg.Tshid]ShardStatus)
	kv.shardNums = make(map[shardcfg.Tshid]shardcfg.Tnum)
	kv.processedReqs = make(map[int64]int)
	kv.cachedReplies = make(map[int64]any)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here
	for i := range shardcfg.NShards {
		shardID := shardcfg.Tshid(i)
		// 1. 为每个分片创建一个空的 key-value 数据库
		kv.shardsDB[shardID] = make(map[string]ValueWithVersion)
		// 2. 将每个分片的状态设置为 Serving (服务中)
		kv.shardStates[shardID] = Serving
		// 3. 记录每个分片对应的初始配置编号 (config number)
		kv.shardNums[shardID] = 0
	}

	return []tester.IService{kv, kv.rsm.Raft()}
}
