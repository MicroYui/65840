package shardgrp

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
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

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	mu       sync.Mutex
	seqNum   int
	clerkId  int64
	leaderId int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	ck.leaderId = 0
	ck.seqNum = 0
	ck.clerkId = nrand()
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	ck.mu.Lock()
	ck.seqNum++
	args := rpc.GetArgs{
		Key:     key,
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,
	}
	server := ck.leaderId
	ck.mu.Unlock()

	DPrintf("[GrpClerk %d] Starting Get for Key '%s'", ck.clerkId, key)
	for {
		DPrintf("[GrpClerk %d] Get Key '%s': Trying server %d", ck.clerkId, key, server)
		var reply rpc.GetReply
		ok := ck.clnt.Call(ck.servers[server], "KVServer.Get", &args, &reply)
		if ok {
			DPrintf("[GrpClerk %d] Get Key '%s': Server %d replied with Err: %s", ck.clerkId, key, server, reply.Err)
			if reply.Err == rpc.ErrWrongGroup {
				return "", 0, rpc.ErrWrongGroup
			}
			if reply.Err == rpc.OK {
				ck.mu.Lock()
				ck.leaderId = server
				ck.mu.Unlock()
				return reply.Value, reply.Version, rpc.OK
			}

			if reply.Err == rpc.ErrNoKey {
				ck.mu.Lock()
				ck.leaderId = server
				ck.mu.Unlock()
				return "", 0, rpc.ErrNoKey
			}
		}
		server = (server + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	ck.mu.Lock()
	ck.seqNum++
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,
	}
	server := ck.leaderId
	ck.mu.Unlock()

	ntries := 0

	DPrintf("[GrpClerk %d] Starting Put for Key '%s'", ck.clerkId, key)
	for {
		DPrintf("[GrpClerk %d] Put Key '%s': Trying server %d", ck.clerkId, key, server)
		ntries++
		var reply rpc.PutReply
		ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", &args, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrMaybe {
			server = (server + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if ok {
			DPrintf("[GrpClerk %d] Put Key '%s': Server %d replied with Err: %s", ck.clerkId, key, server, reply.Err)
			if reply.Err == rpc.ErrWrongGroup {
				return rpc.ErrWrongGroup
			}
			if reply.Err == rpc.ErrVersion {
				if ntries == 1 {
					reply.Err = rpc.ErrVersion
				} else {
					reply.Err = rpc.ErrMaybe
				}
			}
		}

		ck.mu.Lock()
		ck.leaderId = server
		ck.mu.Unlock()

		return reply.Err
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{
		Shard: s,
		Num:   num,
	}

	server := ck.leaderId
	DPrintf("[GrpClerk %d] Starting FreezeShard for Shard %d (Num %d)", ck.clerkId, s, num)
	for {
		DPrintf("[GrpClerk %d] FreezeShard %d: Trying server %d", ck.clerkId, s, server)
		var reply shardrpc.FreezeShardReply
		ok := ck.clnt.Call(ck.servers[server], "KVServer.FreezeShard", &args, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if ok {
			DPrintf("[GrpClerk %d] FreezeShard %d: Server %d replied with Err: %s", ck.clerkId, s, server, reply.Err)
			ck.mu.Lock()
			ck.leaderId = server
			ck.mu.Unlock()
			return reply.State, reply.Err
		}
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{
		Shard: s,
		State: state,
		Num:   num,
	}

	server := ck.leaderId
	DPrintf("[GrpClerk %d] Starting InstallShard for Shard %d (Num %d)", ck.clerkId, s, num)
	for {
		DPrintf("[GrpClerk %d] InstallShard %d: Trying server %d", ck.clerkId, s, server)
		var reply shardrpc.InstallShardReply
		ok := ck.clnt.Call(ck.servers[server], "KVServer.InstallShard", &args, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader {
			server = (server + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if ok {
			DPrintf("[GrpClerk %d] InstallShard %d: Server %d replied with Err: %s", ck.clerkId, s, server, reply.Err)
			ck.mu.Lock()
			ck.leaderId = server
			ck.mu.Unlock()
			return reply.Err
		}
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{
		Shard: s,
		Num:   num,
	}

	server := ck.leaderId
	DPrintf("[GrpClerk %d] Starting DeleteShard for Shard %d (Num %d)", ck.clerkId, s, num)
	for {
		DPrintf("[GrpClerk %d] DeleteShard %d: Trying server %d", ck.clerkId, s, server)
		var reply shardrpc.DeleteShardReply
		ok := ck.clnt.Call(ck.servers[server], "KVServer.DeleteShard", &args, &reply)

		if ok {
			DPrintf("[GrpClerk %d] DeleteShard %d: Server %d replied with Err: %s", ck.clerkId, s, server, reply.Err)
			ck.mu.Lock()
			ck.leaderId = server
			ck.mu.Unlock()
			return reply.Err
		}
	}
}
