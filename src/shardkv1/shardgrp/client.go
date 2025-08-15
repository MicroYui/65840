package shardgrp

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
)

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

	for {
		var reply rpc.GetReply
		ok := ck.clnt.Call(ck.servers[server], "KVServer.Get", &args, &reply)
		if ok {
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

	for {
		ntries++
		var reply rpc.PutReply
		ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", &args, &reply)

		if !ok || reply.Err == rpc.ErrWrongLeader || reply.Err == rpc.ErrMaybe {
			server = (server + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if ok {
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
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}
