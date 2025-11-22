package shardgrp

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const debugShardgrpClerk = false

func sgLog(format string, args ...any) {
	if !debugShardgrpClerk {
		return
	}
	log.Printf("[shardgrp-clerk] "+format, args...)
}

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int

	clientId  int64
	requestId uint64
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	return &Clerk{
		clnt:     clnt,
		servers:  servers,
		clientId: nrand(),
	}
}

func (ck *Clerk) nextRequestId() uint64 {
	return atomic.AddUint64(&ck.requestId, 1)
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	reqId := ck.nextRequestId()
	var reply rpc.GetReply
	n := len(ck.servers)

	for {
		contacted := false
		start := ck.leader % n
		for offset := 0; offset < n; offset++ {
			srv := (start + offset) % n
			args := rpc.GetArgs{
				Key:       key,
				ClientId:  ck.clientId,
				RequestId: reqId,
			}
			reply = rpc.GetReply{}
			if ok := ck.clnt.Call(ck.servers[srv], "KVServer.Get", &args, &reply); ok {
				contacted = true
				switch reply.Err {
				case rpc.OK, rpc.ErrNoKey:
					ck.leader = srv
					sgLog("Get key=%s gid servers=%v leader=%d result=%v", key, ck.servers, srv, reply.Err)
					return reply.Value, reply.Version, reply.Err
				case rpc.ErrWrongLeader:
					sgLog("Get key=%s gid servers=%v server=%s wrong leader", key, ck.servers, ck.servers[srv])
					continue
				default:
					sgLog("Get key=%s gid servers=%v server=%s err=%v", key, ck.servers, ck.servers[srv], reply.Err)
					return "", 0, reply.Err
				}
			} else {
				sgLog("Get key=%s gid servers=%v server=%s unreachable", key, ck.servers, ck.servers[srv])
			}
		}
		if !contacted {
			sgLog("Get key=%s gid servers=%v all servers unreachable, returning ErrWrongGroup", key, ck.servers)
			return "", 0, rpc.ErrWrongGroup
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	reqId := ck.nextRequestId()
	var reply rpc.PutReply
	n := len(ck.servers)
	firstTry := true

	for {
		contacted := false
		start := ck.leader % n
		for offset := 0; offset < n; offset++ {
			srv := (start + offset) % n
			args := rpc.PutArgs{
				Key:       key,
				Value:     value,
				Version:   version,
				ClientId:  ck.clientId,
				RequestId: reqId,
			}
			reply = rpc.PutReply{}
			if ok := ck.clnt.Call(ck.servers[srv], "KVServer.Put", &args, &reply); ok {
				contacted = true
				switch reply.Err {
				case rpc.OK:
					ck.leader = srv
					sgLog("Put key=%s gid servers=%v leader=%d ok", key, ck.servers, srv)
					return rpc.OK
				case rpc.ErrWrongLeader:
					sgLog("Put key=%s gid servers=%v server=%s wrong leader", key, ck.servers, ck.servers[srv])
					continue
				case rpc.ErrVersion:
					sgLog("Put key=%s gid servers=%v version conflict", key, ck.servers)
					if firstTry {
						return rpc.ErrVersion
					}
					return rpc.ErrMaybe
				default:
					sgLog("Put key=%s gid servers=%v server=%s err=%v", key, ck.servers, ck.servers[srv], reply.Err)
					return reply.Err
				}
			} else {
				sgLog("Put key=%s gid servers=%v server=%s unreachable", key, ck.servers, ck.servers[srv])
				firstTry = false
			}
		}
		if !contacted {
			sgLog("Put key=%s gid servers=%v all servers unreachable, returning ErrWrongGroup", key, ck.servers)
			return rpc.ErrWrongGroup
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	var reply shardrpc.FreezeShardReply
	n := len(ck.servers)

	for {
		start := ck.leader % n
		for offset := 0; offset < n; offset++ {
			srv := (start + offset) % n
			args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
			reply = shardrpc.FreezeShardReply{}
			if ok := ck.clnt.Call(ck.servers[srv], "KVServer.FreezeShard", &args, &reply); ok {
				switch reply.Err {
				case rpc.OK, rpc.ErrWrongGroup:
					ck.leader = srv
					sgLog("Freeze shard=%d gid servers=%v num=%d err=%v state=%d", s, ck.servers, num, reply.Err, len(reply.State))
					return reply.State, reply.Err
				case rpc.ErrWrongLeader:
					sgLog("Freeze shard=%d gid servers=%v server=%s wrong leader", s, ck.servers, ck.servers[srv])
					continue
				default:
					sgLog("Freeze shard=%d gid servers=%v server=%s err=%v", s, ck.servers, ck.servers[srv], reply.Err)
					return nil, reply.Err
				}
			} else {
				sgLog("Freeze shard=%d gid servers=%v server=%s unreachable", s, ck.servers, ck.servers[srv])
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	var reply shardrpc.InstallShardReply
	n := len(ck.servers)

	for {
		start := ck.leader % n
		for offset := 0; offset < n; offset++ {
			srv := (start + offset) % n
			args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
			reply = shardrpc.InstallShardReply{}
			if ok := ck.clnt.Call(ck.servers[srv], "KVServer.InstallShard", &args, &reply); ok {
				switch reply.Err {
				case rpc.OK, rpc.ErrWrongGroup:
					ck.leader = srv
					sgLog("Install shard=%d gid servers=%v num=%d err=%v", s, ck.servers, num, reply.Err)
					return reply.Err
				case rpc.ErrWrongLeader:
					sgLog("Install shard=%d gid servers=%v server=%s wrong leader", s, ck.servers, ck.servers[srv])
					continue
				default:
					sgLog("Install shard=%d gid servers=%v server=%s err=%v", s, ck.servers, ck.servers[srv], reply.Err)
					return reply.Err
				}
			} else {
				sgLog("Install shard=%d gid servers=%v server=%s unreachable", s, ck.servers, ck.servers[srv])
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	var reply shardrpc.DeleteShardReply
	n := len(ck.servers)

	for {
		start := ck.leader % n
		for offset := 0; offset < n; offset++ {
			srv := (start + offset) % n
			args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
			reply = shardrpc.DeleteShardReply{}
			if ok := ck.clnt.Call(ck.servers[srv], "KVServer.DeleteShard", &args, &reply); ok {
				switch reply.Err {
				case rpc.OK, rpc.ErrWrongGroup:
					ck.leader = srv
					sgLog("Delete shard=%d gid servers=%v num=%d err=%v", s, ck.servers, num, reply.Err)
					return reply.Err
				case rpc.ErrWrongLeader:
					sgLog("Delete shard=%d gid servers=%v server=%s wrong leader", s, ck.servers, ck.servers[srv])
					continue
				default:
					sgLog("Delete shard=%d gid servers=%v server=%s err=%v", s, ck.servers, ck.servers[srv], reply.Err)
					return reply.Err
				}
			} else {
				sgLog("Delete shard=%d gid servers=%v server=%s unreachable", s, ck.servers, ck.servers[srv])
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	return bigx.Int64()
}
