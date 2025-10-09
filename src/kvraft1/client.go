package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int

	clientId  int64
	requestId uint64
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
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
				if reply.Err == rpc.ErrWrongLeader {
					continue
				}
				if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
					ck.leader = srv
					return reply.Value, reply.Version, reply.Err
				}
				return "", 0, reply.Err
			}
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
				switch reply.Err {
				case rpc.OK:
					ck.leader = srv
					return rpc.OK
				case rpc.ErrWrongLeader:
					continue
				case rpc.ErrVersion:
					if firstTry {
						return rpc.ErrVersion
					}
					return rpc.ErrMaybe
				default:
					return reply.Err
				}
			} else {
				firstTry = false
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
