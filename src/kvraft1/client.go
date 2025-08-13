package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
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
	mu       sync.Mutex // lock
	seqNum   int        // 请求序列号
	clerkId  int64      // clerk 序列号
	leaderId int        // 上一次 leader 的 id
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	ck.leaderId = 0
	ck.seqNum = 0
	ck.clerkId = nrand()
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
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

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
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
