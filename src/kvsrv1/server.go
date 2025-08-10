package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueWithVersion struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	db map[string]ValueWithVersion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.db[args.Key]

	if ok {
		reply.Value = value.Value
		reply.Version = value.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("Put received: Key=%s, Value=%s, Version=%d", args.Key, args.Value, args.Version)

	value, ok := kv.db[args.Key]

	DPrintf("Server state: Key=%s, Exists=%v, StoredEntry=%+v", args.Key, ok, value)

	if !ok {
		if args.Version == 0 {
			kv.db[args.Key] = ValueWithVersion{
				Value:   args.Value,
				Version: 1,
			}
			reply.Err = rpc.OK
			DPrintf("Action: Created key %s. Reply: OK", args.Key)
		} else {
			reply.Err = rpc.ErrNoKey
			DPrintf("Action: Rejected Put for non-existent key %s with non-zero version. Reply: ErrNoKey", args.Key)
		}
	} else {
		if args.Version == value.Version {
			kv.db[args.Key] = ValueWithVersion{
				Value:   args.Value,
				Version: args.Version + 1,
			}
			reply.Err = rpc.OK
			DPrintf("Action: Updated key %s. Reply: OK", args.Key)
		} else {
			reply.Err = rpc.ErrVersion
			DPrintf("Action: Rejected Put for key %s due to version mismatch. Reply: ErrVersion", args.Key)
		}
	}

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	kv.db = make(map[string]ValueWithVersion)
	return []tester.IService{kv}
}
