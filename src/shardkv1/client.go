package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	// You will have to modify this struct.
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	// You'll have to add code here.
	return ck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	DPrintf("[OuterClerk] Starting Get for Key '%s'", key)
	for {
		// 获取最新的配置
		config := ck.sck.Query()
		// 根据 key 找到对应的 shard
		shard := shardcfg.Key2Shard(key)
		// 从配置中找到负责该 shard 的 group id (gid)
		gid := config.Shards[shard]
		DPrintf("[OuterClerk] Get Key '%s': Query config, found Shard %d is on GID %d", key, shard, gid)

		// 根据 gid 找到对应的服务器列表
		if servers, ok := config.Groups[gid]; ok {
			// 为这个 group 创建一个内层客户端
			grp_ck := shardgrp.MakeClerk(ck.clnt, servers)
			// 使用内层客户端发送真正的 Get 请求
			val, ver, err := grp_ck.Get(key)

			if err == rpc.ErrWrongGroup {
				// 如果是 WrongGroup，说明配置过时了，继续循环以获取新配置
				DPrintf("[OuterClerk] Get Key '%s': GID %d reported ErrWrongGroup. Retrying.", key, gid)
				time.Sleep(500 * time.Millisecond) // 在配置变更期间，短暂等待
				continue
			}

			DPrintf("[OuterClerk] Get Key '%s': GID %d returned Err: %s. Finished.", key, gid, err)
			return val, ver, err
		}
		DPrintf("[OuterClerk] Get Key '%s': GID %d not found in config groups. Retrying.", key, gid)
		// time.Sleep(20 * time.Millisecond)
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	// Put 的逻辑与 Get 完全相同，只是调用的内层方法不同
	DPrintf("[OuterClerk] Starting Put for Key '%s'", key)
	for {
		config := ck.sck.Query()
		shard := shardcfg.Key2Shard(key)
		gid := config.Shards[shard]
		DPrintf("[OuterClerk] Put Key '%s': Query config, found Shard %d is on GID %d", key, shard, gid)

		if servers, ok := config.Groups[gid]; ok {
			grp_ck := shardgrp.MakeClerk(ck.clnt, servers)
			err := grp_ck.Put(key, value, version)
			if err == rpc.ErrWrongGroup {
				DPrintf("[OuterClerk] Put Key '%s': GID %d reported ErrWrongGroup. Retrying.", key, gid)
				time.Sleep(500 * time.Millisecond)
				continue
			}
			DPrintf("[OuterClerk] Put Key '%s': GID %d returned Err: %s. Finished.", key, gid, err)
			return err
		}
		DPrintf("[OuterClerk] Put Key '%s': GID %d not found in config groups. Retrying.", key, gid)
		// time.Sleep(20 * time.Millisecond)
	}
}
