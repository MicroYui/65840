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
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const debugClient = false

func clientLog(format string, args ...any) {
	if !debugClient {
		return
	}
	log.Printf("[shardkv-client] "+format, args...)
}

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler

	mu     sync.Mutex
	config *shardcfg.ShardConfig
	clerks map[tester.Tgid]*shardgrp.Clerk
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

func (ck *Clerk) getConfigLocked() *shardcfg.ShardConfig {
	if ck.config == nil {
		ck.config = ck.sck.Query()
		if ck.clerks != nil {
			ck.clerks = make(map[tester.Tgid]*shardgrp.Clerk)
		}
	}
	return ck.config
}

func (ck *Clerk) refreshConfigLocked() {
	prev := shardcfg.Tnum(0)
	if ck.config != nil {
		prev = ck.config.Num
	}
	cfg := ck.sck.Query()
	ck.config = cfg
	ck.clerks = make(map[tester.Tgid]*shardgrp.Clerk)
	clientLog("refresh config from #%d to #%d", prev, cfg.Num)
}

func (ck *Clerk) getShardClerkLocked(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	if ck.clerks == nil {
		ck.clerks = make(map[tester.Tgid]*shardgrp.Clerk)
	}
	if c, ok := ck.clerks[gid]; ok {
		return c
	}
	c := shardgrp.MakeClerk(ck.clnt, servers)
	ck.clerks[gid] = c
	clientLog("created shardgrp clerk gid=%d servers=%v", gid, servers)
	return c
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		ck.mu.Lock()
		cfg := ck.getConfigLocked()
		sh := shardcfg.Key2Shard(key)
		gid, servers, ok := cfg.GidServers(sh)
		if !ok || gid == 0 || len(servers) == 0 {
			clientLog("shard %d missing assignment in cfg #%d, refresh", sh, cfg.Num)
			ck.refreshConfigLocked()
			ck.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		gck := ck.getShardClerkLocked(gid, servers)
		ck.mu.Unlock()

		value, version, err := gck.Get(key)
		switch err {
		case rpc.OK, rpc.ErrNoKey:
			clientLog("Get key=%s shard=%d gid=%d ok err=%v", key, sh, gid, err)
			return value, version, err
		case rpc.ErrWrongGroup:
			clientLog("Get key=%s shard=%d gid=%d wrong group, refresh", key, sh, gid)
			ck.mu.Lock()
			ck.refreshConfigLocked()
			ck.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		default:
			clientLog("Get key=%s shard=%d gid=%d unexpected err=%v", key, sh, gid, err)
			return value, version, err
		}
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	for {
		ck.mu.Lock()
		cfg := ck.getConfigLocked()
		sh := shardcfg.Key2Shard(key)
		gid, servers, ok := cfg.GidServers(sh)
		if !ok || gid == 0 || len(servers) == 0 {
			clientLog("Put key=%s shard=%d cfg #%d missing group, refresh", key, sh, cfg.Num)
			ck.refreshConfigLocked()
			ck.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			continue
		}
		gck := ck.getShardClerkLocked(gid, servers)
		ck.mu.Unlock()

		err := gck.Put(key, value, version)
		switch err {
		case rpc.OK, rpc.ErrVersion, rpc.ErrNoKey, rpc.ErrMaybe:
			clientLog("Put key=%s shard=%d gid=%d result=%v", key, sh, gid, err)
			return err
		case rpc.ErrWrongGroup:
			clientLog("Put key=%s shard=%d gid=%d wrong group, refresh", key, sh, gid)
			ck.mu.Lock()
			ck.refreshConfigLocked()
			ck.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		default:
			clientLog("Put key=%s shard=%d gid=%d unexpected err=%v", key, sh, gid, err)
			return err
		}
	}
}
