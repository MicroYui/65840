package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"sync"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const debugCtrler = false
const logRaceCheck = true
const controllerTimeout = 5 * time.Second

func ctrlerLog(format string, args ...any) {
	if !debugCtrler {
		return
	}
	log.Printf("[shardctrler] "+format, args...)
}

const (
	configKey     = "shard-config"
	nextConfigKey = "shard-config-next"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
	mu             sync.Mutex
	cfgVersion     rpc.Tversion
	nextCfgVersion rpc.Tversion
	resumeNum      shardcfg.Tnum
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

func (sck *ShardCtrler) loadNextConfig() (*shardcfg.ShardConfig, bool) {
	for {
		data, ver, err := sck.IKVClerk.Get(nextConfigKey)
		if err == rpc.ErrNoKey {
			sck.mu.Lock()
			sck.nextCfgVersion = 0
			sck.mu.Unlock()
			return nil, false
		}
		if err == rpc.OK {
			cfg := shardcfg.FromString(data)
			sck.mu.Lock()
			sck.nextCfgVersion = ver
			sck.mu.Unlock()
			return cfg, true
		}
	}
}

func (sck *ShardCtrler) storeNextConfig(data string) bool {
	sck.mu.Lock()
	ver := sck.nextCfgVersion
	sck.mu.Unlock()

	if err := sck.IKVClerk.Put(nextConfigKey, data, ver); err != rpc.OK {
		ctrlerLog("put next config err=%v", err)
		return false
	}
	_, ver, err := sck.IKVClerk.Get(nextConfigKey)
	if err != rpc.OK {
		ctrlerLog("get next config err=%v after put", err)
		return false
	}
	sck.mu.Lock()
	sck.nextCfgVersion = ver
	sck.mu.Unlock()
	return true
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	curr := sck.Query()
	if curr == nil {
		return
	}
	nextCfg, ok := sck.loadNextConfig()
	if !ok {
		return
	}
	if nextCfg != nil && nextCfg.Num > curr.Num {
		ctrlerLog("InitController resuming config #%d from current #%d", nextCfg.Num, curr.Num)
		sck.mu.Lock()
		sck.resumeNum = nextCfg.Num
		sck.mu.Unlock()
		sck.ChangeConfigTo(nextCfg)
		sck.mu.Lock()
		sck.resumeNum = 0
		sck.mu.Unlock()
	}
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	if cfg == nil {
		return
	}

	if err := sck.IKVClerk.Put(configKey, cfg.String(), 0); err != rpc.OK {
		return
	}

	_, ver, err := sck.IKVClerk.Get(configKey)
	if err != rpc.OK {
		return
	}

	sck.mu.Lock()
	sck.cfgVersion = ver
	sck.nextCfgVersion = 0
	sck.mu.Unlock()
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	if new == nil {
		return
	}

	ctrlerLog("ChangeConfigTo target #%d", new.Num)
	old := sck.Query()
	if new.Num <= old.Num {
		ctrlerLog("skip change to #%d (current #%d)", new.Num, old.Num)
		return
	}
	newStr := new.String()

	sck.mu.Lock()
	resumeNum := sck.resumeNum
	sck.mu.Unlock()

	nextCfg, hasNext := sck.loadNextConfig()
	needStore := true
	if hasNext && nextCfg != nil {
		if nextCfg.Num > new.Num {
			ctrlerLog("pending higher config #%d present, skip change to #%d", nextCfg.Num, new.Num)
			return
		}
		if nextCfg.Num == new.Num {
			if resumeNum == new.Num {
				ctrlerLog("resuming pending config #%d", new.Num)
				needStore = false
			} else {
				if nextCfg.String() == newStr {
					ctrlerLog("config #%d already posted by peer, skip", new.Num)
				} else {
					ctrlerLog("config #%d posted with different layout, skip", new.Num)
				}
				return
			}
		}
	}
	if needStore {
		if !sck.storeNextConfig(newStr) {
			ctrlerLog("failed to persist next config #%d (lock lost)", new.Num)
			return
		}
	}

	getClerk := func(cache map[tester.Tgid]*shardgrp.Clerk, gid tester.Tgid, servers []string) *shardgrp.Clerk {
		if ck, ok := cache[gid]; ok {
			return ck
		}
		ck := shardgrp.MakeClerk(sck.clnt, servers)
		ctrlerLog("new shardgrp clerk gid=%d servers=%v", gid, servers)
		cache[gid] = ck
		return ck
	}

	srcClerks := make(map[tester.Tgid]*shardgrp.Clerk)
	dstClerks := make(map[tester.Tgid]*shardgrp.Clerk)

	for shIdx := 0; shIdx < shardcfg.NShards; shIdx++ {
		sh := shardcfg.Tshid(shIdx)
		from := old.Shards[sh]
		to := new.Shards[sh]
		if from == to {
			ctrlerLog("shard %d stays on gid=%d", sh, from)
			continue
		}

		ctrlerLog("moving shard %d from %d to %d (cfg #%d)", sh, from, to, new.Num)

		var state []byte
		freezeDone := false
		if from != 0 {
			if servers, ok := old.Groups[from]; ok && len(servers) > 0 {
				ck := getClerk(srcClerks, from, servers)
				for {
					data, err := ck.FreezeShard(sh, new.Num)
					if err == rpc.OK {
						ctrlerLog("freeze shard %d on %d ok state=%dB", sh, from, len(data))
						state = data
						freezeDone = true
						break
					}
					if err == rpc.ErrWrongGroup {
						ctrlerLog("freeze shard %d on %d rejected wrong group (num=%d)", sh, from, new.Num)
						// Shard already moved away; nothing to copy.
						state = nil
						break
					}
					ctrlerLog("freeze shard %d on %d err=%v retry", sh, from, err)
					time.Sleep(50 * time.Millisecond)
				}
			}
		}

		if from != 0 && !freezeDone {
			ctrlerLog("skip shard %d move since source group %d doesn't own it anymore", sh, from)
			continue
		}

		if to != 0 {
			if servers, ok := new.Groups[to]; ok && len(servers) > 0 {
				ck := getClerk(dstClerks, to, servers)
				for {
					err := ck.InstallShard(sh, state, new.Num)
					if err == rpc.OK {
						ctrlerLog("install shard %d on %d ok", sh, to)
						break
					}
					if err == rpc.ErrWrongGroup {
						ctrlerLog("install shard %d on %d wrong group (num=%d) treat as done", sh, to, new.Num)
						break
					}
					ctrlerLog("install shard %d on %d err=%v retry", sh, to, err)
					time.Sleep(50 * time.Millisecond)
				}
			}
		}

		if from != 0 {
			if servers, ok := old.Groups[from]; ok && len(servers) > 0 {
				ck := getClerk(srcClerks, from, servers)
				for {
					err := ck.DeleteShard(sh, new.Num)
					if err == rpc.OK {
						ctrlerLog("delete shard %d on %d ok", sh, from)
						break
					}
					if err == rpc.ErrWrongGroup {
						ctrlerLog("delete shard %d on %d wrong group (num=%d) treat as done", sh, from, new.Num)
						break
					}
					ctrlerLog("delete shard %d on %d err=%v retry", sh, from, err)
					time.Sleep(50 * time.Millisecond)
				}
			}
		}
	}

	sck.mu.Lock()
	ver := sck.cfgVersion
	sck.mu.Unlock()

	if err := sck.IKVClerk.Put(configKey, newStr, ver); err == rpc.OK {
		ctrlerLog("posted config #%d", new.Num)
		_, ver, err := sck.IKVClerk.Get(configKey)
		if err == rpc.OK {
			sck.mu.Lock()
			sck.cfgVersion = ver
			sck.mu.Unlock()
			ctrlerLog("updated cached version to %d", ver)
		} else {
			ctrlerLog("post-config get err=%v", err)
		}
	} else {
		ctrlerLog("put config #%d err=%v", new.Num, err)
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	for {
		data, ver, err := sck.IKVClerk.Get(configKey)
		if err == rpc.ErrNoKey {
			return shardcfg.MakeShardConfig()
		}
		if err == rpc.OK {
			cfg := shardcfg.FromString(data)
			sck.mu.Lock()
			sck.cfgVersion = ver
			sck.mu.Unlock()
			return cfg
		}
	}
}
