package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
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

const ConfigKey = "config"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	configString := cfg.String()
	sck.IKVClerk.Put(ConfigKey, configString, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	DPrintf("[Ctrler] Starting ChangeConfigTo Num %d", new.Num)
	// 获取当前配置
	val, ver, _ := sck.IKVClerk.Get(ConfigKey)
	current := shardcfg.FromString(val)
	DPrintf("[Ctrler] Current config is Num %d", current.Num)
	// 检查 num
	if current.Num >= new.Num {
		DPrintf("[Ctrler] New config Num %d is not newer than current %d. Aborting.", new.Num, current.Num)
		return
	}
	// 获取需要移动的分片
	shardsToMove := make(map[shardcfg.Tshid]struct {
		From tester.Tgid
		To   tester.Tgid
	})
	for i, gid := range current.Shards {
		// 注意 Tgid 是从 1 开始算的，0 代表未分配给任何一个 group
		if gid != 0 && gid != new.Shards[i] {
			shardsToMove[shardcfg.Tshid(i)] = struct {
				From tester.Tgid
				To   tester.Tgid
			}{gid, new.Shards[i]}
		}
	}
	DPrintf("[Ctrler] For config %d, need to move %d shards: %v", new.Num, len(shardsToMove), shardsToMove)
	// 执行分片移动
	for shard, move := range shardsToMove {
		DPrintf("Moving shard %d from GID %d to GID %d for config %d", shard, move.From, move.To, new.Num)

		// 获取源和目标组的服务器列表
		srcServers, src_ok := new.Groups[move.From]
		destServers, dest_ok := new.Groups[move.To]

		if !src_ok || !dest_ok {
			log.Fatalf("Cannot find servers for GID %d or %d", move.From, move.To)
		}

		// 为源和目标组创建内层客户端
		srcClerk := shardgrp.MakeClerk(sck.clnt, srcServers)
		destClerk := shardgrp.MakeClerk(sck.clnt, destServers)

		var state []byte
		DPrintf("[Ctrler] Step 1: Freezing Shard %d at GID %d", shard, move.From)
		// 冻结源分片，并获取其数据
		for {
			var freezeErr rpc.Err
			state, freezeErr = srcClerk.FreezeShard(shard, new.Num)
			if freezeErr == rpc.OK {
				DPrintf("[Ctrler] -> Freeze success for Shard %d", shard)
				break // 冻结成功
			}
			DPrintf("[Ctrler] -> Freeze for Shard %d failed with %s, retrying...", shard, freezeErr)
			time.Sleep(100 * time.Millisecond)
		}

		// 在目标组安装分片数据
		DPrintf("[Ctrler] Step 2: Installing Shard %d at GID %d", shard, move.To)
		for {
			installErr := destClerk.InstallShard(shard, state, new.Num)
			if installErr == rpc.OK {
				DPrintf("[Ctrler] -> Install success for Shard %d", shard)
				break // 安装成功
			}
			DPrintf("[Ctrler] -> Install for Shard %d failed with %s, retrying...", shard, installErr)
			time.Sleep(100 * time.Millisecond)
		}

		// 从源组删除分片数据
		DPrintf("[Ctrler] Step 3: Deleting Shard %d from GID %d", shard, move.From)
		for {
			deleteErr := srcClerk.DeleteShard(shard, new.Num)
			if deleteErr == rpc.OK {
				DPrintf("[Ctrler] -> Delete success for Shard %d", shard)
				break // 删除成功
			}
			DPrintf("[Ctrler] -> Delete for Shard %d failed with %s, retrying...", shard, deleteErr)
			time.Sleep(100 * time.Millisecond)
		}
	}
	DPrintf("[Ctrler] All shards moved. Updating config in KV store to Num %d", new.Num)
	updateErr := sck.IKVClerk.Put(ConfigKey, new.String(), ver)
	if updateErr == rpc.OK {
		DPrintf("[Ctrler] Config updated successfully.")
		return // 更新完毕
	}
	DPrintf("[Ctrler] Failed to update config! Error: %s", updateErr)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	val, _, _ := sck.IKVClerk.Get(ConfigKey)
	if val == "" {
		return nil
	}
	config := shardcfg.FromString(val)
	return config
}
