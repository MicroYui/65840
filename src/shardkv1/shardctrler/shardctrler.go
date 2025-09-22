package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"

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
const ConfigKeyNext = "config_next"

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
	DPrintf("[Ctrler] InitController: Checking for incomplete configuration change...")
	valNext, verNext, errNext := sck.IKVClerk.Get(ConfigKeyNext)

	if errNext == rpc.ErrNoKey || valNext == "" {
		DPrintf("[Ctrler] InitController: No 'next' config found. Nothing to recover.")
		return
	}

	valCurrent, verCurrent, _ := sck.IKVClerk.Get(ConfigKey)
	current := shardcfg.FromString(valCurrent)
	next := shardcfg.FromString(valNext)

	// 只有当 "next" 配置确实比 "current" 配置新的时候，才执行恢复逻辑。
	if next.Num > current.Num {
		DPrintf("[Ctrler] InitController: Found incomplete change from Num %d to Num %d. Resuming...", current.Num, next.Num)
		success := sck.performFreezeAndInstall(current, next)

		if success {
			DPrintf("[Ctrler] InitController: Recovery moves complete. Finalizing config update.")
			updateErr := sck.IKVClerk.Put(ConfigKey, next.String(), verCurrent)
			if updateErr == rpc.OK {
				_, finalVerNext, _ := sck.IKVClerk.Get(ConfigKeyNext)
				sck.IKVClerk.Put(ConfigKeyNext, "", finalVerNext)
				DPrintf("[Ctrler] InitController: Recovery successful. Config is now Num %d.", next.Num)
				go sck.performDeletes(current, next)
			} else {
				DPrintf("[Ctrler] InitController: FATAL! Failed to finalize recovered config update. Error: %s", updateErr)
			}
		} else {
			DPrintf("[Ctrler] InitController: Recovery moves failed. Aborting finalization. Will retry on next controller start.")
		}
	} else {
		// 如果 next.Num <= current.Num，说明之前的变更已经完成，只是清理工作没做完。
		// 我们只需要把过时的 "next" 配置删除即可。
		DPrintf("[Ctrler] InitController: 'next' config (Num %d) is not newer than 'current' (Num %d). Cleaning up stale 'next' config.", next.Num, current.Num)
		sck.IKVClerk.Put(ConfigKeyNext, "", verNext)
	}
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

func (sck *ShardCtrler) performMoves(current *shardcfg.ShardConfig, new *shardcfg.ShardConfig) bool {
	// 获取需要移动的分片
	shardsToMove := make(map[shardcfg.Tshid]struct {
		From tester.Tgid
		To   tester.Tgid
	})
	for i, gid := range current.Shards {
		// 注意 Tgid 是从 1 开始算的，0 代表未分配给任何一个 group
		if gid != new.Shards[i] {
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

		// 源分组的服务器列表应该从 current 配置中获取
		srcServers := current.Groups[move.From]
		// 目标分组的服务器列表从 new 配置中获取
		destServers, dest_ok := new.Groups[move.To]

		if !dest_ok && move.To != 0 {
			log.Fatalf("Cannot find servers for destination GID %d", move.To)
		}

		// 为源和目标组创建内层客户端
		srcClerk := shardgrp.MakeClerk(sck.clnt, srcServers)
		destClerk := shardgrp.MakeClerk(sck.clnt, destServers)

		if move.To != 0 {
			var state []byte
			DPrintf("[Ctrler] Step 1: Freezing Shard %d at GID %d", shard, move.From)
			// 这里不需要 for，因为实际执行时还会一直循环访问
			state, freezeErr := srcClerk.FreezeShard(shard, new.Num)
			if freezeErr != rpc.OK {
				DPrintf("[Ctrler] FreezeShard for %d failed with err: %s. Aborting config change.", shard, freezeErr)
				return false
			}
			DPrintf("[Ctrler] -> Freeze success for Shard %d", shard)

			// 在目标组安装分片数据
			DPrintf("[Ctrler] Step 2: Installing Shard %d at GID %d", shard, move.To)
			installErr := destClerk.InstallShard(shard, state, new.Num)
			if installErr != rpc.OK {
				DPrintf("[Ctrler] InstallShard for %d failed with err: %s. Aborting config change.", shard, installErr)
				return false
			}
			DPrintf("[Ctrler] -> Install success for Shard %d", shard)

			// 从源组删除分片数据
			DPrintf("[Ctrler] Step 3: Deleting Shard %d from GID %d", shard, move.From)
			deleteErr := srcClerk.DeleteShard(shard, new.Num)
			if deleteErr != rpc.OK {
				DPrintf("[Ctrler] DeleteShard for %d failed with err: %s. Aborting config change.", shard, deleteErr)
				return false
			}
			DPrintf("[Ctrler] -> Delete success for Shard %d", shard)
		}

	}
	return true
}

// performFreezeAndInstall 只执行数据复制
func (sck *ShardCtrler) performFreezeAndInstall(current *shardcfg.ShardConfig, new *shardcfg.ShardConfig) bool {
	shardsToMove := make(map[shardcfg.Tshid]struct {
		From tester.Tgid
		To   tester.Tgid
	})
	for i, gid := range current.Shards {
		if gid != new.Shards[i] && gid != 0 { // 只处理有源组的移动
			shardsToMove[shardcfg.Tshid(i)] = struct {
				From tester.Tgid
				To   tester.Tgid
			}{gid, new.Shards[i]}
		}
	}

	for shard, move := range shardsToMove {
		// ... (和原来一样的逻辑来创建 srcClerk 和 destClerk)
		srcServers := current.Groups[move.From]
		destServers, dest_ok := new.Groups[move.To]
		if !dest_ok && move.To != 0 {
			log.Fatalf("Cannot find servers for destination GID %d", move.To)
		}
		srcClerk := shardgrp.MakeClerk(sck.clnt, srcServers)
		destClerk := shardgrp.MakeClerk(sck.clnt, destServers)

		if move.To != 0 {
			state, freezeErr := srcClerk.FreezeShard(shard, new.Num)
			if freezeErr != rpc.OK {
				DPrintf("[Ctrler] FreezeShard for %d failed. Aborting.", shard)
				return false
			}

			installErr := destClerk.InstallShard(shard, state, new.Num)
			if installErr != rpc.OK {
				DPrintf("[Ctrler] InstallShard for %d failed. Aborting.", shard)
				return false
			}
		}
	}
	return true
}

// performDeletes 只执行删除操作，作为清理
func (sck *ShardCtrler) performDeletes(current *shardcfg.ShardConfig, new *shardcfg.ShardConfig) {
	// ... (和上面类似逻辑计算 shardsToMove) ...
	shardsToMove := make(map[shardcfg.Tshid]struct {
		From tester.Tgid
		To   tester.Tgid
	})
	for i, gid := range current.Shards {
		if gid != new.Shards[i] && gid != 0 {
			shardsToMove[shardcfg.Tshid(i)] = struct {
				From tester.Tgid
				To   tester.Tgid
			}{gid, new.Shards[i]}
		}
	}

	for shard, move := range shardsToMove {
		srcServers := current.Groups[move.From]
		srcClerk := shardgrp.MakeClerk(sck.clnt, srcServers)

		// 删除操作是尽力而为，即使失败也不应阻塞
		srcClerk.DeleteShard(shard, new.Num)
	}
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

	_, verNext, _ := sck.IKVClerk.Get(ConfigKeyNext)
	sck.IKVClerk.Put(ConfigKeyNext, new.String(), verNext)
	DPrintf("[Ctrler] Wrote 'next' config (Num %d) to KV store.", new.Num)

	success := sck.performFreezeAndInstall(current, new)

	if success {
		DPrintf("[Ctrler] Freeze/Install complete. Updating config to Num %d", new.Num)
		// 阶段二: 提交
		updateErr := sck.IKVClerk.Put(ConfigKey, new.String(), ver)
		if updateErr == rpc.OK {
			_, finalVerNext, _ := sck.IKVClerk.Get(ConfigKeyNext)
			sck.IKVClerk.Put(ConfigKeyNext, "", finalVerNext)
			DPrintf("[Ctrler] Config updated successfully.")

			// 阶段三: 清理
			go sck.performDeletes(current, new) // 可以异步执行清理
			return
		}
	} else {
		DPrintf("[Ctrler] Moves failed. Aborting finalization. Will retry on next controller start.")
	}
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
