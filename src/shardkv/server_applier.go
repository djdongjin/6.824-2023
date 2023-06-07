package shardkv

import (
	"6.5840/raft"
	"6.5840/shardctrler"
)

func (kv *ShardKV) applier() {
	for !kv.killed() {
		select {
		case <-kv.killedCh:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.applyCommand(&msg)
			} else if msg.SnapshotValid {
				kv.applySnapshot(&msg)
			} else {
				panic("unknown message type")
			}
		}
	}
}

func (kv *ShardKV) applyCommand(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch op := msg.Command.(type) {
	case Op:
		kv.applyOp(op, msg.CommandIndex)
	case shardctrler.Config:
		kv.applyConfig(op)
	case Shard:
		kv.applyShard(op)
	case CleanShard:
		kv.applyCleanShard(op)
	}

	if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		kv.lastIncludedIndex = msg.CommandIndex
		kv.rf.Snapshot(msg.CommandIndex, kv.makeSnapshot())
	}
}

func (kv *ShardKV) applyOp(op Op, index int) {
	shardNum := key2shard(op.Key)
	if kv.config.Shards[shardNum] != kv.gid {
		return
	}

	if _, ok := kv.shards[shardNum]; !ok {
		kv.shards[shardNum] = NewShard(shardNum, kv.config.Num)
	}
	shard := kv.shards[shardNum]

	DPrintf("[applyOp] %d, shard: %d, applying index: %d, UID: %d, RID: %d", kv.me, shardNum, index, op.UID, op.RID)
	// duplicate `Op`, return directly.
	switch {
	case op.RID < shard.UID2RID[op.UID]:
		DPrintf("[PANIC][applyOp] %d, found too small RID from %d. expected: %d, actual: %d", kv.me, op.UID, shard.UID2RID[op.UID]+1, op.RID)
		return
	case op.RID == shard.UID2RID[op.UID]:
		DPrintf("[applyOp] %d, found duplicate RID from %d. expected: %d, actual: %d", kv.me, op.UID, shard.UID2RID[op.UID]+1, op.RID)
		return
	}

	// update kv server data
	kv.index2uid[index] = op.UID

	shard.UID2RID[op.UID] = op.RID
	switch op.Type {
	case OpGet:
		if _, ok := shard.Data[op.Key]; !ok {
			shard.UID2Val[op.UID] = ""
		} else {
			shard.UID2Val[op.UID] = shard.Data[op.Key]
		}
	case OpPut:
		shard.Data[op.Key] = op.Value
	case OpAppend:
		if _, ok := shard.Data[op.Key]; !ok {
			shard.Data[op.Key] = ""
		}
		shard.Data[op.Key] += op.Value
	}
}

func (kv *ShardKV) applyConfig(config shardctrler.Config) {
	// apply a new config only if:
	// 1. the config is the next config
	// 2. the kv server is not waiting for any shards from the current config
	if config.Num != kv.config.Num+1 || len(kv.inShards) != 0 {
		return
	}

	// if the new config is the first config, apply it directly
	if config.Num == 1 {
		kv.config = config
		return
	}

	DPrintf("[applyConfig] %d, applying config: %d", kv.me, config.Num)

	// otherwise, save the current config and apply the new config
	// and update the `inShards` and `outShards` map
	kv.prevConfig = kv.config
	kv.config = config
	for shardNum := 0; shardNum < shardctrler.NShards; shardNum++ {
		oldGid, newGid := kv.prevConfig.Shards[shardNum], kv.config.Shards[shardNum]
		switch {
		case oldGid == kv.gid && newGid != kv.gid:
			// outgoing shard
			if _, ok := kv.shards[shardNum]; !ok {
				kv.shards[shardNum] = NewShard(shardNum, kv.prevConfig.Num)
			}
			kv.outShards[shardNum] = kv.shards[shardNum].Copy()
			delete(kv.shards, shardNum)
		case oldGid != kv.gid && newGid == kv.gid:
			// incoming shard
			kv.inShards[shardNum] = struct{}{}
		case oldGid == kv.gid && newGid == kv.gid:
			// keep shard
			if _, ok := kv.shards[shardNum]; !ok {
				kv.shards[shardNum] = NewShard(shardNum, kv.prevConfig.Num)
			}
			kv.shards[shardNum].ConfigNum++
		}
	}

	DPrintf("[applyConfig] %d, configNum: %d, inShards: %v, outShards: %v", kv.me, kv.config.Num, kv.inShards, kv.outShards)
}

func (kv *ShardKV) applyShard(shard Shard) {
	// apply a shard only if:
	// 1. the serer is still waiting for this shard
	// 2. the shard is from the previous config
	if _, ok := kv.inShards[shard.Num]; !ok || shard.ConfigNum != kv.prevConfig.Num {
		return
	}
	DPrintf("[applyShard] %d, configNum: %d, shardConfigNum: %d, shardNum: %d", kv.me, kv.config.Num, shard.ConfigNum, shard.Num)

	// when applying a shard, we copy it and increment its config number by 1
	pShard := shard.Copy()
	pShard.ConfigNum++
	kv.shards[pShard.Num] = pShard
	delete(kv.inShards, pShard.Num)

	if _, isLeader := kv.rf.GetState(); isLeader {
		go kv.sendCleanShard(shard.Num, kv.prevConfig)
	}
}

func (kv *ShardKV) applyCleanShard(cleanShard CleanShard) {
	if shard, ok := kv.outShards[cleanShard.Num]; !ok || cleanShard.ConfigNum != shard.ConfigNum {
		return
	}
	delete(kv.outShards, cleanShard.Num)
}

func (kv *ShardKV) applySnapshot(msg *raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[applySnapshot] %d, applying snapshot: %d", kv.me, msg.SnapshotIndex)

	if kv.lastIncludedIndex >= msg.SnapshotIndex {
		return
	}

	kv.readSnapshot(msg.Snapshot)
}
