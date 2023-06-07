package shardkv

import (
	"time"

	"6.5840/shardctrler"
)

func (kv *ShardKV) shardFetcher() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}

		kv.mu.Lock()
		DPrintf("[shardFetcher] %d, prevConfigNum: %d, inShards: %v", kv.me, kv.prevConfig.Num, kv.inShards)
		for shardNum := range kv.inShards {
			go kv.sendFetchShard(shardNum, kv.prevConfig)
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) sendFetchShard(shardNum int, prevConfig shardctrler.Config) {
	args := FetchShardArgs{
		ShardNum:  shardNum,
		ConfigNum: prevConfig.Num,
	}

	DPrintf("[sendFetchShard] %d, prevConfigNum: %v, shardNum: %d", kv.me, prevConfig.Num, shardNum)
	servers, ok := prevConfig.Groups[prevConfig.Shards[shardNum]]
	if !ok {
		return
	}

	for si := 0; si < len(servers); si++ {
		var reply FetchShardReply
		ok := kv.make_end(servers[si]).Call("ShardKV.FetchShard", &args, &reply)
		DPrintf("[sendFetchShard] %d, prevConfigNum: %v, shardNum: %d, reply: %+v", kv.me, prevConfig.Num, shardNum, reply)
		if !ok || !reply.Sueecss {
			continue
		}

		kv.mu.Lock()
		if _, ok := kv.inShards[shardNum]; !ok || kv.prevConfig.Num != prevConfig.Num {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
		shard := reply.Shard.Copy()
		kv.rf.Start(*shard)
		break
	}
}

func (kv *ShardKV) FetchShard(args *FetchShardArgs, reply *FetchShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[FetchShard] %d, configNum: %d, outShards: %+v, args: %+v", kv.me, kv.config.Num, kv.outShards, args)
	if shard, ok := kv.outShards[args.ShardNum]; ok {
		DPrintf("[FetchShard] %d, shardConfigNum: %d", kv.me, shard.ConfigNum)
	}

	if shard, ok := kv.outShards[args.ShardNum]; ok && shard.ConfigNum == args.ConfigNum {
		shard := shard.Copy()
		reply.Sueecss = true
		reply.Shard = *shard
		return
	}
}

func (kv *ShardKV) sendCleanShard(shardNum int, prevConfig shardctrler.Config) {
	args := CleanShardArgs{
		ShardNum:  shardNum,
		ConfigNum: prevConfig.Num,
	}

	DPrintf("[sendCleanShard] %d, prevConfigNum: %v, shardNum: %d", kv.me, prevConfig.Num, shardNum)
	servers, ok := prevConfig.Groups[prevConfig.Shards[shardNum]]
	if !ok {
		return
	}

	for si := 0; si < len(servers); si++ {
		var reply CleanShardReply
		ok := kv.make_end(servers[si]).Call("ShardKV.CleanShard", &args, &reply)
		DPrintf("[sendCleanShard] %d, prevConfigNum: %v, shardNum: %d, reply: %+v", kv.me, prevConfig.Num, shardNum, reply)
		if ok && reply.Success {
			break
		}
	}
}

func (kv *ShardKV) CleanShard(args *CleanShardArgs, reply *CleanShardReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[CleanShard] %d, configNum: %d, outShards: %+v, args: %+v", kv.me, kv.config.Num, kv.outShards, args)
	if shard, ok := kv.outShards[args.ShardNum]; ok {
		DPrintf("[FetchShard] %d, shardConfigNum: %d", kv.me, shard.ConfigNum)
	}

	if shard, ok := kv.outShards[args.ShardNum]; ok && shard.ConfigNum == args.ConfigNum {
		reply.Success = true
		kv.rf.Start(CleanShard{Num: args.ShardNum, ConfigNum: args.ConfigNum})
		return
	}
}
