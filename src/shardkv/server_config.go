package shardkv

import "time"

// configPuller periodically pulls a new config next to `kv.config.Num` from `kv.mck`.
// If there is a new config AND the shard kv server has all its shards ready, it
// will send the config to raft for consensus. When a consensus is reached, the
// config will show up in `kv.applier`, which will apply the new config.
func (kv *ShardKV) configPuller() {
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)

		_, isLeader := kv.rf.GetState()
		if !isLeader {
			continue
		}

		kv.mu.Lock()
		lastConfigNum := kv.config.Num
		kv.mu.Unlock()

		config := kv.mck.Query(lastConfigNum + 1)
		if config.Num == lastConfigNum+1 {
			kv.mu.Lock()
			updateConfig := kv.config.Num+1 == config.Num && len(kv.inShards) == 0
			kv.mu.Unlock()
			if updateConfig {
				DPrintf("[configPuller] %d, found new config %d: %v", kv.me, config.Num, config)
				kv.rf.Start(config)
			}
		}
	}
}
