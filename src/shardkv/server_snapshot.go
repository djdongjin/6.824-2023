package shardkv

import (
	"bytes"

	"6.5840/labgob"
	"6.5840/shardctrler"
)

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var shards map[int]*Shard
	var inShards map[int]struct{}
	var outShards map[int]*Shard
	var lastIncludedIndex int
	var config, prevConfig shardctrler.Config
	if d.Decode(&shards) != nil || d.Decode(&inShards) != nil || d.Decode(&outShards) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&config) != nil || d.Decode(&prevConfig) != nil {
		panic("failed to decode snapshot")
	} else {
		kv.shards = shards
		kv.inShards = inShards
		kv.outShards = outShards
		kv.lastIncludedIndex = lastIncludedIndex
		kv.config = config
		kv.prevConfig = prevConfig
	}
}

func (kv *ShardKV) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shards)
	e.Encode(kv.inShards)
	e.Encode(kv.outShards)
	e.Encode(kv.lastIncludedIndex)
	e.Encode(kv.config)
	e.Encode(kv.prevConfig)
	data := w.Bytes()
	DPrintf("[makeSnapshot] %d, making snapshot: %d, snapshot size: %d", kv.me, kv.lastIncludedIndex, len(data))
	return data
}
