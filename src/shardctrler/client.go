package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	mu     sync.Mutex
	leader int
	uid    int64
	rid    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.

	ck.leader = 0
	ck.uid = nrand()
	// request id (rid) starts from 1 and increments by 1 for each request.
	ck.rid = 1

	return ck
}

func (ck *Clerk) requestId() int64 {
	rid := ck.rid
	ck.rid++
	return rid
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args.UID = ck.uid
	args.RID = ck.requestId()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args.UID = ck.uid
	args.RID = ck.requestId()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args.UID = ck.uid
	args.RID = ck.requestId()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	ck.mu.Lock()
	defer ck.mu.Unlock()
	args.UID = ck.uid
	args.RID = ck.requestId()

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
