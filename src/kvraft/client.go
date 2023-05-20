package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

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
	// You'll have to add code here.
	ck.leader = 0
	ck.uid = nrand()
	// request id (rid) starts from 1 and increments by 1 for each request.
	ck.rid = 1

	DPrintf("Clerk %d created.", ck.uid)
	return ck
}

func (ck *Clerk) requestId() int64 {
	rid := ck.rid
	ck.rid++
	return rid
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	leader := ck.leader
	args := GetArgs{Key: key, UID: ck.uid, RID: ck.requestId()}

	var reply GetReply
	for {
		DPrintf("[Get.Start] clerk %d -> server %d, request: %+v", ck.uid, leader, args)
		reply = GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			break
		}

		// try next server if `ck.leader` is not yet updated.
		if ck.leader == leader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			leader = ck.leader
		}
		time.Sleep(10 * time.Millisecond)
	}

	DPrintf("[Get.End] clerk %d -> server %d, reply: %+v", ck.uid, leader, reply)
	if reply.Err == ErrNoKey {
		return ""
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	defer ck.mu.Unlock()
	leader := ck.leader
	args := PutAppendArgs{Key: key, Value: value, Op: op, UID: ck.uid, RID: ck.requestId()}

	var reply PutAppendReply
	for {
		DPrintf("[PutAppend.Start] clerk %d -> server %d, request: %+v", ck.uid, leader, args)
		reply = PutAppendReply{}
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			break
		}

		// try next server if `ck.leader` is not yet updated.
		if ck.leader == leader {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			leader = ck.leader
		}
		time.Sleep(10 * time.Millisecond)
	}

	DPrintf("[PutAppend.End] clerk %d -> server %d, reply: %+v", ck.uid, leader, reply)
	if reply.Err != OK {
		panic("PutAppend panic: " + reply.Err)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
