package kvraft

import (
	"sync"
	"crypto/rand"
	"math/big"

	"../labrpc"
	"../slog"
)


type Clerk struct {
	servers 		[]*labrpc.ClientEnd
	// You will have to modify this struct.
	mu 				sync.Mutex
	currentLeaderId int
	ClerkId			int64
	// You will have to modify this struct.
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
	ck.ClerkId = nrand()
	// You'll have to add code here.

	return ck
}

func (ck *Clerk) UpdateCurrentLeaderId(RPCReturnValue int) {
	slog.Log(slog.LOG_INFO, "Clerk[%d] will update currentLeaderId[%d] with RPCReturnValue[%d]",
							  ck.ClerkId, ck.currentLeaderId, RPCReturnValue)
	ck.mu.Lock()
	defer ck.mu.Unlock()

	if RPCReturnValue < 0 {
		ck.currentLeaderId = (ck.currentLeaderId + 1) % len(ck.servers)	// Round-Robin to the next server
	} else {
		ck.currentLeaderId = RPCReturnValue
	}
}

//
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
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{}
	args.Key = key

	for {
		reply := &GetReply{}
		ok := ck.servers[ck.currentLeaderId].Call("KVServer.Get", args, reply)
		if !ok {	// cannot reach the kvserver
			ck.UpdateCurrentLeaderId(-1)	// Round-Robin to the next server
		} else {
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			case ErrWrongLeader:
				ck.UpdateCurrentLeaderId(reply.CurrentLeaderId)
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{}
	args.Key 	= key
	args.Value	= value
	args.Op		= op

	for {
		reply := &PutAppendReply{}
		ok := ck.servers[ck.currentLeaderId].Call("KVServer.PutAppend", args, reply)
		if !ok {	// cannot reach the kvserver
			ck.UpdateCurrentLeaderId(-1)	// Round-Robin to the next server
		} else {
			switch reply.Err {
			case OK:
				return
			case ErrWrongLeader:
				ck.UpdateCurrentLeaderId(reply.CurrentLeaderId)
			}
		}
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
