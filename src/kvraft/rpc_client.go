package kvraft

import (
	"time"

	"../slog"
)

func (ck *Clerk) UpdateCurrentLeaderId(RPCReturnValue int) {
	slog.Log(slog.LOG_INFO, "Clerk will update currentLeaderId[%d] with RPCReturnValue[%d]",
							 ck.currentLeaderId, RPCReturnValue)
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.currentLeaderId = (ck.currentLeaderId + 1) % len(ck.servers)	// Round-Robin to the next server
	
	// Bad news, seems that the instance identifier is not the same as the index of the ck.servers
	// if RPCReturnValue < 0 {
	// 	ck.currentLeaderId = (ck.currentLeaderId + 1) % len(ck.servers)	// Round-Robin to the next server
	// } else {
	// 	ck.currentLeaderId = RPCReturnValue
	// }
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
		slog.Log(slog.LOG_INFO, "Clerk will send RPC Get to KVServer[%d] with args:{%v}",
							 	 ck.currentLeaderId, args)
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
			time.Sleep(10 * time.Millisecond)
		}
		slog.Log(slog.LOG_INFO, "Clerk failed in RPC Get to KVServer[%d] with ok[%t] reply:{%v}",
							 	 ck.currentLeaderId, ok, reply)
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
	args := &PutAppendArgs{
		Key		: key,
		Value	: value,
		OpFlag	: op,
	}

	for {
		reply := &PutAppendReply{}
		slog.Log(slog.LOG_INFO, "Clerk will send RPC Get to KVServer[%d] with args:{%v}",
							 	 ck.currentLeaderId, args)
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
			time.Sleep(10 * time.Millisecond)
		}
		slog.Log(slog.LOG_INFO, "Clerk failed in RPC PutAppend to KVServer[%d] with ok[%t] reply:{%v}",
							 	 ck.currentLeaderId, ok, reply)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}