package kvraft

import (
	"../slog"
	"time"
)

const RPC_TIMEOUT = 5 // (5s)

type OpGenerator interface {
    GenerateOp() *Op
}

func (args *GetArgs) GenerateGetOp() *Op {
	GeneratedOp := &Op {
		OpType	: 0,
		Key		: args.Key,
		OpID	: nrand(),
	}
	return GeneratedOp
}

func (args *PutAppendArgs) GenerateGetOp() *Op {
	GeneratedOp := &Op {
		Key		: args.Key,
		Value	: args.Value,
		OpID	: nrand(),
	}
	switch args.OpFlag {
	case "Put":
		GeneratedOp.OpType = 1
	case "Append":
		GeneratedOp.OpType = 2
	default:
		slog.Log(slog.LOG_ERR, "unknown OpFlag in PutAppendArgs {%v}", args)
		panic("unknown OpFlag")
	}
	return GeneratedOp
}

func (kv *KVServer) UpdatePendedQueue(index int, leaderId int, GeneratedOp *Op) {
	GeneratedOp.OpIndex = index
	GeneratedOp.OpLeader = leaderId
	// TODO here need a uniqueID generator.
	GeneratedOp.CommitedChan = make(chan bool)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.pended[index] = GeneratedOp
}

// Remove current request from the pended queue whenever the request success or not.
func (kv *KVServer) CleanupPendedRequest(index int, OpLeaderId int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	
	if val, ok := kv.pended[index]; ok {
		if val.OpLeader == OpLeaderId {
			slog.Log(slog.LOG_INFO, "KVServer[%d] will cleanup its pended request (index[%d])",
						kv.me, index)
			delete(kv.pended, index)
		}
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	slog.Log(slog.LOG_INFO, "KVServer[%d] received a Get request: {%v}", kv.me, args)
	defer func() {
		slog.Log(slog.LOG_INFO, "KVServer[%d] finished handle the Get request with reply: {%v}", kv.me, reply)
	}()

	GeneratedOp := args.GenerateGetOp()

	index, _, isLeader := kv.rf.Start(GeneratedOp)

	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.CurrentLeaderId = index
	}
	
	kv.UpdatePendedQueue(index, kv.me, GeneratedOp)

	// Cleanup: Remove current request from the pended queue whenever the request success or not.
	defer kv.CleanupPendedRequest(index, GeneratedOp.OpLeader)

	TimeoutTimer := time.NewTimer(RPC_TIMEOUT * time.Second)

	select {
	case <-TimeoutTimer.C:
		// might the request has been removed or discarded.
		slog.Log(slog.LOG_INFO, "KVServer[%d] Request[%d] Timeout, will return Err to let the client retry",
								kv.me, index)
		reply.Err = ErrAgain
	case <-GeneratedOp.CommitedChan:
		defer func() {
			slog.Log(slog.LOG_INFO, "KVServer[%d] GeneratedOp.CommitedChan returned for Request[%d] reply.Err will be: [%s]",
								kv.me, index, reply.Err)
		}()

		if GeneratedOp.OpLeaderChanged {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK

			kv.mu.Lock()
			defer kv.mu.Unlock()
			reply.Value = kv.kvStore[args.Key]
		}
		// GET related logic has been handled in RPC thread.
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	slog.Log(slog.LOG_INFO, "KVServer[%d] received a PutAppend request: {%v}", kv.me, args)
	defer func() {
		slog.Log(slog.LOG_INFO, "KVServer[%d] finished handle the PutAppend request with reply: {%v}", kv.me, reply)
	}()

	GeneratedOp := args.GenerateGetOp()

	index, _, isLeader := kv.rf.Start(GeneratedOp)

	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.CurrentLeaderId = index
		return
	} 

	kv.UpdatePendedQueue(index, kv.me, GeneratedOp)

	// Cleanup: Remove current request from the pended queue whenever the request success or not.
	defer kv.CleanupPendedRequest(index, GeneratedOp.OpLeader)

	TimeoutTimer := time.NewTimer(RPC_TIMEOUT * time.Second)

	select {
	case <-TimeoutTimer.C:
		// might the request has been removed or discarded.
		slog.Log(slog.LOG_INFO, "KVServer[%d] Request[%d] Timeout, will return Err to let the client retry",
								kv.me, index)
		reply.Err = ErrAgain
	case <-GeneratedOp.CommitedChan:
		defer func() {
			slog.Log(slog.LOG_INFO, "KVServer[%d] GeneratedOp.CommitedChan returned for Request[%d] reply.Err will be: [%s]",
								kv.me, index, reply.Err)
		}()

		if GeneratedOp.OpLeaderChanged {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
		}
		// PutAppend related logic has been handled in RPC thread.
	}

	// then need to put this RPC request into a queue
	// and there will be a REC thread to monitor all PENDED request
	// if a log has been committed, then RPC thread will apply it to the in-memory table and return.
	
}

// TODO: How to handle a re-ordered applyCh 
func (kv *KVServer) HandleCommit(CommittedOp *Op) {
	if CommittedOp.OpType != GET {
		defer func() {
			slog.Log(slog.LOG_DEBUG, "KVServer[%d] HandleCommit Returned with OpType[%d], {Key[%s]: Value[%s]}",
									  kv.me, CommittedOp.OpType, CommittedOp.Key, kv.kvStore[CommittedOp.Key])
		}()
		switch CommittedOp.OpType {
		case PUT:
			kv.kvStore[CommittedOp.Key] = CommittedOp.Value
		case APPEND:
			if OldVal, exist := kv.kvStore[CommittedOp.Key]; exist {
				kv.kvStore[CommittedOp.Key] = OldVal + CommittedOp.Value
			} else {
				kv.kvStore[CommittedOp.Key] = CommittedOp.Value
			}
		default:
			slog.Log(slog.LOG_ERR, "Unknown OpType when HandleCommit.");
		}
	}
}

func (kv *KVServer) NotifyPended(CommandIndex int, CommittedOp *Op) {
	if PendedOp, ok := kv.pended[CommandIndex]; ok {
		// TODO: Compare the current requestID with the committed one
		// 		 Need to UID generator.
		if PendedOp.OpID != CommittedOp.OpID {
			// this means that the leader has been changed and overwritten the request with the same index
			slog.Log(slog.LOG_INFO, "The PendedOp mismatched the Committed one in index[%d]: {%d vs. %d}",
									CommandIndex, PendedOp.OpID, CommittedOp.OpID)
			PendedOp.OpLeaderChanged = true
		}

		PendedOp.CommitedChan <- true
	}
	// Not cared commit, will just do nothing.
}

func RPCThread(kv *KVServer) {
	for !kv.killed() {
		// type ApplyMsg struct {
		// 	CommandValid bool
		// 	Command      interface{}
		// 	CommandIndex int
		// }
		for RaftApplyMsg := range kv.applyCh {
			if RaftApplyMsg.CommandValid == false {
				// ignore other types of ApplyMsg
			} else {
				CommittedOp := RaftApplyMsg.Command
				
				kv.mu.Lock()

				kv.HandleCommit(CommittedOp.(*Op))
				kv.NotifyPended(RaftApplyMsg.CommandIndex, CommittedOp.(*Op))

				kv.mu.Unlock()
			}

		}
	}
}