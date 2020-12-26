package kvraft

import (
	"../slog"
	"time"
)

const RPC_TIMEOUT = 5 // (5s)

type OpGenerator interface {
    GenerateOp() *Op
}

func (args *GetArgs) GenerateOp() *Op {
	GeneratedOp := &Op {
		OpType	: 0,
		Key		: args.Key,
		OpID	: nrand(),
	}
	return GeneratedOp
}

func (args *PutAppendArgs) GenerateOp() *Op {
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
	}
	return GeneratedOp
}

func (kv *KVServer) UpdatePendedQueue(index int, leaderId int, GeneratedOp *Op) {
	GeneratedOp.OpIndex = index
	GeneratedOp.OpLeader = leaderId
	// TODO here need a uniqueID generator.
	GeneratedOp.CommitedChan = make(chan bool)

	slog.Log(slog.LOG_INFO, "KVServer[%d] will get lock and UpdatePendedQueue[%d] with {%v}",
							kv.me, index, GeneratedOp)
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

	GeneratedOp := args.GenerateOp()
	slog.Log(slog.LOG_INFO, "KVServer[%d] GenerateGetOp: {%v}", kv.me, GeneratedOp)

	index, _, isLeader := kv.rf.Start(GeneratedOp)

	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.CurrentLeaderId = index
	}
	
	kv.UpdatePendedQueue(index, kv.me, GeneratedOp)

	// Cleanup: Remove current request from the pended queue whenever the request success or not.
	defer kv.CleanupPendedRequest(index, GeneratedOp.OpLeader)

	TimeoutTimer := time.NewTimer(RPC_TIMEOUT * time.Second)

	slog.Log(slog.LOG_INFO, "KVServer[%d] will be blocked in Get and wake up for commited or timeout", kv.me)
	select {
	case <-TimeoutTimer.C:
		// might the request has been removed or discarded.
		slog.Log(slog.LOG_INFO, "KVServer[%d] Request[%d] Timeout, will return Err to let the client retry",
								kv.me, index)
		reply.Err = ErrWrongLeader
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

	GeneratedOp := args.GenerateOp()
	slog.Log(slog.LOG_INFO, "KVServer[%d] GeneratePutAppendOp: {%v}", kv.me, GeneratedOp)

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

	slog.Log(slog.LOG_INFO, "KVServer[%d] will be blocked in PutAppend and wake up for commited or timeout", kv.me)
	select {
	case <-TimeoutTimer.C:
		// might the request has been removed or discarded.
		slog.Log(slog.LOG_INFO, "KVServer[%d] Request[%d] Timeout, will return Err to let the client retry",
								kv.me, index)
		reply.Err = ErrWrongLeader
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