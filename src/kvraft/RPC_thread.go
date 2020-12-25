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
	}
	return GeneratedOp
}

func (args *PutAppendArgs) GenerateGetOp() *Op {
	GeneratedOp := &Op {
		Key		: args.Key,
		Value	: args.Value,
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
	
	kv.UpdatePendedQueue(index, term, kv.me, GeneratedOp)

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
		slog.Log(slog.LOG_INFO, "KVServer[%d] Request[%d] has been committed",
								kv.me, index)
		reply.Err = OK
		reply.Value = //  TODO
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

	index, term, isLeader := kv.rf.Start(GeneratedOp)

	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.CurrentLeaderId = index
		return
	} 

	kv.UpdatePendedQueue(index, term, kv.me, GeneratedOp)

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
		slog.Log(slog.LOG_INFO, "KVServer[%d] Request[%d] has been committed",
								kv.me, index)
		reply.Err = OK
		// PutAppend related logic has been handled in RPC thread.
	}

	// TODO: This part should be refactored to a general logic: (GET the same logic)
	// kick off request 
	// -> fill Op 
	// -> put Op into PENDED queue 
	// -> wait for notification of committed 
	// -> send back response

	

	// then need to put this RPC request into a queue
	// and there will be a REC thread to monitor all PENDED request
	// if a log has been committed, then RPC thread will apply it to the in-memory table and return.
	
}

func (kv *KVServer) UpdatePendedQueue(index int, term int, leaderId int, GeneratedOp *Op) {
	GeneratedOp.OpIndex = index
	GeneratedOp.OpTerm = term	// is this necessary? maybe just need a UID
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
			slog.Log(LOG_INFO, "KVServer[%d] will cleanup its pended request (index[%d])",
						kv.me, index)
			delete(kv.pended, index)
		}
	}
}