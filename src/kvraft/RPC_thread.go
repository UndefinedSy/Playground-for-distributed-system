package kvraft

import (
	"../slog"
)

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
	} else {
		reply.Err = OK
		reply.Value = "TEST_GET_OK"
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

	GeneratedOp.OpIndex = index
	GeneratedOp.OpTerm = term	// is this necessary? maybe just need a UID
	GeneratedOp.OpIndex = 0 // TODO here need a uniqueID generator.

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

