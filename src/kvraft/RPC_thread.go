package kvraft

import (
	"../slog"
)


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	slog.Log(slog.LOG_INFO, "KVServer[%d] received a Get request: {%v}", kv.me, args)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	slog.Log(slog.LOG_INFO, "KVServer[%d] received a PutAppend request: {%v}", kv.me, args)
}

