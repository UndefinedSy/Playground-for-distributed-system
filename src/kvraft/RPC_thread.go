package kvraft

import (
	"../slog"
)

// TODO: How to handle a re-ordered applyCh 
func (kv *KVServer) HandleCommit(CommittedOp *Op) {
	if CommittedOp.OpType != GET {
		defer func() {
			slog.Log(slog.LOG_INFO, "KVServer[%d] HandleCommit Returned with OpType[%d], {Key[%s]: Value[%s]}",
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