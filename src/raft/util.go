package raft

import (
	"../slog"
)

func GetLastLogIndex(rf *Raft) int {
	return rf.log[len(rf.log) - 1].Index
}


func GetLastLogTerm(rf *Raft) int {
	return rf.log[len(rf.log) - 1].Term
}

func (rf *Raft) NotifyApplyChannel(commandValid bool, command interface{}, commandIndex int) {
	applyMsg := ApplyMsg {
		CommandValid: commandValid,
		Command		: command,
		CommandIndex: commandIndex,
	}
	slog.Log(slog.LOG_DEBUG, "Raft[%d] notify applyMsg{%+v}",
					    	  rf.me, applyMsg)
	rf.applyCh<- applyMsg
}