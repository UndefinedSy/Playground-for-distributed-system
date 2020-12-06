package raft

import "log"

// Debugging
type LogLevel int
const (
	LOG_ERR		LogLevel = 0
	LOG_INFO 	LogLevel = 1
	LOG_DEBUG	LogLevel = 2
	LOG_TRACE	LogLevel = 3
)

const Debug = 0

func DPrintf(logLevel LogLevel, format string, a ...interface{}) (n int, err error) {
	if Debug >= logLevel {
		log.Printf(format, a...)
	}
	return
}

func GetLastLogIndex(rf *Raft) int {
	// if len(rf.log) == 0 {
	// 	return 0
	// } else {
	// 	return rf.log[len(rf.log) - 1].Index
	// }
	return rf.log[len(rf.log) - 1].Index
}


func GetLastLogTerm(rf *Raft) int {
	// if len(rf.log) == 0 {
	// 	return 0
	// } else {
	// 	return rf.log[len(rf.log) - 1].Term
	// }
	return rf.log[len(rf.log) - 1].Term
}

func (rf *Raft) NotifyApplyChannel(commandValid bool, command interface{}, commandIndex int) {
	applyMsg := ApplyMsg {
		CommandValid: commandValid,
		Command		: command,
		CommandIndex: commandIndex,
	}
	DPrintf(LOG_DEBUG, "Raft[%d] notify applyMsg{%+v}",
					   rf.me, applyMsg)
	rf.applyCh<- applyMsg
}