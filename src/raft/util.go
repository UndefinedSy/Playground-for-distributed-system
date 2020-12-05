package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func GetLastLogIndex(rf *Raft) int {
	if len(rf.log) == 0 {
		return 0
	} else {
		return rf.log[len(rf.log) - 1].Index
	}
}


func GetLastLogTerm(rf *Raft) int {
	if len(rf.log) == 0 {
		return 0
	} else {
		return rf.log[len(rf.log) - 1].Term
	}
}