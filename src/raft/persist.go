package raft

import (
	"../slog"
	"bytes"
	"../labgob"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).

	bufferToPersist := new(bytes.Buffer)
	bufferEncoder := labgob.NewEncoder(bufferToPersist)
	bufferEncoder.Encode(rf.currentTerm)
	bufferEncoder.Encode(rf.votedFor)
	bufferEncoder.Encode(rf.log[:rf.commitIndex+1])	// here persist the whole log entries. Maybe should only persist operations incrementally
	data := bufferToPersist.Bytes()
	rf.persister.SaveRaftState(data)
	slog.Log(slog.LOG_INFO, "Raft[%d] has persisted its state: {currentTerm[%d], votedFor[%d], log[%+v]}",
							rf.me, rf.currentTerm, rf.votedFor, rf.log)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	bufferRestored := bytes.NewBuffer(data)
	bufferDecoder := labgob.NewDecoder(bufferRestored)

	var currentTerm int
	var votedFor int
	var log []*Entry

	if bufferDecoder.Decode(&currentTerm) != nil ||
		bufferDecoder.Decode(&votedFor) != nil ||
		bufferDecoder.Decode(&log) != nil {
			slog.Log(slog.LOG_ERR, "Raft[%d] readPersist Error", rf.me)
			return
	}

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	
	rf.commitIndex = len(rf.log) - 1
	slog.Log(slog.LOG_INFO, "Raft[%d] has read persistent state: {currentTerm[%d], votedFor[%d], log[%+v]}",
							rf.me, rf.currentTerm, rf.votedFor, rf.log)
}