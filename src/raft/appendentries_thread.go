package raft

import (
	"time"
)

const HeartBeatTimeout = 50 // Milliseconds

func AppendEntriesHandler(rf *Raft, peerIndex int, appendEntriesArgs *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peerIndex, appendEntriesArgs, reply)
	// DPrintf("Raft[%d] - AppendEntriesHandler - to peer[%d] finished:%t\n",
	// 		 rf.me, peerIndex, ok)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.ReInitFollower(reply.Term)
		}

		if reply.Success {
			DPrintf("Raft[%d] - AppendEntriesHandler - to peer[%d] success:%t\n",
					rf.me, peerIndex, reply.Success)
		}
	} else {
		// might need retry or something
	}
}

func AppendEntriesThread(rf *Raft) {
	for {
		time.Sleep(10 * time.Millisecond)	// here may need a condition_variable.wait_for
		
		rf.mu.Lock()

		if rf.currentRole != Leader { 	// here should be a condition variable
			rf.mu.Unlock()
			continue
		}

		if time.Now().Sub(rf.lastHeartbeat) < (HeartBeatTimeout * time.Millisecond) {
			rf.mu.Unlock()
			continue
		}

		// begin heartbeat

		appendEntriesArgs := &AppendEntriesArgs {
			Term: 		rf.currentTerm,
			LeaderId:	rf.me,
		}

		rf.lastHeartbeat = time.Now()
	
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if rf.me == i {
				continue
			}
			go AppendEntriesHandler(rf, i, appendEntriesArgs)
		}
	}
}
