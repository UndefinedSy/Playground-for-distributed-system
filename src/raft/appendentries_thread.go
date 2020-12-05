package raft

import (
	"time"
)

const HeartBeatTimeout = 50 // Milliseconds

func AppendEntriesProcessor(rf *Raft, peerIndex int) {
	rf.mu.Lock()
	appendEntriesArgs := &AppendEntriesArgs {
		Term: 			rf.currentTerm,
		LeaderId:		rf.me,
		
		prevLogIndex:	rf.nextIndex[peerIndex] - 1
		
		LeaderCommit:	rf.commitIndex,
	}

	if appendEntriesArgs.prevLogIndex == 0 {
		appendEntriesArgs.prevLogTerm = 0
		Entries = rf.log[0:]
	} else {
		appendEntriesArgs.prevLogTerm = rf.log[appendEntriesArgs.prevLogIndex - 1].Term
		Entries = rf.log[(appendEntriesArgs.prevLogIndex - 1):]
	}

	rf.mu.Unlock()

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
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)	// here may need a condition_variable.wait_for
		rf.condLeader.L.Lock()
        for rf.currentRole != Leader {
			rf.condLeader.Wait()
		}
		
		// rf.mu.Lock() // is this still necessary

		if rf.currentRole != Leader { 	// here should be a condition variable
			// rf.mu.Unlock()
			rf.condLeader.L.Unlock()
			continue
		}

		if time.Now().Sub(rf.lastHeartbeat) < (HeartBeatTimeout * time.Millisecond) {
			// rf.mu.Unlock()
			rf.condLeader.L.Unlock()
			continue
		}

		// begin heartbeat

		rf.lastHeartbeat = time.Now()
	
		rf.condLeader.L.Unlock()
		
		for i := 0; i < len(rf.peers); i++ {
			if rf.me == i {
				continue
			}
			go AppendEntriesProcessor(rf, i)
		}
	}
}
