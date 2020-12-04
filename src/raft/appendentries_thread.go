package raft

import (
	"fmt"
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
		rf.condLeader.L.Lock()
        for rf.currentRole != Leader {
			/rf.condLeader.Wait()
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

		appendEntriesArgs := &AppendEntriesArgs {
			Term: 		rf.currentTerm,
			LeaderId:	rf.me,
		}

		rf.lastHeartbeat = time.Now()
	
		rf.condLeader.L.Unlock()
		
		for i := 0; i < len(rf.peers); i++ {
			if rf.me == i {
				continue
			}
			go AppendEntriesHandler(rf, i, appendEntriesArgs)
		}
	}
}
