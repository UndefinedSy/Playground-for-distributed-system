package raft

import (
	"time"
	"sort"
)

const HeartBeatTimeout = 50 // Milliseconds

func (rf *Raft) LeaderTryUpdateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	matchIndexCopy := make([]int, len(rf.peers))
	copy(matchIndexCopy, rf.matchIndex)
	matchIndexCopy[rf.me] = GetLastLogIndex(rf)

	sort.Ints(matchIndexCopy)
	matchIndexWithQuorum := matchIndexCopy[len(rf.peers) / 2]
	DPrintf(LOG_INFO, "Raft[%d] - LeaderTryUpdateCommitIndex - matchIndexWithQuorum is: [%d], commitIndex is:[%d], logEntry in N is:{%+v} currentTerm is:[%d]",
					   rf.me, matchIndexWithQuorum, rf.commitIndex, rf.log[matchIndexWithQuorum], rf.currentTerm)
	if matchIndexWithQuorum > rf.commitIndex && rf.log[matchIndexWithQuorum].Term == rf.currentTerm {
		for i := rf.commitIndex + 1; i <= matchIndexWithQuorum; i++ {
			rf.NotifyApplyChannel(true, rf.log[i].Command, i)
		}
		rf.commitIndex = matchIndexWithQuorum
		DPrintf(LOG_INFO, "Raft[%d] - LeaderTryUpdateCommitIndex - commitIndex has been updated to:[%d]",
						   rf.me, rf.commitIndex)
	}
}

func AppendEntriesProcessor(rf *Raft, peerIndex int) {
	rf.mu.Lock()
	if rf.killed() || rf.currentRole != ROLE_LEADER {
		rf.mu.Unlock()
		return
	}
	appendEntriesArgs := &AppendEntriesArgs {
		Term: 			rf.currentTerm,
		LeaderId:		rf.me,
		
		PrevLogIndex:	rf.nextIndex[peerIndex] - 1,
		
		LeaderCommit:	rf.commitIndex,
	}

	// if appendEntriesArgs.PrevLogIndex == 0 {
	// 	appendEntriesArgs.PrevLogTerm = 0
	// 	appendEntriesArgs.Entries = rf.log[0:]
	// } else {
	// 	appendEntriesArgs.PrevLogTerm = rf.log[appendEntriesArgs.PrevLogIndex - 1].Term
	// 	appendEntriesArgs.Entries = rf.log[(appendEntriesArgs.PrevLogIndex - 1):]
	// }
	appendEntriesArgs.PrevLogTerm = rf.log[appendEntriesArgs.PrevLogIndex].Term
	appendEntriesArgs.Entries = rf.log[appendEntriesArgs.PrevLogIndex + 1:]
	DPrintf(LOG_INFO, "Raft[%d], peerIndex[%d], current PrevLogIndex is:[%d], current log is:{%+v}, Entries is:{%+v}",
					   rf.me, peerIndex, appendEntriesArgs.PrevLogIndex, rf.log, appendEntriesArgs.Entries)


	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peerIndex, appendEntriesArgs, reply)
	if ok {
		DPrintf(LOG_DEBUG, "Raft[%d] currentTerm[%d] reply.Term[%d] reply success[%t]",
						   rf.me, rf.currentTerm, reply.Term, reply.Success)
		if reply.Term > rf.currentTerm {
			rf.ReInitFollower(reply.Term)
			return
		}

		if reply.Success {
			rf.nextIndex[peerIndex] = appendEntriesArgs.PrevLogIndex + len(appendEntriesArgs.Entries) + 1
			rf.matchIndex[peerIndex] = rf.nextIndex[peerIndex] - 1
			rf.LeaderTryUpdateCommitIndex()
			DPrintf(LOG_INFO, "Raft[%d] - AppendEntriesHandler - to peer[%d] success:%t, nextIndex[%d] now is:[%d]\n",
					rf.me, peerIndex, reply.Success, peerIndex, rf.nextIndex[peerIndex])
		} else {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.nextIndex[peerIndex]--
			go AppendEntriesProcessor(rf, peerIndex)
		}
	} else {
		go AppendEntriesProcessor(rf, peerIndex)
		// might need retry or something
	}
}

func AppendEntriesThread(rf *Raft) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)	// here may need a condition_variable.wait_for
		rf.condLeader.L.Lock()
        for rf.currentRole != ROLE_LEADER {
			rf.condLeader.Wait()
		}
		
		// rf.mu.Lock() // is this still necessary

		// if rf.currentRole != ROLE_LEADER { 	// here should be a condition variable
		// 	// rf.mu.Unlock()
		// 	rf.condLeader.L.Unlock()
		// 	continue
		// }

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
