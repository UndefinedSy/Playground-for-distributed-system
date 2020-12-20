package raft

import (
	"time"
	"sort"

	"../slog"
)

const HeartBeatTimeout = 50 // Milliseconds

func (rf *Raft) LeaderTryUpdateCommitIndex() {
	slog.Log(slog.LOG_DEBUG, "Raft[%d] will try to lock its mutex.", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// deep copy the matchIndex array from the leader
	matchIndexCopy := make([]int, len(rf.peers))
	copy(matchIndexCopy, rf.matchIndex)
	matchIndexCopy[rf.me] = GetLastLogIndex(rf)

	// Get the marjority matchedIndex
	sort.Ints(matchIndexCopy)
	matchIndexWithQuorum := matchIndexCopy[len(rf.peers) / 2]
	slog.Log(slog.LOG_DEBUG, "Raft[%d] matchIndexWithQuorum is: [%d], currentCommitIndex is:[%d], logEntry in majority is:{%+v} currentTerm is:[%d]",
							  rf.me, matchIndexWithQuorum, rf.commitIndex, rf.log[matchIndexWithQuorum], rf.currentTerm)

	if matchIndexWithQuorum > rf.commitIndex && rf.log[matchIndexWithQuorum].Term == rf.currentTerm {
		i := rf.commitIndex + 1
		rf.commitIndex = matchIndexWithQuorum

		rf.persist()

		for ; i <= matchIndexWithQuorum; i++ {
			rf.NotifyApplyChannel(true, rf.log[i].Command, i)
		}

		slog.Log(slog.LOG_DEBUG, "Raft[%d] commitIndex has been updated to:[%d]",
						   		  rf.me, rf.commitIndex)
	}
}

func (rf *Raft) GenerateAppendEntriesArgs(peerIndex int, args *AppendEntriesArgs) {
	args.Term 			= rf.currentTerm
	args.LeaderId		= rf.me
	args.PrevLogIndex	= rf.nextIndex[peerIndex] - 1
	args.PrevLogTerm 	= rf.log[args.PrevLogIndex].Term
	args.Entries 		= rf.log[args.PrevLogIndex + 1:]
	
	args.LeaderCommit	= rf.commitIndex
	
	slog.Log(slog.LOG_DEBUG, "Raft[%d], peerIndex[%d], PrevLogIndex:[%d], PrevLogTerm:[%d] Entries:{%+v}",
					   		  rf.me, peerIndex, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
}

func AppendEntriesProcessor(rf *Raft, peerIndex int) {
	slog.Log(slog.LOG_DEBUG, "Raft[%d] will try to lock its mutex.", rf.me)
	rf.mu.Lock()

	// Check if still need to process AppendEntries
	if rf.killed() || rf.currentRole != ROLE_LEADER {
		rf.mu.Unlock()
		return
	}

	appendEntriesArgs := &AppendEntriesArgs{}
	rf.GenerateAppendEntriesArgs(peerIndex, appendEntriesArgs)

	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peerIndex, appendEntriesArgs, reply)
	if ok {
		slog.Log(slog.LOG_INFO, "Raft[%d] sendAppendEntries to Raft[%d]: reply is {%v}.",
								 rf.me, peerIndex, reply)
		rf.mu.Lock()

		if reply.Term > rf.currentTerm {
			slog.Log(slog.LOG_INFO, `Raft[%d] will return to follower because currentTerm[%d] | reply from Raft[%d] reply.Term[%d]`,
						   	   		 rf.me, rf.currentTerm, peerIndex, reply.Term)
			rf.ReInitFollower(reply.Term)
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			rf.nextIndex[peerIndex] = appendEntriesArgs.PrevLogIndex + len(appendEntriesArgs.Entries) + 1
			rf.matchIndex[peerIndex] = rf.nextIndex[peerIndex] - 1
			rf.mu.Unlock()

			rf.LeaderTryUpdateCommitIndex()
			slog.Log(slog.LOG_DEBUG, "Raft[%d] to peer[%d] success:%t, nextIndex to Raft[%d] now is:[%d]",
									  rf.me, peerIndex, reply.Success, peerIndex, rf.nextIndex[peerIndex])

		} else {
			if reply.ConflictIndex == -1 { 
				// Packet delay because of the unreliable network
				// And we will only care about the conflict situation.
				slog.Log(slog.LOG_DEBUG, "Raft[%d] received a reply from Raft[%d]: {%v}. Might caused by unreliable network.",
									  	  rf.me, peerIndex, reply)
				rf.mu.Unlock()
				return
			}
			// Met conlict in AppendEntries
			rf.nextIndex[peerIndex] = reply.ConflictIndex
			slog.Log(slog.LOG_INFO, "Raft[%d] to peer[%d] success:%t, rf.nextIndex dec to:[%d]",
									 rf.me, peerIndex, reply.Success, rf.nextIndex[peerIndex])
			rf.mu.Unlock()
			go AppendEntriesProcessor(rf, peerIndex)
		}
	} else {
		slog.Log(slog.LOG_ERR, "Raft[%d] there is an error in sendAppendEntries to Raft[%d], will return.",
						  		rf.me, peerIndex)
		// go AppendEntriesProcessor(rf, peerIndex)
		// might need retry or something
	}
}

func AppendEntriesThread(rf *Raft) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.condLeader.L.Lock()
        for rf.currentRole != ROLE_LEADER {
			rf.condLeader.Wait()
		}


		if time.Now().Sub(rf.lastHeartbeat) < (HeartBeatTimeout * time.Millisecond) {
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
