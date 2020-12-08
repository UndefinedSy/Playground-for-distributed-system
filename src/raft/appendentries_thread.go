package raft

import (
	"time"
	"sort"
	"sync"
)

const HeartBeatTimeout = 50 // Milliseconds

func (rf *Raft) LeaderTryUpdateCommitIndex() {
	DPrintf(LOG_DEBUG, "Raft[%d] - LeaderTryUpdateCommitIndex - will try to lock its mutex.", rf.me)
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

func (rf *Raft) GenerateAppendEntriesArgs(targetIndex int, args *AppendEntriesArgs) {
	args.Term 			= rf.currentTerm
	args.LeaderId		= rf.me
	args.PrevLogIndex	= rf.nextIndex[targetIndex] - 1
	args.PrevLogTerm 	= rf.log[args.PrevLogIndex].Term
	args.Entries 		= rf.log[args.PrevLogIndex + 1:]
	
	args.LeaderCommit	= rf.commitIndex
	
	DPrintf(LOG_INFO, "Raft[%d], peerIndex[%d], current PrevLogIndex is:[%d], Entries is:{%+v}",
					   rf.me, targetIndex, args.PrevLogIndex, args.Entries)

}

func AppendEntriesProcessor(rf *Raft, peerIndex int, wg *sync.WaitGroup) {
	defer wg.Done()
	
	DPrintf(LOG_DEBUG, "Raft[%d] - AppendEntriesProcessor - will try to lock its mutex.", rf.me)
	rf.mu.Lock()
	if rf.killed() || rf.currentRole != ROLE_LEADER {
		rf.mu.Unlock()
		return
	}
	// Check if still need to process AppendEntries

	appendEntriesArgs := &AppendEntriesArgs{}
	rf.GenerateAppendEntriesArgs(peerIndex, appendEntriesArgs)

	rf.mu.Unlock()

	reply := &AppendEntriesReply{}

	ok := rf.sendAppendEntries(peerIndex, appendEntriesArgs, reply)
	DPrintf(LOG_DEBUG, "Raft[%d] - AppendEntriesProcessor - sendAppendEntries to [%d] has returned ok: [%t], with reply: {%+v}",
									rf.me, peerIndex, ok, reply)
	if ok {
		if reply.Term > rf.currentTerm {
			DPrintf(LOG_INFO, "Raft[%d] will return to follower because currentTerm[%d] replyRaftIndex[%d], reply.Term[%d]",
						   	   rf.me, rf.currentTerm, peerIndex, reply.Term)
			rf.ReInitFollower(reply.Term)
			return
		}

		if reply.Success {
			rf.nextIndex[peerIndex] = appendEntriesArgs.PrevLogIndex + len(appendEntriesArgs.Entries) + 1
			rf.matchIndex[peerIndex] = rf.nextIndex[peerIndex] - 1
			rf.LeaderTryUpdateCommitIndex()
			DPrintf(LOG_INFO, "Raft[%d] - AppendEntriesHandler - to peer[%d] success:%t, nextIndex to Raft[%d] now is:[%d]\n",
					rf.me, peerIndex, reply.Success, peerIndex, rf.nextIndex[peerIndex])
		} else {
			DPrintf(LOG_DEBUG, "Raft[%d] - AppendEntriesProcessor Success - will try to lock its mutex.", rf.me)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.nextIndex[peerIndex]--
			DPrintf(LOG_INFO, "Raft[%d] - AppendEntriesHandler - to peer[%d] success:%t, rf.nextIndex dec to:[%d]\n",
					rf.me, peerIndex, reply.Success, rf.nextIndex[peerIndex])
			wg.Add(1)
			go AppendEntriesProcessor(rf, peerIndex, wg)
		}
	} else {
		DPrintf(LOG_ERR, "Raft[%d] - AppendEntriesProcessor - there is an error in sendAppendEntries to Raft[%d], will return.\n",
						  rf.me, peerIndex)
		// go AppendEntriesProcessor(rf, peerIndex)
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


		if time.Now().Sub(rf.lastHeartbeat) < (HeartBeatTimeout * time.Millisecond) {
			rf.condLeader.L.Unlock()
			continue
		}

		// begin heartbeat

		rf.lastHeartbeat = time.Now()
	
		rf.condLeader.L.Unlock()
		
		// var AppendEntriesProcessorWG sync.WaitGroup
		for i := 0; i < len(rf.peers); i++ {
			if rf.me == i {
				continue
			}

			// AppendEntriesProcessorWG.Add(1)
			go AppendEntriesProcessor(rf, i) // , &AppendEntriesProcessorWG)
		}
		// AppendEntriesProcessorWG.Wait()
	}
}
