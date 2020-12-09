package raft

import (
	"math/rand"
	"time"

	"../slog"
)

const electionTimeoutBase = 200 // Milliseconds

func (rf *Raft) CollectVotes(requestVoteResultChan chan *RequestVoteReply) {
	votesObtained := 1

	participantsNum := cap(requestVoteResultChan)
	for i := 0; i < participantsNum; i++ {
		requestVoteResult := <-requestVoteResultChan

		if requestVoteResult != nil {
			if requestVoteResult.VoteGranted {
				votesObtained++
				slog.Log(slog.LOG_DEBUG, "Raft[%d] got 1 vote. Currently have [%d] votes.", rf.me, votesObtained)

				if votesObtained > (participantsNum / 2) {
					slog.Log(slog.LOG_INFO, "Raft[%d] has got majority[%d] votes, will become a leader | currentRole is: [%d].",
											 rf.me, votesObtained, rf.currentRole)
					if (rf.currentRole == ROLE_CANDIDATE) {
						rf.mu.Lock()

						rf.BecomeLeader()
						rf.condLeader.Signal()	// kick off AppendEntriesThread

						rf.mu.Unlock()
					}
					return
				}
			}

			if requestVoteResult.Term > rf.currentTerm {
				rf.mu.Lock()

				slog.Log(slog.LOG_INFO, "Raft[%d] has met a peer with higher Term[%d], will return to a follower.", rf.me, requestVoteResult.Term)
				rf.ReInitFollower(requestVoteResult.Term)

				rf.mu.Unlock()
				// give up requesting vote.
				return
			}
		} else {
			slog.Log(slog.LOG_ERR, "Raft[%d] there is an error in Call sendRequestVote.", rf.me)
		}
	}
	slog.Log(slog.LOG_DEBUG, "Raft[%d] obtained [%d] votes and did not become leader, will go back to follower", rf.me, votesObtained)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ReInitFollower(rf.currentTerm)
	return
}

func ElectionThread(rf *Raft) {
	for !rf.killed() {
		time.Sleep(50 * time.Millisecond)
		
		slog.Log(slog.LOG_DEBUG, "Raft[%d] will try to lock its mutex.", rf.me)
		rf.mu.Lock()

		elapse := time.Now().Sub(rf.lastActivity)
		electionTimeout := time.Duration(electionTimeoutBase + rand.Intn(150)) * time.Millisecond
		slog.Log(slog.LOG_DEBUG, "Raft[%d] elapse: [%d] electionTimeout: [%d]", rf.me, elapse.Milliseconds(), electionTimeout.Milliseconds())
		if elapse < electionTimeout {
			rf.mu.Unlock()
			continue
		}

		if (rf.currentRole != ROLE_FOLLOWER) {
			rf.mu.Unlock()
			continue
		}
		rf.BecomeCandidate()

		// Prepare RequestVoteArgs
		requestVoteArgs := &RequestVoteArgs {
			Term: 			rf.currentTerm,
			CandidateId: 	rf.me,
			LastLogIndex: 	GetLastLogIndex(rf),
			LastLogTerm:	GetLastLogTerm(rf),
		}
		
		peersNum := len(rf.peers)

		rf.mu.Unlock()

		requestVoteResultChan := make(chan *RequestVoteReply, peersNum - 1)
		for i := 0; i < peersNum; i++ {
			if rf.me == i {
				continue
			}
			
			go func(peerIndex int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(peerIndex, requestVoteArgs, reply)
				if ok {
					requestVoteResultChan<- reply
				} else {
					requestVoteResultChan<- nil
				}

			}(i)
		}
		
		rf.CollectVotes(requestVoteResultChan)
	}
}