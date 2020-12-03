package raft

import (
	"math/rand"
	"time"
)

const electionTimeoutBase = 100 // Milliseconds

func (rf *Raft) CollectVotes(requestVoteResultChan chan *RequestVoteReply) {
	var me, participantsNum int
	{
		rf.mu.Lock()
		// defer rf.mu.Unlock()
		me = rf.me
		participantsNum = len(rf.peers)
		rf.mu.Unlock()
	}

	votesObtained := 1

	for i := 0; i < participantsNum - 1; i++ {
		requestVoteResult := <-requestVoteResultChan
		if requestVoteResult != nil {
			if requestVoteResult.VoteGranted {
				votesObtained++
				DPrintf("Raft[%d] - CollectVotes - has got 1 vote. Currently have [%d] votes.\n", me, votesObtained)
				if votesObtained > (participantsNum / 2) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					DPrintf("Raft[%d] - CollectVotes - has got majority[%d] votes, will become a leader | currentRole is: [%d].\n",
							me, votesObtained, rf.currentRole)
					if (rf.currentRole == Candidate) {
						rf.BecomeLeader()
						rf.lastHeartbeat = time.Unix(0, 0)
					}
					return
				}
			}

			if requestVoteResult.Term > rf.currentTerm {
				DPrintf("Raft[%d] - CollectVotes - has met a peer with higher Term[%d], will return to a follower.\n", me, requestVoteResult.Term)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.ReInitFollower(requestVoteResult.Term)
				// give up requesting vote.
				return
			}
		} else {
			DPrintf("Raft[%d] - CollectVotes - there is an error in return value of the sendRequestVote.\n", me)
		}
	}
	DPrintf("Raft[%d] - CollectVotes - obtained [%d] votes and did not become leader, will go back to follower", me, votesObtained)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.ReInitFollower(rf.currentTerm)
	return
}

func ElectionThread(rf *Raft) {
	for {
		time.Sleep(electionTimeoutBase * time.Millisecond)
		
		rf.mu.Lock()

		elapse := time.Now().Sub(rf.lastActivity)
		electionTimeout := time.Duration(electionTimeoutBase + rand.Intn(200)) * time.Millisecond

		if elapse < electionTimeout {
			rf.mu.Unlock()
			continue
		}

		if (rf.votedFor != -1) {
			rf.mu.Unlock()
			DPrintf("Raft[%d] - ElectionThread - has voted to Raft[%d], will give up this round", rf.me, rf.votedFor)
			continue
		}

		rf.BecomeCandidate()

		logLength := len(rf.log)
		var localLastLogIndex, localLastLogTerm int
		if logLength == 0 {
			localLastLogIndex = 0
			localLastLogTerm = 0
		} else {
			localLastLogIndex = rf.log[logLength - 1].Index
			localLastLogTerm = rf.log[logLength - 1].Term
		}

		requestVoteArgs := &RequestVoteArgs {
			Term: 			rf.currentTerm,
			CandidateId: 	rf.me,
			LastLogIndex: 	localLastLogIndex,
			LastLogTerm:	localLastLogTerm,
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
				DPrintf("Raft[%d] - ElectionThread - sendRequestVote to [%d] has returned [%t], with reply: %p",
						rf.me, peerIndex, ok, reply)
				if ok {
					requestVoteResultChan<- reply
				} else {
					requestVoteResultChan<- nil
				}

			}(i)
		}
		
		rf.CollectVotes(requestVoteResultChan)

		//*

		// var me int
		// var lastActivity time.Time
		// {
		// 	rf.mu.Lock()
		// 	// defer rf.mu.Unlock()
		// 	me = rf.me
		// 	lastActivity = rf.lastActivity
		// 	rf.mu.Unlock()
		// }

		// electionTimeout := time.Duration(electionTimeoutBase + rand.Intn(100))
		// DPrintf("Raft[%d]'s electionTimeout in this round is %d microseconds...\n", me, electionTimeout)

		// if time.Now().Sub(lastActivity) > (electionTimeout * time.Microsecond) {
		// 	DPrintf("test timeoutThread 0")
		// 	{
		// 		rf.mu.Lock()
		// 		// defer rf.mu.Unlock()
		// 		if (rf.votedFor != -1) {
		// 			DPrintf("Raft[%d] has voted to Raft[%d], will skip this round", rf.me, rf.votedFor)
		// 			continue
		// 		}
		// 		rf.BecomeCandidate()
		// 	}

		// 		DPrintf("test timeoutThread 1")

		// 	var localLastLogIndex, localLastLogTerm int
		// 	var logLength int
		// 	var requestVoteArgs *RequestVoteArgs
		// 	var peersNum int

		// 	{
		// 		// defer rf.mu.Unlock()
		// 		logLength = len(rf.log)
		// 		if logLength == 0 {
		// 			localLastLogIndex = 0
		// 			localLastLogTerm = 0
		// 		} else {
		// 			localLastLogIndex = rf.log[logLength - 1].Index
		// 			localLastLogTerm = rf.log[logLength - 1].Term
		// 		}

		// 		requestVoteArgs = &RequestVoteArgs {
		// 			Term: 			rf.currentTerm,
		// 			CandidateId: 	rf.me,
		// 			LastLogIndex: 	localLastLogIndex,
		// 			LastLogTerm:	localLastLogTerm,
		// 		}
		// 		peersNum = len(rf.peers)
		// 		rf.mu.Unlock()
		// 	}
		// 	DPrintf("test timeoutThread 2")

		// 	requestVoteResultChan := make(chan *RequestVoteReply, peersNum - 1)
		// 	for i := 0; i < peersNum; i++ {
		// 		if me == i {
		// 			continue
		// 		}
		// 		go func(peerIndex int) {
		// 			reply := &RequestVoteReply{}
		// 			ok := rf.sendRequestVote(peerIndex, requestVoteArgs, reply)
		// 			DPrintf("Vote Request from [%d] to [%d] has returned [%t], with reply: %p",
		// 					me, peerIndex, ok, reply)
		// 			if ok {
		// 				requestVoteResultChan<- reply
		// 			} else {
		// 				requestVoteResultChan<- nil
		// 			}

		// 		}(i)
		// 	}
			
		// 	rf.CollectVotes(requestVoteResultChan)

		// }
	}
}