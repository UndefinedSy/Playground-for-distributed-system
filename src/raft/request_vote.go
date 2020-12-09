package raft

import (
	"time"
	"../slog"
)

type RequestVoteArgs struct {
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
	// Your data here (2A, 2B).
}

type RequestVoteReply struct {
	Term		int
	VoteGranted	bool
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	slog.Log(slog.LOG_DEBUG, "Raft[%d] will try to lock its mutex.", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	slog.Log(slog.LOG_INFO, `Raft[%d] Handle RequestVote, currentVotedFor[%d], CurrentTerm[%d]; args: CandidatesId[%d] Term[%d] LastLogIndex[%d] LastLogTerm[%d]`,
							rf.me, rf.votedFor, rf.currentTerm, 
							args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
	defer func() {
		slog.Log(slog.LOG_INFO, "Raft[%d] Return RequestVote, CandidatesId[%d] Term[%d] currentTerm[%d] localLastLogIndex[%d] localLastLogTerm[%d] VoteGranted[%v]",
			 				     rf.me, args.CandidateId, args.Term, rf.currentTerm, GetLastLogIndex(rf), GetLastLogTerm(rf), reply.VoteGranted)
	}()

	// init reply
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		slog.Log(slog.LOG_INFO, "requester[%d]'s Term[%d] < local[%d] term[%d]",
					 			 args.CandidateId, args.Term, rf.me, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		slog.Log(slog.LOG_INFO, "Raft[%d] goes to Follower because Term in Vote Request[%d] from Raft[%d] is higher than currentTerm[%d]",
						   		 rf.me, args.Term, args.CandidateId, rf.currentTerm)
		rf.ReInitFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		slog.Log(slog.LOG_DEBUG, "Raft[%d] has already voted to candidate(%d)",
					 			  rf.me, rf.votedFor)
		return
	}

	localLastLogIndex := GetLastLogIndex(rf)
	localLastLogTerm := GetLastLogTerm(rf)
	if args.LastLogTerm > localLastLogTerm || (args.LastLogTerm == localLastLogTerm && args.LastLogIndex >= localLastLogIndex) {
		slog.Log(slog.LOG_DEBUG, "Raft[%d] will vote candidate(%d)",
					 			  rf.me, args.CandidateId)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastActivity = time.Now()
	}

	// Your code here (2A, 2B).
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. 
// 
// If a reply arrives within a timeout interval, Call() returns true; 
// otherwise Call() returns false. Thus Call() may not return for a while.
// 
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is **no need to implement your own timeouts** around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	slog.Log(slog.LOG_DEBUG, "Raft[%d] has received response from Raft[%d]", rf.me, server)
	return ok
}

//--------------------RequestVote RPC--------------------//