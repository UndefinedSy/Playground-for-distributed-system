package raft

import "time"

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
	rf.mu.Lock()
	DPrintf("Raft[%d] has locked its mutex in RequestVote.\n", rf.me)
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	DPrintf("Raft[%d] Handle RequestVote, CandidatesId[%d] Term[%d] CurrentTerm[%d] LastLogIndex[%d] LastLogTerm[%d] votedFor[%d]\n",
			rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	defer func() {
		DPrintf("Raft[%d] Return RequestVote, CandidatesId[%d] Term[%d] currentTerm[%d] VoteGranted[%v]\n",
			 	rf.me, args.CandidateId, args.Term, rf.currentTerm, reply.VoteGranted)
	}()

	if args.Term < rf.currentTerm {
		DPrintf("requester(%d)'s Term[%d] < local(%d) term[%d]\n",
					 args.CandidateId, args.Term, rf.me, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.ReInitFollower(args.Term)
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("Raft[%d] has already voted other candidate(%d)\n",
					 rf.me, rf.votedFor)
		return
	}

	var localLastLogIndex, localLastLogTerm int
	localLogLength := len(rf.log)
	if localLogLength == 0 {
		localLastLogIndex = 0
		localLastLogTerm = 0
	} else {
		localLastLogIndex = rf.log[localLogLength - 1].Index
		localLastLogTerm = rf.log[localLogLength - 1].Term
	} 

	if (localLastLogTerm <= args.LastLogTerm) {
		if (localLastLogIndex <= args.LastLogIndex) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastActivity = time.Now() // is this necessary?
		}
	}

	return
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
	return ok
}

//--------------------RequestVote RPC--------------------//