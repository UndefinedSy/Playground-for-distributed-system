package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "sync/atomic"
import "../labrpc"

import "time"
import "math/rand"
// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Term 	int
	Index	int
	Data	[]byte
}

type Role int
const (
	Follower 	Role = 0
	Candidate	Role = 1
	Leader		Role = 2
)

const electionTimeoutBase = 200 // microsecond

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	
	// generic persistent state
	currentTerm int
	votedFor	int
	log			[]*Entry	
	// generic persistent state

	// generic volatile state
	commitIndex	int
	lastApplied	int
	// generic volatile state

	// leader's volatile state
	nextIndex	[]int
	matchIndex	[]int
	// leader's volatile state

	// Self-defined state
	currentLeader	int
	currentRole		Role
	lastActivity	time.Time
	// Self-defined state


	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) BecomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentRole = Leader
	rf.currentLeader = rf.me
	DPrintf("Raft[%d] became Leader term[%d]",
			 rf.me, rf.currentTerm)
	// need to boardcast empty AppendEntry()
}

func (rf *Raft) ReInitFollower(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
	rf.currentRole = Follower
	rf.votedFor = -1
	DPrintf("Raft[%d] became Follower term[%d]\n",
				 rf.me, rf.currentTerm)
}

func (rf *Raft) BecomeCandidate() {
	rf.currentRole = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastActivity = time.Now()
	DPrintf("Raft[%d] became Candidate term[%d], votedFor[%d] lastActivity[%s]\n",
			rf.me, rf.currentTerm, rf.votedFor, rf.lastActivity.Format(time.RFC3339))
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	isleader := false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.currentRole == Leader {
		isleader = true
	}
	DPrintf("GetState of Raft[%d]: term[%d], isLeader[%t]", rf.me, rf.currentTerm, isleader)

	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//--------------------RequestVote RPC--------------------//

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
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

//=======================================================//

//-------------------AppendEntries RPC-------------------//

type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries			[]*Entry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term		int
	Success		bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("Raft[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] currentRole[%d] lastActivity[%s]\n",
				 rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.currentRole, rf.lastActivity.Format(time.RFC3339))
	defer func() {
		DPrintf("Raft[%d] Return AppendEntries, LeaderId[%d] Term[%d] currentTerm[%d] currentRole[%d] success:%t lastActivity[%s]\n",
					 rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.currentRole, reply.Success, rf.lastActivity.Format(time.RFC3339))
	}()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	rf.ReInitFollower(args.Term)
	{
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.lastActivity = time.Now()
		reply.Success = true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//-------------------AppendEntries RPC-------------------//

func (rf *Raft) BoardCastAppendEntries() {
	DPrintf("test BoardCastAppendEntries 0")
	var me, currentTerm, peersNum int
	{
		rf.mu.Lock()
		// defer rf.mu.Unlock()
		me = rf.me
		currentTerm = rf.currentTerm
		peersNum = len(rf.peers)
		rf.mu.Unlock()
	}
	DPrintf("test BoardCastAppendEntries 1")

	appendEntriesArgs := &AppendEntriesArgs {
		Term: 		currentTerm,
		LeaderId:	me,
	}

	for i := 0; i < peersNum; i++ {
		if me == i {
			continue
		}
		go func(peerIndex int) {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(peerIndex, appendEntriesArgs, reply)
			DPrintf("Raft[%d] BoardCastAppendEntries to peer[%d] success:%t\n",
					 rf.me, peerIndex, ok)
		}(i)
	}

	time.Sleep(500 * time.Microsecond)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

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
		DPrintf("Raft[%d] will query the [%d] requestVoteResult", me, i+1)
		requestVoteResult := <-requestVoteResultChan
		DPrintf("Raft[%d] has fetched the [%d] requestVoteResult", me, i+1)
		if requestVoteResult != nil {
			if requestVoteResult.VoteGranted {
				DPrintf("Raft[%d] has got 1 vote. Currently have [%d] votes.\n", me, votesObtained)
				votesObtained++
				if votesObtained > (participantsNum / 2) {
					DPrintf("Raft[%d] has got majority[%d] votes, will become a leader.\n",
							me, votesObtained)
					rf.BecomeLeader()
					rf.BoardCastAppendEntries()
					return
				}
			}

			if requestVoteResult.Term > rf.currentTerm {
				DPrintf("Raft[%d] has met a peer with higher Term[%d], will return to a follower.\n", me, requestVoteResult.Term)
				rf.ReInitFollower(requestVoteResult.Term)
				// give up requesting vote.
				return
			}
		} else {
			DPrintf("CollectVotes - there is an error in return value of the sendRequestVote.\n",)
		}
	}
	DPrintf("Raft[%d] obtained [%d] votes and did not become leader, will go back to follower", me, votesObtained)
	rf.ReInitFollower(rf.currentTerm)
	return
}

func timeoutThread(rf *Raft) {
	for {
		DPrintf("Raft[%d] will sleep for 200ms and then begin a new timeoutThread round.\n", rf.me)
		time.Sleep(electionTimeoutBase * time.Microsecond)

		var me int
		var lastActivity time.Time
		{
			rf.mu.Lock()
			// defer rf.mu.Unlock()
			me = rf.me
			lastActivity = rf.lastActivity
			rf.mu.Unlock()
		}

		electionTimeout := time.Duration(electionTimeoutBase + rand.Intn(100))
		DPrintf("Raft[%d]'s electionTimeout in this round is %d microseconds...\n", me, electionTimeout)

		if time.Now().Sub(lastActivity) > (electionTimeout * time.Microsecond) {
			DPrintf("test timeoutThread 0")
			{
				rf.mu.Lock()
				// defer rf.mu.Unlock()
				if (rf.votedFor != -1) {
					DPrintf("Raft[%d] has voted to Raft[%d], will skip this round", rf.me, rf.votedFor)
					continue
				}
				rf.BecomeCandidate()
			}

				DPrintf("test timeoutThread 1")

			var localLastLogIndex, localLastLogTerm int
			var logLength int
			var requestVoteArgs *RequestVoteArgs
			var peersNum int

			{
				// defer rf.mu.Unlock()
				logLength = len(rf.log)
				if logLength == 0 {
					localLastLogIndex = 0
					localLastLogTerm = 0
				} else {
					localLastLogIndex = rf.log[logLength - 1].Index
					localLastLogTerm = rf.log[logLength - 1].Term
				}

				requestVoteArgs = &RequestVoteArgs {
					Term: 			rf.currentTerm,
					CandidateId: 	rf.me,
					LastLogIndex: 	localLastLogIndex,
					LastLogTerm:	localLastLogTerm,
				}
				peersNum = len(rf.peers)
				rf.mu.Unlock()
			}
			DPrintf("test timeoutThread 2")

			requestVoteResultChan := make(chan *RequestVoteReply, peersNum - 1)
			for i := 0; i < peersNum; i++ {
				if me == i {
					continue
				}
				go func(peerIndex int) {
					reply := &RequestVoteReply{}
					ok := rf.sendRequestVote(peerIndex, requestVoteArgs, reply)
					DPrintf("Vote Request from [%d] to [%d] has returned [%t], with reply: %p",
							me, peerIndex, ok, reply)
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
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	
	// persistent state
	rf.votedFor = -1
	// persistent state on all servers

	// volatile state

	// Your initialization code here (2A, 2B, 2C).

	// volatile state on leader

	// volatile state on leader

	rand.Seed(time.Now().UnixNano())
	go timeoutThread(rf)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
