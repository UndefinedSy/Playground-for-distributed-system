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

import (
	"sync"
	"sync/atomic"
	"time"
	"math/rand"

	"../labrpc"
	"fmt"

	"../slog"
)




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
	Command	interface{}
}

// for debug trace
func (entry *Entry) String() string {
	return fmt.Sprintf("{Term[%d], Index[%d], Command[%v]}", entry.Term, entry.Index, entry.Command)
}

type Role int
const (
	ROLE_FOLLOWER 	Role = 0
	ROLE_CANDIDATE	Role = 1
	ROLE_LEADER		Role = 2
)

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
	condLeader		*sync.Cond
	currentLeader	int
	currentRole		Role
	lastActivity	time.Time
	lastHeartbeat	time.Time
	// Self-defined state

	applyCh			chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) BecomeLeader() {
	rf.currentRole 	 = ROLE_LEADER
	rf.currentLeader = rf.me

	rf.nextIndex 	 = make([]int, len(rf.peers))
	rf.matchIndex	 = make([]int, len(rf.peers))

	for i := range(rf.nextIndex) {
		rf.nextIndex[i] = GetLastLogIndex(rf) + 1
	}

	slog.Log(slog.LOG_INFO, "Raft[%d] became Leader term[%d]",
								rf.me, rf.currentTerm)

	rf.persist()
}

func (rf *Raft) ReInitFollower(term int) {
	rf.currentTerm  = term
	rf.currentRole  = ROLE_FOLLOWER
	rf.votedFor 	= -1
	slog.Log(slog.LOG_DEBUG, "Raft[%d] became Follower term[%d]",
							   rf.me, rf.currentTerm)

	rf.persist()
}

func (rf *Raft) BecomeCandidate() {
	rf.currentTerm++
	rf.currentRole  = ROLE_CANDIDATE
	rf.votedFor 	= rf.me
	rf.lastActivity = time.Now()
	slog.Log(slog.LOG_DEBUG, "Raft[%d] became Candidate term[%d], votedFor[%d]",
								rf.me, rf.currentTerm, rf.votedFor)

	rf.persist()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	isleader := false

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.currentRole == ROLE_LEADER {
		isleader = true
	}
	slog.Log(slog.LOG_INFO, "GetState of Raft[%d]: term[%d], isLeader[%t]",
					   		 rf.me, rf.currentTerm, isleader)

	// Your code here (2A).
	return term, isleader
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
	slog.Log(slog.LOG_DEBUG, "Raft[%d] in Start will lock its mutex", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentRole != ROLE_LEADER {
		isLeader = false
	} else {
		index = len(rf.log)
		term = rf.currentTerm
		newEntry := &Entry {
			Index: 	 index,
			Term:	 term,
			Command: command,
		}
		rf.log = append(rf.log, newEntry)
		slog.Log(slog.LOG_DEBUG, "Raft[%d] will append new entry{Index: [%d], Term: [%d], Command:[%T | %v]",
									rf.me, index, term, command, command)
	}
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
	slog.Log(slog.LOG_INFO, "Raft[%d] has been killed", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	rf.condLeader = sync.NewCond(&rf.mu)

	// Your initialization code here (2A, 2B, 2C).
	
	// persistent state
	rf.votedFor = -1
	rf.log = append(rf.log, &Entry{})
	// persistent state on all servers

	// volatile state
	rf.currentRole = ROLE_FOLLOWER
	// Your initialization code here (2A, 2B, 2C).

	// volatile state on leader

	// volatile state on leader

	rf.applyCh = applyCh

	rand.Seed(int64(me))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go ElectionThread(rf)
	go AppendEntriesThread(rf)

	return rf
}
