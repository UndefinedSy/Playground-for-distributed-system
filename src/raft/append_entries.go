package raft

import (
	"time"
)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Raft[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] currentRole[%d] currentLeader[%d] lastActivity[%s]\n",
				 rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.currentRole, rf.currentLeader, rf.lastActivity.Format(time.RFC3339))
	defer func() {
		DPrintf("Raft[%d] Return AppendEntries, LeaderId[%d] Term[%d] currentTerm[%d] currentRole[%d] success:%t currentLeader[%d] lastActivity[%s]\n",
					 rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.currentRole, reply.Success, rf.currentLeader, rf.lastActivity.Format(time.RFC3339))
	}()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if (args.Term >= rf.currentTerm) {
		rf.ReInitFollower(args.Term)
	}

	rf.currentLeader = args.LeaderId
	rf.lastActivity = time.Now()
	
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//-------------------AppendEntries RPC-------------------//