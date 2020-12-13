package raft

import (
	"time"

	"../slog"
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
	Term			int
	Success			bool
	// fast back up when there is a conflict in AppendEntries
	ConflictIndex	int
}

func (rf *Raft) FollowerTryUpdateCommitIndex(leaderCommit int) {
	if leaderCommit > rf.commitIndex { // will update commitIndex
		// commitIndex = min(leaderCommit, index of last new entry)
		tmpDPrintfCommitIndex := rf.commitIndex
		rf.commitIndex = GetLastLogIndex(rf)
		if rf.commitIndex > leaderCommit {
			rf.commitIndex = leaderCommit
		}
		
		// notify the client.
		// ??? I'm not sure if this is necessary fot a follower ???
		for i := tmpDPrintfCommitIndex + 1; i <= rf.commitIndex; i++ {
			rf.NotifyApplyChannel(true, rf.log[i].Command, i)
		}
		slog.Log(slog.LOG_DEBUG, "Raft[%d] commitIndex has been change from[%d] to [%d]",
								  rf.me, tmpDPrintfCommitIndex, rf.commitIndex)
	}
}

func (rf *Raft) GetFirstIndexByTerm(targetTerm int) int {
	var targetIndex int // init?
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Term < targetTerm {
			targetIndex = i + 1
			break
		}
	}
	return targetIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	slog.Log(slog.LOG_DEBUG, "Raft[%d] will try to lock its mutex.", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	slog.Log(slog.LOG_INFO, `Raft[%d] Handle AppendEntries, CurrentTerm[%d] currentRole[%d] currentLeader[%d]; args: LeaderId[%d] Term[%d] PrevLogIndex[%d], PrevLogTerm[%d] leaderCommit[%d], EntriesLength[%d]`,
							 rf.me, rf.currentTerm, rf.currentRole, rf.currentLeader,
							 args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
	defer func() {
		slog.Log(slog.LOG_INFO, `Raft[%d] Return AppendEntries, currentTerm[%d] currentRole[%d] success:%t currentLeader[%d] commitIndex[%d]`,
								 rf.me, rf.currentTerm, rf.currentRole, reply.Success, rf.currentLeader, rf.commitIndex)
	}()

	// init reply
	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1

	if args.Term < rf.currentTerm {
		slog.Log(slog.LOG_INFO, "The request from Raft[%d] is stale, will return False.", args.LeaderId)
		return
	}

	if (args.Term >= rf.currentTerm) {
		rf.ReInitFollower(args.Term)
	}

	rf.currentLeader = args.LeaderId
	rf.lastActivity = time.Now()
	
	// for debug, can be deleted
	if args.PrevLogIndex < 0 {
		slog.Log(slog.LOG_ERR, "Raft[%d] Index out of range: args.PrevLogIndex[%d] length of rf.log[%d]",
						  rf.me, args.PrevLogIndex, len(rf.log))
	}

	// can put together with the next if 
	if args.PrevLogIndex > GetLastLogIndex(rf) {
		slog.Log(slog.LOG_INFO, "The args PrevLogIndex[%d] exceed the Raft[%d]'s lastLogIndex[%d], return false to go back.",
								 args.PrevLogIndex, rf.me, GetLastLogIndex(rf))
		reply.ConflictIndex = GetLastLogIndex(rf)
		return
	} 
	
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictIndex = rf.GetFirstIndexByTerm(args.PrevLogTerm)
		slog.Log(slog.LOG_INFO, "The args PrevLogTerm[%d] conlicts with Raft[%d]'s existing one[%d], return false and back up to [%d].",
						  		 args.PrevLogTerm, rf.me, rf.log[args.PrevLogIndex].Term, reply.ConflictIndex)
		return
	}

	rf.log = rf.log[:args.PrevLogIndex + 1] // 删除掉之后的 slice
	slog.Log(slog.LOG_DEBUG, "Raft[%d] removed logs after index[%d], current log is: {%v}.",
							  rf.me, args.PrevLogIndex, rf.log)

	for _, Entry := range(args.Entries) {
		rf.log = append(rf.log, Entry)
	}

	rf.FollowerTryUpdateCommitIndex(args.LeaderCommit)

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//-------------------AppendEntries RPC-------------------//