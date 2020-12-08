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
	DPrintf(LOG_DEBUG, "Raft[%d] - AppendEntries - will try to lock its mutex.", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(LOG_INFO, "Raft[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] currentRole[%d] currentLeader[%d] currentLog{%+v} PrevLogIndex[%d], PrevLogTerm[%d] leaderCommit[%d], EntriesLength[%d]\n",
				 rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.currentRole, rf.currentLeader, rf.log, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(args.Entries))
	defer func() {
		DPrintf(LOG_INFO, "Raft[%d] Return AppendEntries, LeaderId[%d] Term[%d] currentTerm[%d] currentRole[%d] success:%t currentLeader[%d] commitIndex[%d] log:{%v}\n",
					 rf.me, args.LeaderId, args.Term, rf.currentTerm, rf.currentRole, reply.Success, rf.currentLeader, rf.commitIndex, rf.log)
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
	
	if args.PrevLogIndex < 0 {
		DPrintf(LOG_ERR, "Raft[%d] - AppendEntries - Index out of range: args.PrevLogIndex[%d] length of rf.log[%d]",
						  rf.me, args.PrevLogIndex, len(rf.log))
	}

	if args.PrevLogIndex > GetLastLogIndex(rf) {
		DPrintf(LOG_INFO, "Raft[%d] Handle AppendEntries, input PrevLogIndex[%d] exceed the lastLogIndex[%d], return false to go back.",
						  rf.me, args.PrevLogIndex, GetLastLogIndex(rf))
		return
	} 
	
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf(LOG_INFO, "Raft[%d] Handle AppendEntries, input PrevLogTerm[%d] conlicts with the existing one[%d], return false to go back.",
						  rf.me, args.PrevLogTerm, rf.log[args.PrevLogIndex].Term)
		// 可以返回冲突的 term 及该 term 的第一个 index，使 leader 可以直接回退 nextIndex 到合适位置。（到哪没想好）
		return
	}

	rf.log = rf.log[:args.PrevLogIndex + 1] // 删除掉之后的 slice
	DPrintf(LOG_DEBUG, "Raft[%d] - AppendEntries - Remove logs after index[%d], current log is: {%v}.", rf.me, args.PrevLogIndex, rf.log)

	for _, Entry := range(args.Entries) {
		rf.log = append(rf.log, Entry)
	}

	if args.LeaderCommit > rf.commitIndex {
		tmpDPrintfCommitIndex := rf.commitIndex
		rf.commitIndex = rf.log[len(rf.log) - 1].Index
		if rf.commitIndex > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		}
		
		for i := tmpDPrintfCommitIndex + 1; i <= rf.commitIndex; i++ {
			rf.NotifyApplyChannel(true, rf.log[i].Command, i)
		}
		DPrintf(LOG_DEBUG, "Raft[%d] - AppendEntries - commitIndex will change from[%d] to [%d]",
							rf.me, tmpDPrintfCommitIndex, rf.commitIndex)
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	DPrintf(LOG_DEBUG, "Raft[%d] - sendAppendEntries - will send AppendEntries Request to [%d]",
									rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//-------------------AppendEntries RPC-------------------//