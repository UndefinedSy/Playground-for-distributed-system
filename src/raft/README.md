# About Raft

This is a basic implementation of the Raft protocol, which enables basic multi-node consensus in a cluster with an odd number of nodes.

# Exposed Function Signature for Client Usage:

### `func (rf *Raft) GetState() (currentTerm int, isLeader bool)` 
The receiver instance will return its state:  

@ return `currentTerm`: The receiver's currentTerm 
@ return `isLeader`:    Whether the receiver believes it is the leader.

### `func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool)`
the service using Raft (e.g. a k/v server) wants to start agreement on the next command to be appended to Raft's log. 
* If this server isn't the leader, returns false. 
* Otherwise start the agreement and return immediately. 

There is no guarantee that this command will ever be committed to the Raft log, since the leader may fail or lose an election. 

@ IN `command`:   Command from the client. (duck type)
@ return `index`:    The position of the log entry for `command`
@ return `term`:     The term of the log entry for `command`
@ return `isLeader`: Whether the receiver believes it is the leader.

### `func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) raftInstance *Raft`
Create a Raft server. 

@ IN `peers`: The ports of all the Raft servers (including this one, peers[me]). all the servers' peers[] arrays have the same order.
@ IN `me`:    The identifier of current instance. (normally ip addr)
@ IN `persister`: Place to save its persistent state, and also initially holds the most recent saved state, if any.
@ IN `applyCh`: A channel on which the client expects Raft to send ApplyMsg messages.
@ return `raftInstance`: The pointer to the new Raft instance.

# Dependencies

* RPC: `./src/labrpc/labrpc.go`
* Persister: `./src/labgob/labgob.go`
* Log: `./src/slog/slog.go`

# TODO:

- [ ] Code refactoring to make the structure clearer and simpler
- [ ] Cluster Membership Change Mechanism
- [ ] Log Compaction / SnapShot
- [ ] Move the Persister to a real persistent I/O
  
# ISSUEs
- [ ] When log level switch to LOG_INFO/LOG_DEBUG, there will be too mant logs to print and maybe break the timing, which will cause test failed.