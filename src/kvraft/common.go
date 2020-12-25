package kvraft

const (
	OK            	= "OK"
	ErrNoKey      	= "ErrNoKey"
	ErrWrongLeader	= "ErrWrongLeader"
	ErrAgain		= "EAGAIN"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key		string
	Value	string
	OpFlag	string // "Put" or "Append"
	//  Here need a UID to reprensent a certain request

	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err 			Err
	CurrentLeaderId	int
}

type GetArgs struct {
	Key string
	//  Here need a UID to reprensent a certain request

	// You'll have to add definitions here.
}

type GetReply struct {
	Err   			Err
	Value 			string
	CurrentLeaderId	int
}
