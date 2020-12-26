package kvraft

import (
	"sync"
	"crypto/rand"
	"math/big"
	// "time"

	"../labrpc"
	// "../slog"
)


type Clerk struct {
	servers 		[]*labrpc.ClientEnd
	// You will have to modify this struct.
	mu 				sync.Mutex
	currentLeaderId int
	// ClerkId			int64
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	// ck.ClerkId = nrand()
	// You'll have to add code here.

	return ck
}