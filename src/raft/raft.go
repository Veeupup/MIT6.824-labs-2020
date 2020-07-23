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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
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

type LogEntry struct {
	Term  int
	Index int
}

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
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that receieved vote in current term
	log         []LogEntry // log entries

	// Volatile state
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	currentState   int       // which state: leader / follower / candidate
	isConnected    bool      // decide whether this node has disconnected from the leader
	timeout        int       // election timeout
	voteCounts     int       // count how many votes this node has got
	lastHeartBTime time.Time // last time get heartbeat

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.currentState == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()

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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // leader's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

//
// RequestVoteReply RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.votedFor > 0 {
		reply.VoteGranted = false
		return
	}

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		fmt.Printf("Refuse Vote: %v refuse to give vote to %v\n", rf.me, args.CandidateId)
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		fmt.Printf("Vote: %v(term: %v) give vote to %v(term: %v)\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		// go rf.startElectionTimeout()
	}

	return
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
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
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

// AppendEntriesArgs args
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        //follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store(empty for heartbeat; may send more than one for effiency)
	LeaderCommit int        // leader's commitIndex
}

// AppendEntriesReply reply
type AppendEntriesReply struct {
	Term    int  // current term, for leader to update itself
	Success bool // matching
}

// AppendEntries RPC handler, receive entries from leaders
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else if len(args.Entries) == 0 {
		// update heartbeat time
		rf.currentState = FOLLOWER         // update it's state
		rf.voteCounts = -1 * len(rf.peers) // tell him not to elect for a leader
		rf.votedFor = -1
		rf.lastHeartBTime = time.Now() // update heartbeat time
		rf.currentTerm = args.Term

		reply.Success = true
		fmt.Printf("HeartBeat: %v(term: %v) -> %v(term: %v)\n", args.LeaderId, args.Term, rf.me, rf.currentTerm)

	}

	rf.mu.Unlock()

	return
}

// for leader to send heartbeats and append entries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

func (rf *Raft) prepareAppendEntries() {

	for {
		rf.mu.Lock()

		time.Sleep(time.Millisecond * time.Duration(100))

		if rf.currentState == LEADER {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}

				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.Entries = make([]LogEntry, 0)

				reply := AppendEntriesReply{}

				// fmt.Printf("%v leader send heart to %v \n", rf.me, i)
				go rf.sendAppendEntries(i, &args, &reply)

				// rf.currentTerm = reply.Term

				// if !reply.Success {
				// 	time.Sleep(time.Second * time.Duration(1))
				// }
			}
		}

		rf.mu.Unlock()
	}

}

func (rf *Raft) prepareElection() {
	fmt.Printf("Timeout: %v timeout, and is preparing election......\n", rf.me)
	args := RequestVoteArgs{}

	rf.mu.Lock()

	rf.currentState = CANDIDATE
	rf.votedFor = rf.me // vote for myself
	rf.voteCounts = 1
	args.Term = rf.currentTerm + 1
	args.CandidateId = rf.me

	// to do for 2b 2c
	args.LastLogIndex = 0
	args.LastLogTerm = 0

	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		reply := RequestVoteReply{}
		// fmt.Printf("%v ask %v for vote\n", rf.me, i)
		rf.sendRequestVote(i, &args, &reply)

		rf.mu.Lock()

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
		}

		if rf.currentState == CANDIDATE {
			if reply.VoteGranted {
				rf.voteCounts++
				if rf.voteCounts > len(rf.peers)/2 {
					rf.currentState = LEADER
					rf.currentTerm = args.Term
					fmt.Printf("Leader: %v(term: %v) has become leader!\n", rf.me, rf.currentTerm)
				}
			} else {
				fmt.Printf("%v(has voted: %v) confused to give vote to %v\n", i, rf.votedFor, rf.me)
			}
		}

		rf.mu.Unlock()
	}
}

// electionTimeout for each follower to decide when to elect a new leader
func (rf *Raft) startElectionTimeout() {
	for {
		time.Sleep(time.Millisecond * time.Duration(rf.timeout))
		rf.mu.Lock()
		if time.Since(rf.lastHeartBTime) > time.Millisecond*time.Duration(rf.timeout) {
			rf.mu.Unlock()
			rf.prepareElection()
			rf.mu.Lock()
		}
		rf.mu.Unlock()
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

	// initialize state
	rf.votedFor = -1           // vote for no no one
	rf.currentTerm = 0         // initilize to term 0
	rf.currentState = FOLLOWER // initilize to follower
	rf.lastHeartBTime = time.Now()
	// initilize as not connected, wait for electionTimeout, if heartbeat comes, update this state
	rf.voteCounts = 0

	// set random election timeout between 150ms ~ 300ms
	rand.Seed(time.Now().UnixNano())
	rf.timeout = rand.Intn(150) + 150

	go rf.startElectionTimeout()
	go rf.prepareAppendEntries()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
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
