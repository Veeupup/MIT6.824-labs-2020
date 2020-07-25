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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// states
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

// LogEntry log
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

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// Volatile state on candidates
	voteCounts int // count how many votes this node has got

	// Volatile state on followers
	latestHeartTime time.Time // last time get heartbeat
	electionTimeout int

	state    int // which state: leader / follower / candidate
	leaderId int

	electionTimeoutChan chan bool // true 表示可以开始选举
	heartbeatPeriodChan chan bool // true 表示可以开始发送心跳
	heartbeatPeriod     int       // 心跳发送间隔时间，单位 ms

	leaderCond    *sync.Cond // 给 heartbeatPeriodTick 发送信号，表示可以开始发送心跳
	nonLeaderCond *sync.Cond // 当 peer 不再是 leader 时发送信号

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == LEADER {
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

	if rf.currentTerm <= args.Term {

		if rf.currentTerm < args.Term {
			DPrintf("[RequestVote]: Id %d Term %d State %s\t||\t args's term %d is larger",
				rf.me, rf.currentTerm, state2name(rf.state), args.Term)

			rf.currentTerm = args.Term

			rf.votedFor = -1
			rf.switchTo(FOLLOWER)

			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			return
		}

	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

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
	if rf.currentTerm <= args.Term {
		if rf.currentTerm < args.Term {
			DPrintf("[AppendEntries]: Id %d Term %d State %s\t||\targs't term %d is newer\n",
				rf.me, rf.currentTerm, state2name(rf.state), args.Term)
		}
		if len(args.Entries) == 0 {
			DPrintf("[AppendEntries]: Id %d Term %d State %s\t||\theartbeat from leader %v\n",
				rf.me, rf.currentTerm, state2name(rf.state), args.LeaderId)
			reply.Success = true

			rf.resetElectionTimer()
			rf.votedFor = -1
			rf.switchTo(FOLLOWER)
			rf.leaderId = args.LeaderId
		}

	} else {
		reply.Success = false
	}

	rf.mu.Unlock()
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

// 转换状态，并且做出相应的操作
func (rf *Raft) switchTo(newState int) {
	oldState := rf.state
	rf.state = newState
	if oldState == LEADER && newState == FOLLOWER {
		rf.nonLeaderCond.Broadcast()
	} else if oldState == CANDIDATE && newState == LEADER {
		rf.leaderCond.Broadcast()
	}
}

func (rf *Raft) resetElectionTimer() {
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = 150 + rand.Intn(150)
	rf.latestHeartTime = time.Now()
}

// 选举超时 routine
// 需要一直检测是否超时
func (rf *Raft) electionTimeoutTick() {
	for {
		if _, isLeader := rf.GetState(); isLeader { // 如果是 Leader 就不需要进行选举 timeout
			rf.mu.Lock()
			rf.nonLeaderCond.Wait() // 释放锁，并且等待这个下次成为非 Leader 的信号量
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			// 如果从上次心跳的时间到现在超过了选举超时，那么开始进行选举
			if time.Since(rf.latestHeartTime) >= time.Duration(rf.electionTimeout)*time.Millisecond {
				DPrintf("[ElectionTimeoutTick]: Id %v Term %v State %s\t||\ttimeout, convert to Candidate\n", rf.me, rf.currentTerm, state2name(rf.state))
				rf.electionTimeoutChan <- true
			}
			rf.mu.Unlock()
			time.Sleep(time.Microsecond * 20) // 每隔 10ms 检查一次是否超时，这样在第一时间就能发现是否超时
		}
	}
}

// 发送心跳 routine
func (rf *Raft) heartbeatPeriodTick() {
	for {
		if _, isLeader := rf.GetState(); isLeader {
			rf.mu.Lock()
			rf.heartbeatPeriodChan <- true
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * time.Duration(rf.heartbeatPeriod))
		} else {
			rf.mu.Lock()
			rf.leaderCond.Wait()
			rf.mu.Unlock()
		}
	}
}

// 开始选举 routine
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.switchTo(CANDIDATE)
	// 1. term++
	rf.currentTerm++
	// 2. 给自己投票
	rf.votedFor = rf.me
	nVotes := 1
	// 3. 重置定时器
	rf.resetElectionTimer()
	DPrintf("[StartElection]: Id %v Term %v State %s\t||\ttimeout, start Election\n", rf.me, rf.currentTerm, state2name(rf.state))
	rf.mu.Unlock()

	go func(nVotes *int, rf *Raft) {
		var wg sync.WaitGroup
		winThreshold := len(rf.peers)/2 + 1

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}

			rf.mu.Lock()
			wg.Add(1)
			lastLogIndex := len(rf.log) - 1
			if lastLogIndex < 0 {
				DPrintf("[StartElection]: Id %v Term %v State %s\t||\tinvalid Log index\n", rf.me, rf.currentTerm, state2name(rf.state))
			}
			args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me,
				LastLogIndex: lastLogIndex, LastLogTerm: rf.log[lastLogIndex].Term}
			DPrintf("[StartElection]: Id %v Term %v State %s\t||\tissue RequestVote RPC to peer %v\n", rf.me, rf.currentTerm, state2name(rf.state), i)
			rf.mu.Unlock()
			var reply RequestVoteReply

			// 使用 go routine 来单独给每个 peer 发送 RPC
			go func(i int, rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) {
				defer wg.Done()

				ok := rf.sendRequestVote(i, args, reply)

				if !ok {
					rf.mu.Lock()
					DPrintf("[StartElection]: Id %v Term %v State %s\t||\tissue RequestVote RPC to peer %v failed\n", rf.me, rf.currentTerm, state2name(rf.state), i)
					rf.mu.Unlock()
					return
				}

				// 没有获得选票有很多种可能
				if reply.VoteGranted == false {

					rf.mu.Lock()
					DPrintf("[StartElection]: Id %v Term %v State %s\t||\tPeer %v Rejected to vote\n", rf.me, rf.currentTerm, state2name(rf.state), i)

					// 如果 reply 的 term 大于当前的 term，设置 currentTerm = reply.Term
					// 而且转变为 FOLLOWER
					if rf.currentTerm < reply.Term {
						DPrintf("[StartElection]: Id %v Term %v State %s\t||\tless than %v term\n", rf.me, rf.currentTerm, state2name(rf.state), i)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.switchTo(FOLLOWER)
					}
					rf.mu.Unlock()
				} else {
					// 获得了选票
					rf.mu.Lock()
					DPrintf("[StartElection]: Id %v Term %v State %s\t||\tget vote from %v\n", rf.me, rf.currentTerm, state2name(rf.state), i)

					*nVotes++

					if rf.state == CANDIDATE && *nVotes >= winThreshold {
						DPrintf("[WinElection]: Id %v Term %v State %s\t||\twin election with nVotes %v\n", rf.me, rf.currentTerm, state2name(rf.state), *nVotes)
						rf.currentTerm = args.Term
						rf.switchTo(LEADER)
						rf.leaderId = rf.me

						// leader 启动的时候初始化所有的 nextIndex 为其 log 之后的位置
						// for i := 0; i < len(rf.peers); i++ {
						// 	rf.nextIndex[i] = len(rf.log)
						// }

						// 立即发送一次心跳,防止其他的 peer 开始无意义的投票行动
						go rf.broadcastHeartbeat()
					}

					rf.mu.Unlock()

				}

			}(i, rf, &args, &reply)

		}

	}(&nVotes, rf)

}

// 并行给其他所有的 peer 发送 AppendEntries，在每个发送的 goroutine 中实时统计
// 已发送成功的 RPC 的个数，当达到多数条件的时候，提升 commitIndex 到 index，并通过一次心跳通知
// 其他所有 peer 提升自己的 commitIndex
func (rf *Raft) broadcastAppendEntries(index int, term int, commitIndex int, nReplica int, name string) {
	var wg sync.WaitGroup

	// 只有 Leader 需要发送心跳
	if _, isLeader := rf.GetState(); !isLeader {
		return
	}

	// 避免得到调度过迟
	rf.mu.Lock()
	if rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)

		go func(i int, rf *Raft) {
			defer wg.Done()

			// 涉及到 retry 操作，避免过期的 leader 继续操作
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}

			// 避免进入新 term 时仍然继续旧操作
			rf.mu.Lock()
			if rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()

			// 封装 AppendEntries 参数
			rf.mu.Lock()
			entries := make([]LogEntry, 0)
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
				Entries:      entries,
				LeaderCommit: 0,
			}
			DPrintf("[%s]: Id %d Term %d State %s\t||\tissue AppendEntries RPC for index %d"+
				" to peer %d with commitIndex %d nextIndex %d\n", name, rf.me, rf.currentTerm, state2name(rf.state), index, i, commitIndex, commitIndex)
			rf.mu.Unlock()

			var reply AppendEntriesReply

			ok := rf.sendAppendEntries(i, &args, &reply)

			// 发送失败，表示无法建立通信，直接放弃
			if !ok {
				rf.mu.Lock()
				DPrintf("[%s]: Id %d Term %d State %s\t||\tiAppendEntries RPC for index %d"+
					" to peer %d Failed\n", name, rf.me, rf.currentTerm, state2name(rf.state), index, i)
				rf.mu.Unlock()
				return
			}

			// rf.mu.Lock()
			// if rf.currentTerm != args.Term {
			// 	rf.mu.Unlock()
			// }
			// rf.mu.Unlock()

			// 被拒绝可能有多种原因：1. leader任期过时 2. 一致性检查不通过
			if reply.Success == false {
				rf.mu.Lock()

				if args.Term < reply.Term {
					DPrintf("[%s]: Id %d Term %d State %s\t||\tiAppendEntries RPC for index %d"+
						" to peer %d Failed, newer Term is %d\n", name, rf.me, rf.currentTerm, state2name(rf.state), index, i, reply.Term)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.switchTo(FOLLOWER)
				}

				rf.mu.Unlock()
				return
			} else {
				// AppendEntries 发送成功
				DPrintf("[%s]: Id %d Term %d State %s\t||\tiAppendEntries RPC"+
					" to peer %d Success!\n", name, rf.me, rf.currentTerm, state2name(rf.state), i)
			}

		}(i, rf)

	}

	wg.Wait()

}

// 发送心跳 routine
func (rf *Raft) broadcastHeartbeat() {

	if _, isLeader := rf.GetState(); !isLeader {
		return
	}

	rf.mu.Lock()
	rf.latestHeartTime = time.Now()
	rf.mu.Unlock()

	rf.mu.Lock()
	index := len(rf.log) - 1
	nReplica := 1
	go rf.broadcastAppendEntries(index, rf.currentTerm, rf.commitIndex, nReplica, "Broadcast")
	rf.mu.Unlock()

}

func state2name(state int) string {
	var name string
	if state == FOLLOWER {
		name = "FOLLOWER"
	} else if state == CANDIDATE {
		name = "CANDIDATE"
	} else {
		name = "LEADER"
	}
	return name
}

func (rf *Raft) eventLoop() {

	for {
		select {
		case <-rf.electionTimeoutChan:
			rf.mu.Lock()
			DPrintf("[Eventloop]: Id %v Term %v State %s\t||\telection timeout, start an election\n", rf.me, rf.currentTerm, state2name(rf.state))
			rf.mu.Unlock()
			go rf.startElection()
		case <-rf.heartbeatPeriodChan:
			rf.mu.Lock()
			DPrintf("[Eventloop]: Id %v Term %v State %s\t||\theartbeat period occurs, broadcast heartbeats\n", rf.me, rf.currentTerm, state2name(rf.state))
			rf.mu.Unlock()
			go rf.broadcastHeartbeat()
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
	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.nonLeaderCond = sync.NewCond(&rf.mu)
	rf.heartbeatPeriod = 100

	//
	rf.votedFor = -1   // -1 代表没有给任何人投票
	rf.currentTerm = 0 // 初始化为 Term 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER // initilize to follower
	rf.leaderId = -1

	// log 中的有效元素从 1 开始算起
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0})

	// 重置选举超时计时器
	rf.resetElectionTimer()
	rf.electionTimeoutChan = make(chan bool)
	rf.heartbeatPeriodChan = make(chan bool)

	// 初始化 nextIndex[] 和 matchIndex[] 的大小
	size := len(rf.peers)
	rf.nextIndex = make([]int, size)
	rf.matchIndex = make([]int, size)

	go rf.electionTimeoutTick()
	go rf.heartbeatPeriodTick()
	go rf.eventLoop()

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

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
