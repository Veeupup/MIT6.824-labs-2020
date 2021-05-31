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
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{} // 实际的数据，采用字节数组保存
}

const (
	FOLLOWER  = 0
	CANDICATE = 1
	LEADER    = 2
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
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	// volatile state
	state       int
	commitIndex int
	lastApplied int
	leaderId    int

	// volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// others
	lastHeartbeatTime time.Time
	electionTimeout   int // rand
	heartbeatPeriod   int

	electionTimeoutChan  chan bool // true 代表可以进行选举
	heartbeatTimeoutChan chan bool // true 代表可以发送心跳

	leaderCon    *sync.Cond
	nonleaderCon *sync.Cond

	baseElectionTimeout int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = rf.state == LEADER
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.CurrentTerm || (args.Term == rf.CurrentTerm && rf.VotedFor == -1) {
		// FIXME(tw) 是否只需要比较日志的新旧程度即可，应该是
		lastLogIndex := len(rf.Log) - 1
		lastLogTerm := rf.Log[lastLogIndex].Term

		if lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
			DPrintf("%d-%s-term:%d [RequestVote] give vote to %d(lastLogTerm: %d, lastLogIndex: %d) in term %d, lastLogTerm: %d, lastLogIndex: %d",
				rf.me, state2Str(rf.state), rf.CurrentTerm, args.CandidateId, args.LastLogTerm, args.LastLogIndex, args.Term, lastLogTerm, lastLogIndex)
			rf.CurrentTerm = args.Term
			rf.VotedFor = args.CandidateId
			rf.turnToState(FOLLOWER)
			rf.resetElectionTimeout()
			rf.leaderId = args.CandidateId
			reply.VoteGranted = true
			reply.Term = args.Term
			return
		}
	}

	DPrintf("%d-%s-term:%d [RequestVote] refused give vote to %d in term %d, voted for %d",
		rf.me, state2Str(rf.state), rf.CurrentTerm, args.CandidateId, args.Term, rf.VotedFor)
	reply.VoteGranted = false
	reply.Term = rf.CurrentTerm
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// TODO(tw) 对于日志不一致情况的处理，需要返回 false，
	// 如果日志在某个点达成一致，之后的日志删除掉并且增加新的日志
	if args.Term >= rf.CurrentTerm {

		if args.PrevLogIndex < len(rf.Log) && rf.Log[args.PrevLogIndex].Term == args.PrevLogTerm { // 符合一致性检查，应该从一致开始的位置覆盖为 leader 发来的日志
			if args.LeaderCommit > rf.commitIndex { // 不能够放在外面，因为心跳不一定能够成功，如果先把 commitIndex 改了，那么可能提交一些非法的 log
				if args.LeaderCommit < len(rf.Log)-1 {
					rf.commitIndex = args.LeaderCommit
				} else {
					rf.commitIndex = len(rf.Log) - 1
				}
			}
			reply.Success = true
			rf.Log = rf.Log[:args.PrevLogIndex+1]
			rf.Log = append(rf.Log, args.Entries...)
			if len(args.Entries) > 0 {
				DPrintf("%d-%s-term:%d [AppendEntries] receive entries from %d success, commitIndex: %d, logs: %v",
					rf.me, state2Str(rf.state), rf.CurrentTerm, args.LeaderId, rf.commitIndex, rf.Log)
			}
		} else {
			reply.Success = false
			DPrintf("%d-%s-term:%d [AppendEntries] receive entries from %d failed, consistency check failed",
				rf.me, state2Str(rf.state), rf.CurrentTerm, args.LeaderId)
		}

		rf.CurrentTerm = args.Term
		rf.leaderId = args.LeaderId
		rf.resetElectionTimeout()
		rf.turnToState(FOLLOWER)

		reply.Term = rf.CurrentTerm

		if len(args.Entries) == 0 {
			DPrintf("%d-%s-term:%d [AppendEntries] receive heartbeat from %d success, commitIndex: %d",
				rf.me, state2Str(rf.state), rf.CurrentTerm, args.LeaderId, rf.commitIndex)
		}

		return
	}
	DPrintf("%d-%s-term:%d [AppendEntries] from %d-term:%d failed",
		rf.me, state2Str(rf.state), rf.CurrentTerm, args.LeaderId, args.Term)
	reply.Success = false
	reply.Term = rf.CurrentTerm
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func state2Str(state int) string {
	if state == FOLLOWER {
		return "FOLLOWER"
	} else if state == CANDICATE {
		return "CANDIDATE"
	} else if state == LEADER {
		return "LEADER"
	}
	return "UNKNDOWN"
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

	if rf.killed() {
		rf.mu.Lock()
		index = len(rf.Log)
		rf.mu.Unlock()
		term, isLeader = rf.GetState()
		return index, term, isLeader
	}

	// Your code here (2B).
	if term, isLeader = rf.GetState(); !isLeader {
		return index, term, isLeader
	}
	rf.mu.Lock()
	index = len(rf.Log)
	rf.Log = append(rf.Log, LogEntry{Term: rf.CurrentTerm, Command: command})
	rf.mu.Unlock()

	// TODO(tw) agreement
	go func(index, term int, command interface{}, rf *Raft) {
		var wg sync.WaitGroup
		winThreshold := len(rf.peers) / 2

		rf.mu.Lock()
		// rf.Log = append(rf.Log, LogEntry{Term: rf.CurrentTerm, Command: command})
		nReplica := 1
		DPrintf("%d-%s-term:%d [Start] logs: %v",
			rf.me, state2Str(rf.state), rf.CurrentTerm, rf.Log)
		rf.mu.Unlock()

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			wg.Add(1)
			// TODO(tw) 发送 AppendEntries，并且进行重试
			go func(server, winThreshold, index, term int, nReplica *int, command interface{}, rf *Raft) {
				defer wg.Done()

				rf.mu.Lock()
				prevLogIndex := index - 1
				entries := []LogEntry{{Term: term, Command: command}}
				args := AppendEntriesArgs{Term: term, LeaderId: rf.me, PrevLogIndex: prevLogIndex,
					PrevLogTerm: rf.Log[prevLogIndex].Term, Entries: entries, LeaderCommit: rf.commitIndex}
				rf.mu.Unlock()
				var reply AppendEntriesReply
				for {
					rf.mu.Lock()
					DPrintf("%d-%s-term:%d [AppendEntries] send to %d, prevLogIndex: %d, prevLogTerm: %d",
						rf.me, state2Str(rf.state), rf.CurrentTerm, server, args.PrevLogIndex, args.PrevLogTerm)
					rf.mu.Unlock()
					ok := rf.sendAppendEntries(server, &args, &reply)

					if ok {
						if reply.Success {
							// TODO(tw)
							// 增加 nReplica 的数量，并且判断自己还是不是之前的那个少年，
							// 然后判断是否能够标记为 commit
							rf.mu.Lock()
							if args.Term == rf.CurrentTerm && rf.state == LEADER {
								*nReplica++
								DPrintf("%d-%s-term:%d [AppendEntries] send to %d success, entry index: %d, entry term: %d",
									rf.me, state2Str(rf.state), rf.CurrentTerm, server, index, term)
								if *nReplica > winThreshold {
									DPrintf("%d-%s-term:%d [AppendEntries] log index: %d, log len: %d",
										rf.me, state2Str(rf.state), rf.CurrentTerm, index, len(rf.Log)-1)
									if index > rf.commitIndex {
										rf.commitIndex = index
									}
								}
							}
							rf.mu.Unlock()
							return
						} else {

							// TODO
							// 不成功有两种原因
							// 1. 自己的 term 太小，跟不上别人了
							// 2. 日志的一致性检查失败了，减少 prevLogIndex 并且重新进行一致性检查
							rf.mu.Lock()
							if args.Term == rf.CurrentTerm && rf.state == LEADER {
								if reply.Term > rf.CurrentTerm {
									rf.CurrentTerm = reply.Term
									rf.resetElectionTimeout()
									rf.turnToState(FOLLOWER)
									rf.VotedFor = -1
									DPrintf("%d-%s-term:%d [AppendEntries] failed, server %d term is %d, logs: %v",
										rf.me, state2Str(rf.state), rf.CurrentTerm, server, reply.Term, args.Entries)
									rf.mu.Unlock()
									return
								} else {
									confilctTerm := args.Term
									idx := args.PrevLogIndex - 1
									for ; idx > 0 && rf.Log[idx].Term == confilctTerm; idx-- {
									}
									prevLogIndex := idx
									// prevLogIndex := args.PrevLogIndex - 1
									prevLogTerm := rf.Log[prevLogIndex].Term
									entries = rf.Log[prevLogIndex+1:]
									args.Entries = entries
									args.PrevLogIndex = prevLogIndex
									args.PrevLogTerm = prevLogTerm
									DPrintf("%d-%s-term:%d [AppendEntries] retry, send to %d, prevLogIndex: %d, preLogTerm: %d, logs: %v",
										rf.me, state2Str(rf.state), rf.CurrentTerm, server, prevLogIndex, prevLogTerm, entries)
								}
							} else {
								rf.mu.Unlock()
								return
							}
							rf.mu.Unlock()
						}

					} else {
						// TODO(tw) 不ok说明网络有问题，进行重试
						rf.mu.Lock()
						if args.Term == rf.CurrentTerm && rf.state == LEADER {
							rf.mu.Unlock()
							continue
						}
						rf.mu.Unlock()
						return
					}
				}

			}(i, winThreshold, index, term, &nReplica, command, rf)
		}

		wg.Wait()
	}(index, term, command, rf)

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

func (rf *Raft) turnToState(toState int) {
	oldState := rf.state
	rf.state = toState
	if oldState == LEADER && toState == FOLLOWER {
		rf.nonleaderCon.Broadcast()
	} else if oldState == CANDICATE && toState == LEADER {
		rf.leaderCon.Broadcast()
	}
}

func (rf *Raft) startElection() {
	// state to candiate
	rf.mu.Lock()
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.resetElectionTimeout()
	rf.turnToState(CANDICATE)
	nVotes := 1
	rf.mu.Unlock()

	go func(nVotes *int, rf *Raft) {
		var wg sync.WaitGroup
		winThreshold := len(rf.peers) / 2

		for i, _ := range rf.peers {
			if i == rf.me {
				continue
			}
			wg.Add(1)

			rf.mu.Lock()
			lastLogIndex := len(rf.Log) - 1
			args := RequestVoteArgs{Term: rf.CurrentTerm, CandidateId: rf.me,
				LastLogIndex: lastLogIndex, LastLogTerm: rf.Log[lastLogIndex].Term}
			rf.mu.Unlock()
			var reply RequestVoteReply
			// DPrintf("%d-%s-term:%d [start election] lastLogTerm: %d, lastLogIndex: %d",
			// rf.me, state2Str(rf.state), rf.CurrentTerm, args.LastLogTerm, args.LastLogIndex)
			go func(i int, nVotes *int, winThreshold int, args *RequestVoteArgs, reply *RequestVoteReply, rf *Raft) {
				defer wg.Done()

				ok := rf.sendRequestVote(i, args, reply)

				if !ok {
					rf.mu.Lock()
					DPrintf("%d-%s-term:%d [RequestVote] rpc failed",
						rf.me, state2Str(rf.state), rf.CurrentTerm)
					rf.mu.Unlock()
				} else {
					if reply.VoteGranted {
						rf.mu.Lock()
						if rf.CurrentTerm == args.Term && rf.state == CANDICATE { // 还是之前的那个状态，term 没变同时仍然是 candidate
							*nVotes++
							if *nVotes > winThreshold {
								// TODO(tw) 需要初始化 nextIndex[] 数组，matchIndex[] 数组
								rf.VotedFor = -1
								rf.leaderId = rf.me
								rf.turnToState(LEADER)
								for i, _ := range rf.nextIndex {
									rf.nextIndex[i] = len(rf.Log)
									rf.matchIndex[i] = 0
								}
								DPrintf("%d-%s-term:%d [Win Election]",
									rf.me, state2Str(rf.state), rf.CurrentTerm)
								go rf.sendHeartbeat() // 立即发布心跳
							}
						}
						rf.mu.Unlock()
					} else {
						rf.mu.Lock()
						if reply.Term > rf.CurrentTerm { // 现在已经不是当初那个少年，而且不能
							rf.CurrentTerm = reply.Term
							rf.VotedFor = -1
							rf.turnToState(FOLLOWER)
						}
						rf.mu.Unlock()
					}
				}

			}(i, nVotes, winThreshold, &args, &reply, rf)
		}
		wg.Wait()
	}(&nVotes, rf)
}

func (rf *Raft) sendHeartbeat() {

	if _, isleader := rf.GetState(); !isleader {
		return
	}

	DPrintf("%d-%s-term:%d [sendHeartbeat] leader ready to send heartbeat",
		rf.me, state2Str(rf.state), rf.CurrentTerm)
	var wg sync.WaitGroup

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)

		rf.mu.Lock()
		prevLogIndex := len(rf.Log) - 1
		args := AppendEntriesArgs{Term: rf.CurrentTerm, LeaderId: rf.me, PrevLogIndex: prevLogIndex,
			PrevLogTerm: rf.Log[prevLogIndex].Term, Entries: []LogEntry{}, LeaderCommit: rf.commitIndex}
		rf.mu.Unlock()
		var reply AppendEntriesReply

		go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, rf *Raft) {
			wg.Done()

			for {
				ok := rf.sendAppendEntries(server, args, reply)

				if !ok {
					rf.mu.Lock()
					DPrintf("%d-%s-term:%d [AppendEntries] heartbeat rpc failed %d failed ",
						rf.me, state2Str(rf.state), rf.CurrentTerm, server)
					rf.mu.Unlock()
					return
				}

				// TODO(tw) 需要增加对于失败的处理，也许发送心跳的时候发现别人的 term 比自己的更大，那么自己就会转变成 follower
				// 如果在 AppendEntries 的时候发现日志不一致，那么应该如何处理？在这里也重新发送新的日志？是不是发送的频率太高了，
				// 因为过一会 heartbeat timeout 的时候也需要进行检查

				if reply.Success {
					return
				}

				if reply.Term > args.Term {
					rf.mu.Lock()
					rf.CurrentTerm = reply.Term
					rf.turnToState(FOLLOWER)
					rf.VotedFor = -1
					rf.resetElectionTimeout()
					rf.mu.Unlock()
					return
				}

				rf.mu.Lock()
				if rf.CurrentTerm == args.Term && rf.state == LEADER {
					// TODO(tw) 优化，每次退回一个 term 的内容
					confilctTerm := args.Term
					idx := args.PrevLogIndex
					for ; idx > 0 && rf.Log[idx].Term == confilctTerm; idx-- {
					}
					args.PrevLogIndex = idx
					args.PrevLogTerm = rf.Log[idx].Term
					args.Entries = rf.Log[idx+1:]
					DPrintf("%d-%s-term:%d [HeartBeat] to %d failed, retry with logs: %v",
						rf.me, state2Str(rf.state), rf.CurrentTerm, server, args.Entries)
				} else {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

			}

		}(i, &args, &reply, rf)
	}
	wg.Wait()
}

func (rf *Raft) eventLoop() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimeoutChan:
			rf.mu.Lock()
			DPrintf("%d-%s-term:%d [ElectionTimeout]",
				rf.me, state2Str(rf.state), rf.CurrentTerm)
			rf.mu.Unlock()
			go rf.startElection()
		case <-rf.heartbeatTimeoutChan:
			rf.mu.Lock()
			DPrintf("%d-%s-term:%d [Heartbeat Timeout]",
				rf.me, state2Str(rf.state), rf.CurrentTerm)
			rf.mu.Unlock()
			go rf.sendHeartbeat()
		}
	}
}

func (rf *Raft) electionTimeoutTicker() {
	for rf.killed() == false {
		if _, isleader := rf.GetState(); isleader {
			rf.mu.Lock()
			rf.nonleaderCon.Wait()
			rf.mu.Unlock()
		} else {
			rf.mu.Lock()
			if time.Since(rf.lastHeartbeatTime) >= time.Millisecond*time.Duration(rf.electionTimeout) {
				rf.electionTimeoutChan <- true
			}
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (rf *Raft) heartbeatTimeoutTicker() {
	for rf.killed() == false {
		if _, isleader := rf.GetState(); isleader {
			rf.mu.Lock()
			rf.heartbeatTimeoutChan <- true
			rf.mu.Unlock()
			time.Sleep(time.Millisecond * time.Duration(rf.heartbeatPeriod))
		} else {
			rf.mu.Lock()
			rf.leaderCon.Wait()
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) checkAppliedLogTicker(applyCh chan ApplyMsg) {
	for {
		rf.mu.Lock()
		if rf.commitIndex > len(rf.Log)-1 {
			rf.commitIndex = len(rf.Log) - 1
		}
		if rf.lastApplied < rf.commitIndex {
			for idx := rf.lastApplied + 1; idx <= rf.commitIndex; idx++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.Log[idx].Command,
					CommandIndex: idx, // 其实这个时候是从 1 开始计数的，但是给测试表现成 0 开始的
				}
				applyCh <- applyMsg
				rf.lastApplied++
				DPrintf("%d-%s-term:%d [ApplyMsg] index: %d, term: %d, lastAppliedIndex: %d, msg: %v",
					rf.me, state2Str(rf.state), rf.CurrentTerm, idx, rf.Log[idx].Term, rf.lastApplied, rf.Log[idx].Command)
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 20)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

// reset electionTimeout
func (rf *Raft) resetElectionTimeout() {
	rand.Seed(time.Now().UnixNano())
	rf.lastHeartbeatTime = time.Now()
	rf.electionTimeout = rf.baseElectionTimeout + rand.Intn(150)
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
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]LogEntry, 0)
	rf.Log = append(rf.Log, LogEntry{Term: 0}) // 有效的 term 从下标 1 开始

	// volatile state
	atomic.StoreInt32(&rf.dead, 0)
	rf.state = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.leaderId = -1

	// volatile state on leader
	size := len(peers)
	rf.nextIndex = make([]int, size)
	rf.matchIndex = make([]int, size)

	// others
	rf.heartbeatPeriod = 100
	rf.resetElectionTimeout()

	rf.electionTimeoutChan = make(chan bool)
	rf.heartbeatTimeoutChan = make(chan bool)

	rf.leaderCon = sync.NewCond(&rf.mu)
	rf.nonleaderCon = sync.NewCond(&rf.mu)

	rf.baseElectionTimeout = 150

	// long running goroutines
	go rf.eventLoop()
	go rf.electionTimeoutTicker()
	go rf.heartbeatTimeoutTicker()
	go rf.checkAppliedLogTicker(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()

	return rf
}
