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

import "sync"
import "sync/atomic"
import "../labrpc"
import "math/rand"
import "time"

// import "bytes"
// import "../labgob"

type ServerState string

const (
	Follower ServerState = "follower"
	Candidate = "candidate"
	Leader = "leader"
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
	// each entry contains command for state machine
	// term when entries was received by leader(从1开始)
	Command interface{}
	Index int
	Term int
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

	state ServerState
	timerStartTime time.Time

	// persistent state
	currentTerm int // 当前服务器能看到的latest term
	votedFor int // 当前服务器把票投给谁了，如果没投票就是null
	log []LogEntry // 存储log entries
	// volatile state
	commitIndex int // index最大的被commit的log entry
	lastApplied int // index最大的被apply到state machine(k/v store)的log entry
	// volatile state on leaders
	// 每次选举后都要重新初始化
	nextIndex []int // 对于每个peer server，下一个将被发送给它的log entry的索引。初始化为leader的最后一个log的index+1
	matchIndex[] int // 对于每一个peer server，最大的已经被replicated的log entry的索引。初始化为-1.

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
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
	Term int // candidate's term
	CandidateId int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm int // term of candidate's last log entry
	// see section 5.4.1 in paper
	// The RequestVote RPC实现了选举限制：他包含了candidate的log的信息，
	// 如果voter自己的log比candidate更新，那么它会投否决票
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	// 就是说，如果当前server的term比candidate大，那么说明candidate无法当选为leader，return false，并且返回最新的term
	// 如果当前server的votedFor是null，那么直接投票给他，return true
	Term int // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received
}


func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 就是说，如果当前server的term比candidate大，那么说明candidate无法当选为leader，return false，并且返回最新的term
	// 如果当前server的votedFor是null，那么直接投票给他，return true
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if (rf.currentTerm < args.Term) {
			// 进入新term了，要重新投票了
			rf.currentTerm = args.Term
			rf.votedFor = -1
			// 如果它是leader，要变回follower
			if rf.state == Leader {
				rf.state = Follower
				rf.currentTerm = args.Term
				rf.timerStartTime = time.Now()
			}
		}

		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			if len(rf.log) != 0 {
				if args.LastLogIndex != -1 {
					// 当前节点有log，并且candidate也有log
					// election restriction，保证选出来的leader包含所有的committed的logEntries。
					lastLog := rf.log[len(rf.log)-1]
					if args.LastLogTerm < lastLog.Term {
						reply.VoteGranted = false
						reply.Term = rf.currentTerm
					} else if args.LastLogTerm == lastLog.Term {
						if len(rf.log) > args.LastLogIndex + 1 {
							reply.VoteGranted = false
							reply.Term = rf.currentTerm
						}
					}
				}
			}
			if reply.VoteGranted {
				// If a Follower receives a RequestVote RPC, and if the Follower decides to grant its vote to that Candidate, the Follower will also reset its election timeout.
				DPrintf("%d投票给%d", rf.me,args.CandidateId)
				rf.timerStartTime = time.Now()
				DPrintf("%d的重置时间：%v", rf.me, rf.timerStartTime)
			}
		}
	}
}


func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term int // leader's term
	LeaderId int // so follower can redirect clients
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int // leader's commitIndex
}

type AppendEntriesReply struct {
	Term int // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// general heartbeat logic

	if rf.currentTerm > args.Term {
		// 让发送方知道现在最新的term：你那个任期早就结束了，大哥:/
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	DPrintf("Handle Heartbeat: Raft(%d), args.PrevLogIndex:%d, len(rf.log):%d", rf.me, args.PrevLogIndex, len(rf.log))
	DPrintf("Raft(%d) received the entries %v", rf.me, args.Entries)
	DPrintf("Raft(%d) received heartbeat arguments: %v", rf.me, args)
	DPrintf("raft(%d)'s log: %v, commitIndex: %d, lastApplied: %d", rf.me, rf.log, rf.commitIndex, rf.lastApplied)
	// you get an AppendEntries RPC from the current leader (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer)
	rf.timerStartTime = time.Now()
	if args.Term > rf.currentTerm {
		// 发现有比自己term更高的领导，Leader主动卸任，follower就更新一下自己的currentTerm
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.timerStartTime = time.Now()
	}
	// entries为空的时候，prevLogIndex可能是-1，也可能不是。
	// args>PrevLogIndex为-1的时候，肯定是正确的，因为这将相当于把所有的Leader's entries给它。
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		return
	}
	if args.PrevLogIndex != -1 {
		tempLog := rf.log[args.PrevLogIndex]
		if tempLog.Term != args.PrevLogTerm {
			reply.Success = false
			// truncate
			rf.log = rf.log[:args.PrevLogIndex]
			return
		}
	}
	reply.Success = true
	// Append
	if args.Entries != nil {
		DPrintf("Server: %d. before append: %v", rf.me, rf.log)
		// 可能PrevLogIndex和Term都相同，但是Follower后面的并没有被truncate，这里得确认一下。
		rf.log = rf.log[:args.PrevLogIndex+1] // 哈哈哈 TestFailNoAgree2B加上这一行就解决了
		rf.log = append(rf.log, args.Entries...)
		DPrintf("Server: %d. after append: %v %v", rf.me, rf.log, reply.Success)
	}
	// Update commitIndex
	if rf.commitIndex < args.LeaderCommit {
		if len(rf.log) - 1 <= args.LeaderCommit {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{true, rf.log[i].Command, i+1}
			rf.applyCh <- applyMsg
		}
		rf.lastApplied = rf.commitIndex
		DPrintf("Applied: raft(%d)'s log: %v, commitIndex: %d", rf.me, rf.log, rf.commitIndex)
	}
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = (rf.state == Leader)
	if isLeader {
		index = len(rf.log)
		newLog := LogEntry{command, index, term}
		DPrintf("Leader (%d) Start, %v", rf.me, newLog)
		rf.log = append(rf.log, newLog)
	}
	// 如果这个log被commit了，要通过`ApplyCh`通知KV Server
	return index+1, term, isLeader
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.timerStartTime = time.Now()

	rf.applyCh = applyCh

	// for log replication
	rf.commitIndex = -1
	rf.lastApplied = -1
	go rf.LeaderElection()
	DPrintf("Raft节点 %d 初始化", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

func (rf *Raft) LeaderElection() {
	// 如果在timeout时间内，没收到任何消息（Heartbeat或者Request Vote），就开始选举。
	maxElectionTimeout := 300
	minElectionTimeout := 150
	electionTimeout := rand.Intn(maxElectionTimeout-minElectionTimeout) + minElectionTimeout
	for {
		// 每10ms检查一下 0.0
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		now := time.Now()
		// 计算一下timer走了多少时间
		timerRunning := now.Sub(rf.timerStartTime)
		if timerRunning > time.Duration(electionTimeout) * time.Millisecond {
			// 如果是Leader的话，啥也不做，这个timeout和他已经没有关系了。
			if rf.state != Leader {
				DPrintf("%d不是Leader", rf.me)
				// 不是Leader的话， electionTimeout结束了要开始选举。
				// 更新一下electionTimeout为另一个随机值
				electionTimeout = rand.Intn(maxElectionTimeout-minElectionTimeout) + minElectionTimeout
				go rf.KickoffElection()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) KickoffElection() {
	rf.mu.Lock()
	DPrintf("%d 开始term: %d的选举", rf.me, rf.currentTerm+1)
	// 重设计时器！
	rf.timerStartTime = time.Now()
	// 身份转变为candidate了
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	// 获取该节点的最后一个log entry
	var lastLogEntry LogEntry
	if len(rf.log) == 0 {
		lastLogEntry = LogEntry{Index:-1, Term:-1}
	} else {
		lastLogEntry = rf.log[len(rf.log)-1]
	}
	args := RequestVoteArgs{rf.currentTerm, rf.me, lastLogEntry.Index, lastLogEntry.Term}
	// 当然要先给自己投一票了 :/
	numVote := 1
	rf.mu.Unlock()

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// 开始给其他节点发送RequestVote RPC
	for i := 0; i < len(rf.peers); i++ {
		// 初始化nextIndex和matchIndex
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
		if i == rf.me {
			// 不用给自己发送RequestVote
			continue
		}

		go func(p int) {
			reply := RequestVoteReply{}
			DPrintf("%d 发送 RequestVote 给 %d", rf.me, p)
			ok := rf.SendRequestVote(p, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				// 如果发现别人比自己的term更大，那么就会认识到自己根本不配做leader
				// 主动变回Follower
				rf.state = Follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.timerStartTime = time.Now()
				return
			}
			if reply.VoteGranted {
				numVote++
				if numVote > len(rf.peers) / 2 && rf.state == Candidate {
					// 拿到超过一半的选票了，成为Leader，开始给小弟们发送心跳
					rf.state = Leader
					DPrintf("%d 成为term: %d的Leader了！", rf.me, rf.currentTerm)	
					go rf.SendHeartBeats()
				}
			}
		}(i)
	}
}

func (rf *Raft) SendHeartBeats() {
	DPrintf("Leader: %d 开始发心跳", rf.me)
	for {
		DPrintf("Leader（%d）'s log: %v, commitIndex: %d", rf.me, rf.log, rf.commitIndex)

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			// 发送心跳，如果reply.Term > currentTerm，要变回follower
			go rf.HeartBeat(i)

		}
		// 发送心跳的间隔时间
		time.Sleep(time.Duration(100)*time.Millisecond)

		// 检查一下有没有可以commit的数据
		rf.mu.Lock()


		for N := len(rf.log) - 1; N > rf.commitIndex && rf.log[N].Term == rf.currentTerm; N-- {
			cnt := 1 // leader肯定自身有这个log
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N {
					cnt++
				}
			}
			if cnt > len(rf.peers) / 2 {
				rf.commitIndex = N
				DPrintf("Leader commit : %d", N)
				break
			}
		}
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				applyMsg := ApplyMsg{true, rf.log[i].Command, i+1}
				rf.applyCh <- applyMsg
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.mu.Unlock()

	}

}

func (rf *Raft) HeartBeat(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		// 多check一下总是好的，如果不是Leader了就不能发心跳了。
		return
	}
	// 根据nextIndex[server]求出PrevLogIndex, PrevLogTerm, Entries
	DPrintf("len(rf.nextIndex):%d, server:%d", len(rf.nextIndex), server)
	DPrintf("Leader: %d, nextIndex: %v", rf.me, rf.nextIndex)
	next := rf.nextIndex[server]
	var entries []LogEntry
	var prevLogEntry LogEntry

	if next > 0 {
		prevLogEntry = rf.log[next-1]
	} else if next == 0 {
		prevLogEntry = LogEntry{Index:-1, Term:-1}
	}
	entries = rf.log[next:]

	args := AppendEntriesArgs {
		Term: rf.currentTerm, // 当前任期
		LeaderId: rf.me, // LeaderId
		PrevLogIndex: prevLogEntry.Index,
		PrevLogTerm: prevLogEntry.Term,
		Entries: entries, // entries和leaderCommit得根据真实情况来，通过nextIndex[]数组来得出Entries的值。
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	DPrintf("%d 发心跳给 %d", rf.me, server)
	ok := rf.SendAppendEntries(server, &args, &reply)
	// Note：注意发送心跳后，PrevLogIndex和PrevLogTerm可能已经修改了
	// 所以不能通过这些值来update nextIndex[]和matchIdnex[]
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// 你以为自己是Leader，但发现别人的term比你大，别自以为是了，你只能变回follower了哦：）
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.timerStartTime = time.Now()
		return
	}
	if reply.Success {
		// 如果成功的话，更新nextIndex和matchIndex
		rf.matchIndex[server] = prevLogEntry.Index + len(entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		DPrintf("SUCCESS：server:%v; %v, %v, matchIndex: %v, nextIndex: %v", server, prevLogEntry.Index, len(entries), rf.matchIndex, rf.nextIndex)
	} else {
		// 失败的话，decrement nextIndex and retry
		rf.nextIndex[server]--
		return
	}
}