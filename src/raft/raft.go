package raft

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

func Min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func Max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot []byte
	SnapshotTerm int
	SnapshotIndex int
	SnapshotSeq int
}

type State int8

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Valid   bool
	Term    int
	Index   int
	Seq     int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state           State
	term            int
	votedFor        int
	log             []*LogEntry
	lastHeartbeat   time.Time
	electionTimeout time.Duration

	commitIndex   int
	lastApplied   int
	applyCond     *sync.Cond
	applyCh       chan ApplyMsg
	nextIndex     []int
	matchIndex    []int
	replicateCond []*sync.Cond

	leaseEndAt time.Time
	leaseSyncing bool
}

func (rf *Raft) IsMajority(vote int) bool {
	return vote >= rf.Majority()
}

func (rf *Raft) Majority() int {
	return len(rf.peers)/2 + 1
}

func (rf *Raft) GetLogAtIndex(logIndex int) *LogEntry {
	if logIndex < rf.log[0].Index {
		return nil
	}
	subscript := rf.LogIndexToSubscript(logIndex)
	if len(rf.log) > subscript {
		return rf.log[subscript]
	} else {
		return nil
	}
}

func (rf *Raft) LogIndexToSubscript(logIndex int) int {
	return logIndex - rf.log[0].Index
}

func (rf *Raft) LogTail() *LogEntry {
	return LogTail(rf.log)
}

func LogTail(xs []*LogEntry) *LogEntry {
	return xs[len(xs)-1]
}

func (rf *Raft) resetTerm(term int) {
	rf.term = term
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) becomeFollower() {
	rf.state = Follower
	rf.leaseSyncing = false
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.leaseSyncing = false
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.serializeState())
}

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.term
	isleader = rf.state == Leader && !rf.leaseSyncing && time.Now().Before(rf.leaseEndAt)
	return term, isleader
}

func (rf *Raft) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.term)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []*LogEntry
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if e := d.Decode(&term); e != nil {
		panic(e)
	}
	if e := d.Decode(&votedFor); e != nil {
		panic(e)
	}
	if e := d.Decode(&log); e != nil {
		panic(e)
	}

	rf.term = term
	rf.votedFor = votedFor
	rf.log = log
	rf.commitIndex = rf.log[0].Index
	rf.lastApplied = rf.log[0].Index
}

/* ==================== Snapshotting Start ========================= */
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, lastIncludedSeq int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex < rf.commitIndex {
		return false
	}

	if lastIncludedIndex >= rf.LogTail().Index {
		rf.log = rf.log[0:1]
	} else {
		rf.log = rf.log[rf.LogIndexToSubscript(lastIncludedIndex):]
	}
	rf.log[0].Index = lastIncludedIndex
	rf.log[0].Term = lastIncludedTerm
	rf.log[0].Seq = lastIncludedSeq
	rf.log[0].Command = nil
	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.serializeState(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(seq int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := len(rf.log) - 1; i >= 0; i-- {
		entry := rf.log[i]
		if entry.Seq == seq {
			rf.log = rf.log[rf.LogIndexToSubscript(entry.Index):]
			rf.log[0].Command = nil
			rf.lastApplied = entry.Index
			rf.commitIndex = Max(rf.commitIndex, entry.Index)
			if rf.leaseSyncing && rf.lastApplied >= rf.commitIndex {
				rf.leaseSyncing = false
			}
			rf.persister.SaveStateAndSnapshot(rf.serializeState(), snapshot)
			return
		}
	}
}
/* ==================== Snapshotting End ========================= */



/* ==================== Replication Start ========================= */
func (rf *Raft) DoReplicate(peer int) {
	rf.replicateCond[peer].L.Lock()
	defer rf.replicateCond[peer].L.Unlock()
	for {
		// rf.Debug(dWarn, "DoReplicate: try to acquire replicateCond[%d].L", peer)
		// rf.Debug(dWarn, "DoReplicate: acquired replicateCond[%d].L", peer)
		for !rf.needReplicate(peer) {
			rf.replicateCond[peer].Wait()
			if rf.killed() {
				return
			}
		}

		rf.Replicate(peer, rf.lease())
	}
}

func (rf *Raft) needReplicate(peer int) bool {
	// rf.Debug(dWarn, "needReplicate: try to acquire mu.Lock")
	rf.mu.Lock()
	// rf.Debug(dWarn, "needReplicate: acquired mu.Lock")
	defer rf.mu.Unlock()
	nextIndex := rf.nextIndex[peer]
	return rf.state == Leader && peer != rf.me && rf.GetLogAtIndex(nextIndex) != nil && rf.LogTail().Index > nextIndex
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term             int
	Success          bool
	ConflictingTerm  int
	ConflictingIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	now := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	if args.Term < rf.term {
		reply.Success = false
		return
	}
	rf.lastHeartbeat = now

	if rf.state == Candidate && args.Term >= rf.term {
		rf.becomeFollower()
		rf.resetTerm(args.Term)
	} else if rf.state == Follower && args.Term > rf.term {
		rf.resetTerm(args.Term)
	} else if rf.state == Leader && args.Term > rf.term {
		rf.becomeFollower()
		rf.resetTerm(args.Term)
	}

	if args.PrevLogIndex > 0 && args.PrevLogIndex >= rf.log[0].Index {
		if prev := rf.GetLogAtIndex(args.PrevLogIndex); prev == nil || prev.Term != args.PrevLogTerm {
			if prev != nil {
				for _, entry := range rf.log {
					if entry.Term == prev.Term {
						reply.ConflictingIndex = entry.Index
						break
					}
				}
				reply.ConflictingTerm = prev.Term
			} else {
				reply.ConflictingIndex = rf.LogTail().Index
				reply.ConflictingTerm = 0
			}
			reply.Success = false

			return
		}
	}
	if len(args.Entries) > 0 {
		// if pass log consistency check, do merge
		if LogTail(args.Entries).Index >= rf.log[0].Index {
			appendLeft := 0
			for i, entry := range args.Entries {
				if local := rf.GetLogAtIndex(entry.Index); local != nil {
					if local.Index != entry.Index {
					}
					if local.Term != entry.Term {
						rf.log = rf.log[:rf.LogIndexToSubscript(entry.Index)]
						appendLeft = i
						break
					}
					appendLeft = i + 1
				}
			}
			for i := appendLeft; i < len(args.Entries); i++ {
				entry := *args.Entries[i]
				rf.log = append(rf.log, &entry)
			}
		}
		rf.persist()
	}
	// trigger apply
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.LogTail().Index)
		rf.applyCond.Broadcast()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) DoApply(applyCh chan ApplyMsg) {
	rf.applyCond.L.Lock()
	defer rf.applyCond.L.Unlock()
	for {
		for !rf.needApply() {
			rf.applyCond.Wait()
			if rf.killed() {
				return
			}
		}

		rf.mu.Lock()
		if !rf.needApplyL() {
			rf.mu.Unlock()
			continue
		}
		rf.lastApplied += 1
		if rf.leaseSyncing && rf.lastApplied >= rf.commitIndex {
			rf.leaseSyncing = false
		}
		entry := rf.GetLogAtIndex(rf.lastApplied)
		if entry == nil {
		}
		toCommit := *entry
		rf.mu.Unlock()

		applyCh <- ApplyMsg{
			Command:      toCommit.Command,
			CommandValid: toCommit.Valid,
			CommandIndex: toCommit.Seq,
		}
	}
}

func (rf *Raft) needApply() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.needApplyL()
}

func (rf *Raft) needApplyL() bool {
	return rf.commitIndex > rf.lastApplied && (rf.state != Leader || time.Now().Before(rf.leaseEndAt))
}
/* ==================== Replication End ========================= */

/* ==================== Heartbeat Start (Snapshotting)========================= */
func (rf *Raft) needHeartbeat() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == Leader
}

func (rf *Raft) DoHeartbeat() {
	for !rf.killed() {
		time.Sleep(HeartbeatInterval)
		if !rf.needHeartbeat() {
			continue
		}

		rf.mu.Lock()
		rf.BroadcastHeartbeat()
		rf.mu.Unlock()
	}
}

type Lease struct {
	leaseVote int
	leaseEndAt time.Time
}

func (rf *Raft) lease() *Lease {
	return &Lease{1, time.Now().Add(LeaseDuration)}
}

func (rf *Raft) BroadcastHeartbeat() {
	rf.mu.Unlock()
	lease := rf.lease()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		// lease的时间小于下一次发heartbeat timeout
		go rf.Replicate(i, lease)
	}

	rf.mu.Lock()
}
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedSeq   int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) Replicate(peer int, lease *Lease) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[peer] <= rf.log[0].Index {
		// 如果peer的nextIndex小于等于当前log列表中的最小Index，说明数据在快照里，所以得恢复一下。
		var installReply InstallSnapshotReply
		installArgs := &InstallSnapshotArgs{
			Term: rf.term,
			LeaderId: rf.me,
			LastIncludedIndex: rf.log[0].Index,
			LastIncludedSeq: rf.log[0].Seq,
			LastIncludedTerm: rf.log[0].Term,
			Data: rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()

		installOk := rf.sendInstallSnapshot(peer, installArgs, &installReply)
		if !installOk {
			return
		}

		rf.mu.Lock()
		if rf.term != installArgs.Term {
			rf.mu.Unlock()
			return
		}
		if installReply.Term > rf.term {
			rf.becomeFollower()
			rf.resetTerm(installReply.Term)
			rf.mu.Unlock()
			return
		}
		rf.nextIndex[peer] = installArgs.LastIncludedIndex + 1
		rf.mu.Unlock()
		return
	}

	var entries []*LogEntry
	nextIndex := rf.nextIndex[peer]
	for j := nextIndex; j <= rf.LogTail().Index; j++ {
		atIndex := rf.GetLogAtIndex(j)
		entry := *atIndex
		entries = append(entries, &entry)
	}
	prev := rf.GetLogAtIndex(nextIndex - 1)
	args := &AppendEntriesArgs{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: prev.Index,
		PrevLogTerm:  prev.Term,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}
	rf.mu.Unlock()

	rf.Sync(peer, args, lease)
}

func (rf *Raft) Sync(peer int, args *AppendEntriesArgs, lease *Lease) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(peer, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.term != args.Term {
		return
	}
	if reply.Term > rf.term {
		rf.becomeFollower()
		rf.resetTerm(reply.Term)
	} else {
		if reply.Success {
			lease.leaseVote++
			if rf.IsMajority(lease.leaseVote) {
				if rf.leaseEndAt.Before(lease.leaseEndAt) {
					// raft获取一个比election timeout小的租期，这时候才被确认是Leader。
					rf.leaseEndAt = lease.leaseEndAt
				}
			}
			if len(args.Entries) == 0 {
				rf.applyCond.Broadcast()
				return
			}
			logTailIndex := LogTail(args.Entries).Index
			rf.matchIndex[peer] = logTailIndex
			rf.nextIndex[peer] = logTailIndex + 1

			// update commitIndex
			preCommitIndex := rf.commitIndex
			for i := rf.commitIndex; i <= logTailIndex; i++ {
				count := 0
				for p := range rf.peers {
					if rf.matchIndex[p] >= i {
						count += 1
					}
				}
				if rf.IsMajority(count) && rf.GetLogAtIndex(i).Term == rf.term {
					preCommitIndex = i
				}
			}
			rf.commitIndex = preCommitIndex

			// trigger DoApply
			if rf.commitIndex > rf.lastApplied {
				rf.applyCond.Broadcast()
			}
		} else {
			if reply.Term < rf.term {
				return
			}
			nextIndex := rf.nextIndex[peer]
			rf.matchIndex[peer] = 0

			if reply.ConflictingTerm > 0 {
				for i := len(rf.log) - 1; i >= 1; i-- {
					if rf.log[i].Term == reply.ConflictingTerm {
						rf.nextIndex[peer] = Min(nextIndex, rf.log[i].Index+1)
						return
					}
				}
			}
			rf.nextIndex[peer] = Max(Min(nextIndex, reply.ConflictingIndex), 1)
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	now := time.Now()
	rf.mu.Lock()
	reply.Term = rf.term
	if args.Term < rf.term {
		rf.mu.Unlock()
		return
	}
	rf.lastHeartbeat = now

	if rf.state == Candidate && args.Term >= rf.term {
		rf.becomeFollower()
		rf.resetTerm(args.Term)
	} else if rf.state == Follower && args.Term > rf.term {
		rf.resetTerm(args.Term)
	} else if rf.state == Leader && args.Term > rf.term {
		rf.becomeFollower()
		rf.resetTerm(args.Term)
	}
	rf.mu.Unlock()

	go func() {
		rf.applyCh <- ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
			SnapshotSeq:   args.LastIncludedSeq,
		}
	}()
}
/* ==================== Heartbeat End ========================= */


/* ==================== Leader Election Start ========================= */
const (
	ElectionTimeoutMax = int64(600 * time.Millisecond)
	ElectionTimeoutMin = int64(500 * time.Millisecond)
	HeartbeatInterval  = 100 * time.Millisecond
	LeaseDuration      = 400 * time.Millisecond
)

func NextElectionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(rand.Int63n(ElectionTimeoutMax-ElectionTimeoutMin) + ElectionTimeoutMin) // already Millisecond
}
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) DoElection() {
	for !rf.killed() {
		time.Sleep(rf.electionTimeout)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			continue
		}
		if time.Since(rf.lastHeartbeat) >= rf.electionTimeout {
			rf.electionTimeout = NextElectionTimeout()
			rf.resetTerm(rf.term + 1)
			rf.becomeCandidate()
			rf.votedFor = rf.me
			rf.persist()

			args := &RequestVoteArgs{
				Term:         rf.term,
				CandidateId:  rf.me,
				LastLogIndex: rf.LogTail().Index,
				LastLogTerm:  rf.LogTail().Term,
			}
			rf.mu.Unlock()

			vote := uint32(1)
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go func(i int, args *RequestVoteArgs) {
					var reply RequestVoteReply
					ok := rf.sendRequestVote(i, args, &reply)
					if !ok {
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.state != Candidate || rf.term != args.Term {
						return
					}
					if reply.Term > rf.term {
						rf.becomeFollower()
						rf.resetTerm(reply.Term)
						return
					}
					if reply.VoteGranted {
						vote += 1

						if rf.IsMajority(int(vote)) {
							for i = 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = rf.LogTail().Index + 1
								rf.matchIndex[i] = 0
							}
							rf.matchIndex[rf.me] = rf.LogTail().Index
							rf.becomeLeader()
							rf.leaseSyncing = true

							// 为了确定Leader中的哪些log已经被committed
							// Raft handles this by having each leader commit a blank no-op entry into the log at the start of its term.
							rf.StartL(LogEntry{
								Valid: false,
								Term:  rf.term,
								Index: rf.LogTail().Index + 1,
								Seq:   rf.LogTail().Seq,
							})
						}
					}
				}(i, args)
			}
			rf.mu.Lock()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	now := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	if args.Term < rf.term {
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.term {
		rf.becomeFollower()
		rf.resetTerm(args.Term)
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isAtLeastUpToDate(args) {
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.lastHeartbeat = now
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) isAtLeastUpToDate(args *RequestVoteArgs) bool {
	b := false
	if args.LastLogTerm == rf.LogTail().Term {
		b = args.LastLogIndex >= rf.LogTail().Index
	} else {
		b = args.LastLogTerm >= rf.LogTail().Term
	}
	return b
}
/* ==================== Leader Election End ========================= */
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	entry := LogEntry{
		Valid:   true,
		Term:    rf.term,
		Index:   rf.LogTail().Index + 1,
		Seq:     rf.LogTail().Seq + 1,
		Command: command,
	}
	rf.StartL(entry)
	return entry.Seq, rf.term, rf.state == Leader
}


func (rf *Raft) StartL(entry LogEntry) {
	rf.log = append(rf.log, &entry)
	rf.persist()
	rf.matchIndex[rf.me] += 1
	rf.BroadcastHeartbeat()
}


func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.Initialize(peers, me, persister, applyCh)
	return rf
}

func (rf *Raft) Initialize(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) {
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&sync.Mutex{})

	rf.electionTimeout = NextElectionTimeout()
	rf.log = append(rf.log, &LogEntry{
		Term: 0,
		Index: 0,
		Seq: 0,
		Command: nil,
	})

	go rf.DoElection()
	go rf.DoHeartbeat()
	go rf.DoApply(applyCh)
	for range rf.peers {
		rf.replicateCond = append(rf.replicateCond, sync.NewCond(&sync.Mutex{}))
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.DoReplicate(i)
	}
	rf.readPersist(persister.ReadRaftState())
}