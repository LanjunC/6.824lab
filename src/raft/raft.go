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
	"sync"
	"time"
)
import "sync/atomic"
import "LanjunC/mit6.824/labrpc"

// import "bytes"
// import "../labgob"

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
	state RaftState // server状态可以是leader follower candidate. todo: raft状态机

	// 持久性状态 (在响应 RPC 请求之前，已经更新到了稳定的存储设备)
	currentTerm int64
	votedFor    int64
	log         []*LogEntry

	// 易失性状态
	commitIndex int64
	lastApplied int64

	// leader专属的易失性状态
	nextIndex  []int64
	matchIndex []int64

	lastHeartBeatTime time.Time     //leader上一次发送心跳时间
	timeOutDuration   time.Duration // 本次选举超时时间
}

// 非并发安全
func (rf *Raft) logPrint(format string, args ...interface{}) {
	s := fmt.Sprintf(format, args...)
	fmt.Printf("[%v][state: %v] [term: %v] [votedFor: %v] | %s\n", rf.me, stateMap[rf.state], rf.currentTerm, rf.votedFor, s)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term, isleader = int(rf.currentTerm), rf.state == RaftStateLeader
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
	Term         int64
	CandidateId  int64
	LastLogIndex int64
	LastLogTerm  int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logPrint("Request vote, args=%+v", args)

	reply.Term = rf.currentTerm
	// candidate的term小于接收者的当前term，拒绝投票
	// todo
	if rf.currentTerm > args.Term {
		rf.logPrint("Request vote: refuse it because [%v]`s term [%v] is less.", args.CandidateId, args.Term)
		reply.VoteGranted = false
		return
	}
	// candidate的term大于接收者的currentTerm, 需更新currentTerm为candidate的term，状态变更为follower
	if rf.currentTerm < args.Term {
		rf.logPrint("Receive appendEntries: term is less than [%v]`s term[%v], so update to follower.", args.CandidateId, args.Term)
		rf.currentTerm = args.Term
		rf.state = RaftStateFollower
		rf.votedFor = -1
	}
	// 如果 votedFor 为空或者为 candidateId，并且candidate的日志至少和自己一样新，那么就投票
	// 投票后更新心跳时间 （论文fiture2有一句话提到）
	votedFor := rf.votedFor
	logLen := len(rf.log) - 1
	// todo: 新旧比较补充完整
	// 1.如果两份日志最后 entry 的 term 号不同，则 term 号大的日志更新
	// 2.如果两份日志最后 entry 的 term 号相同，则比较长的日志更新
	if (votedFor == -1 || votedFor == args.CandidateId) && int64(logLen) <= args.LastLogIndex {
		rf.logPrint("Receive appendEntries: agree to vote for [%v].", args.CandidateId)
		rf.votedFor = args.CandidateId
		rf.lastHeartBeatTime = time.Now()
		reply.VoteGranted = true
	}
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

type AppendEntriesArgs struct {
	Term         int64
	LeaderId     int64
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []*LogEntry
	LeaderCommit int64
}

type AppendEntriesReply struct {
	Term    int64
	Success bool
}

// 用于日志的复制，同时也用做心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logPrint("Receive appendEntries, args=%+v", args)
	reply.Term = rf.currentTerm
	// leader的term小于接收者的当前term, 返回false
	if rf.currentTerm > args.Term {
		rf.logPrint("Receive appendEntries: refuse it because [%v]`s term [%v] is less.", args.LeaderId, args.Term)
		reply.Success = false
		return
	}
	// leader的term大于接收者的currentTerm, 需更新currentTerm为leader的term，状态变更为follower
	// todo
	if rf.currentTerm < args.Term {
		rf.logPrint("Receive appendEntries: term is less than [%v]`s term[%v], so update to follower.", args.LeaderId, args.Term)
		rf.currentTerm = args.Term
		rf.state = RaftStateFollower
		rf.votedFor = -1
	}

	// 本次请求为heartBeat
	if len(args.Entries) == 0 {
		rf.logPrint("Receive appendEntries: get heartBeat from [%v] and accept it.", args.LeaderId)
		rf.lastHeartBeatTime = time.Now()
	}
	reply.Success = true
	// 本次只从处理心跳相关的
	return
}

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

func (rf *Raft) mainLoop() {
	for !rf.killed() {
		time.Sleep(HeartBeatDuration)
		rf.mu.Lock()
		if rf.state == RaftStateLeader {
			rf.doMainLeader()
		}
		rf.checkAndStartElection()
		rf.mu.Unlock()
	}
}

func (rf *Raft) doMainLeader() {
	rf.logPrint("Heartbeat timeout, start to issue heartbeat again.")

	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = int64(rf.me)

	for peerId, _ := range rf.peers {
		if peerId == rf.me {
			rf.lastHeartBeatTime = time.Now()
			continue
		}
		// 对所有server发送心跳
		rf.logPrint("Issue heartbeat to %v.", peerId)
		go func(peerId int) {
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(peerId, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// 不可达
			if !ok {
				rf.logPrint("Issue heartbeat: %v unreachable.", peerId)
				return
			}
			// 判断收到的term是否和发送时term一样，不一样说明此次rpc请求已过期，可忽略
			//if args.Term != rf.currentTerm {
			//	rf.logPrint("Issue heartbeat: term has been changed from previous args`s term to [%v], so ignore it.", peerId)
			//	return
			//}
			// leader的term小于收到的的term, 需更新term为收到的term，状态变更为follower
			if rf.currentTerm < reply.Term {
				rf.logPrint("Issue heartbeat: term is less than [%v]`s term[%v], so update to follower.", peerId, reply.Term)
				rf.currentTerm = reply.Term
				rf.state = RaftStateFollower
				//rf.lastHeartBeatTime = time.Now() // 虽然不是收到心跳rpc，但也重置定时器
				rf.votedFor = -1
			}
		}(peerId)
	}
}

// 两种情况进入选举流程：
// 1. 由follower->candidate
// 2. 选票分裂直到超时，需要发起新一轮选举
func (rf *Raft) checkAndStartElection() {
	if time.Now().Sub(rf.lastHeartBeatTime) > rf.timeOutDuration {
		// 增加term，投票给自己，重置计时器，重置timeout时间
		rf.logPrint("Election timeout, start election.")
		rf.state = RaftStateCandidate
		rf.currentTerm++
		rf.votedFor = int64(rf.me)
		rf.timeOutDuration = generateTimeout()

		// 发送请求投票的RPC
		args := RequestVoteArgs{}
		args.Term = rf.currentTerm
		args.CandidateId = int64(rf.me)
		args.LastLogIndex = int64(len(rf.log) - 1)
		args.LastLogTerm = 0 // lab2A为0即可 todo

		var voteCount int64 = 1 // 获得的投票数
		for peerId, _ := range rf.peers {
			if peerId == rf.me {
				rf.lastHeartBeatTime = time.Now()
				continue
			}
			rf.logPrint("Candidate request vote of %v.", peerId)
			go func(peerId int) {
				var reply RequestVoteReply
				ok := rf.sendRequestVote(peerId, &args, &reply)
				// 不可达
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if !ok {
					rf.logPrint("Candidate request votes: %v unreachable.", peerId)
					return
				}
				// 回复的term大于当前term，转变为follower，更新term
				if reply.Term > rf.currentTerm {
					rf.logPrint("Candidate request votes: term is less than [%v]`s term[%v], so update to follower.", peerId, reply.Term)
					rf.currentTerm = reply.Term
					rf.state = RaftStateFollower
					rf.lastHeartBeatTime = time.Now() // 虽然不是收到心跳rpc，但也重置定时器
					rf.votedFor = -1
					return
				}
				// 赢得选票
				majorCount := len(rf.peers)/2 + 1
				if reply.VoteGranted {
					voteCount++
					if voteCount >= int64(majorCount) {
						// 已获得大多数选票，无须其他选票结果了

						// 当前状态不为candidate则是无效投票，因为已经获得足够选票成为leader
						if rf.state == RaftStateCandidate {
							rf.logPrint("Candidate request votes: received majority. tranfer from candidate to leader.")
							rf.state = RaftStateLeader
						}
						//优化 todo: 成为领导人后立刻发送心跳建立权威
						rf.doMainLeader()
					}
				}
			}(peerId)
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
	rf.state = RaftStateFollower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]*LogEntry, 0)
	rf.commitIndex = -1 // attention: 论文log的初始索引为1, 这里实际采用log初始索引0
 	rf.lastApplied = -1 // attention
 	rf.nextIndex = make([]int64, 0)
	rf.matchIndex = make([]int64, 0)
	rf.lastHeartBeatTime = time.Now()
	rf.timeOutDuration = generateTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.logPrint("Make Done.")
	// 处理心跳和选举-2A
	go rf.mainLoop()

	return rf
}
