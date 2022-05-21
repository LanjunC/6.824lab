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
	"LanjunC/mit6.824/labgob"
	"bytes"
	"fmt"
	"log"
	"time"
)
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
	CommandTerm  int64
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	//mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state *RaftState

	//// 持久性状态 (在响应 RPC 请求之前，已经更新到了稳定的存储设备)
	//currentTerm int64
	//votedFor    int64
	//logs        []Log
	//
	//// 易失性状态
	//commitIndex int64
	//lastApplied int64
	//
	//// leader专属的易失性状态
	//nextIndex  []int64
	//matchIndex []int64

	//lastHeartBeatTime time.Time     //leader上一次发送心跳时间
	//timeOutDuration   time.Duration // 本次选举超时时间

	ticker *time.Ticker
	msgCh  chan interface{}

	applyCh chan ApplyMsg
	//applyCond *sync.Cond
	closeCh chan struct{}
}

type getStateReq struct {
	getStateCh chan getStateResp
}

type getStateResp struct {
	term     int64
	isLeader bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	if rf.killed() {
		return 0, false
	}
	getStateCh := make(chan getStateResp)
	rf.msgCh <- getStateReq{getStateCh: getStateCh}
	select {
	case res := <-getStateCh:
		return int(res.term), res.isLeader
	case <-rf.closeCh:
		return 0, false
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.state.currentTerm)
	e.Encode(rf.state.votedFor)
	e.Encode(rf.state.logs.entries)
	// todo: 相比paper可新增如下字段方便快速恢复
	//e.Encode(rf.state.logs.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int64
	var votedFor int64
	var entries []Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&entries) != nil {
		log.Fatal("read persist failed")
	} else {
		rf.state.currentTerm = currentTerm
		rf.state.votedFor = votedFor
		rf.state.logs.entries = entries
		rf.state.logPrint("Read persist success.")
	}
}

type VoidArgs struct{}

func (rf *Raft) Message(args *Message, _ *VoidArgs) {
	if rf.killed() {
		return
	}
	rf.msgCh <- *args
}

func (rf *Raft) sendMessage(server int, args *Message, reply *VoidArgs) bool {
	ok := rf.peers[server].Call("Raft.Message", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
//type RequestVoteArgs struct {
//	// Your data here (2A, 2B).
//	Term         int64
//	CandidateId  int64
//	LastLogIndex int64
//	LastLogTerm  int64
//}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
//type RequestVoteReply struct {
//	// Your data here (2A).
//	Term        int64
//	VoteGranted bool
//}

//
// example RequestVote RPC handler.
//
//func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
//	// Your code here (2A, 2B).
//}

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
//func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
//	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
//	return ok
//}

//type AppendEntriesArgs struct {
//	Term         int64
//	LeaderId     int64
//	PrevLogIndex int64
//	PrevLogTerm  int64
//	Entries      []Log
//	LeaderCommit int64
//}

//type AppendEntriesReply struct {
//	Term    int64
//	Success bool
//	Xterm   int64
//	XIndex  int64
//	XLen    int64
//}

//// 用于日志的复制，同时也用做心跳
//func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
//}

//func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
//	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
//	return ok
//}

type startReq struct {
	command     interface{}
	startRespCh chan startResp
}

type startResp struct {
	index    int64
	term     int64
	isLeader bool
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
	// Your code here (2B).
	if rf.killed() {
		return 0, 0, false
	}
	startRespCh := make(chan startResp)
	rf.msgCh <- startReq{
		command:     command,
		startRespCh: startRespCh,
	}
	select {
	case res := <-startRespCh:
		return int(res.index), int(res.term), res.isLeader
	case <-rf.closeCh:
		return 0, 0, false
	}
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
	//atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.closeCh)
}

//
func (rf *Raft) killed() bool {
	//z := atomic.LoadInt32(&rf.dead)
	//return z == 1
	select {
	// 已经close的channel, 还有数据时读ok为true，读完后ok为false，读到的数据是0值
	case _, ok := <-rf.closeCh:
		return !ok
	default:
		return false
	}
}

func (rf *Raft) eventLoop() {
	for {
		select {
		case msg := <-rf.msgCh:
			//rf.state.logPrint("DEBUG msg: %+v", msg)
			l := len(rf.msgCh)
			msgs := make([]interface{}, 0, l+1)
			msgs = append(msgs, msg)
			for i := 0; i < l; i++ {
				msgs = append(msgs, <-rf.msgCh)
			}
			for _, msg := range msgs {
				switch v := msg.(type) {
				case Message:
					rf.state.step(&v)
				case getStateReq:
					v.getStateCh <- getStateResp{
						term:     rf.state.currentTerm,
						isLeader: rf.state.stateType == StateLeader,
					}
				case startReq:
					if rf.state.stateType != StateLeader {
						v.startRespCh <- startResp{isLeader: false}
					} else {
						index, term := rf.state.getLastLog().Index+1, rf.state.currentTerm
						rf.state.logPrint("start working %+v", v.command)
						toPropose := Message{
							MsgType: MsgPropose,
							From:    int64(rf.me),
							To:      int64(rf.me),
							Entries: []Entry{{
								Command: v.command,
								Term:    term,
								Index:   index,
							}},
						}
						rf.state.step(&toPropose)
						v.startRespCh <- startResp{
							index:    index,
							term:     term,
							isLeader: true,
						}
					}
				default:
					panic(fmt.Sprintf("Invalid type %T", v))
				}
			}
			rf.handleRaftReady()
		case <-rf.closeCh:
			return
		}
	}
}

func (rf *Raft) onTick() {
	rf.ticker = time.NewTicker(tickDuration)
	defer rf.ticker.Stop()
	for {
		select {
		case <-rf.ticker.C:
			rf.msgCh <- Message{MsgType: MsgTick, From: int64(rf.me), To: int64(rf.me)}
		case <-rf.closeCh:
			return
		}
	}
}
func (rf *Raft) handleRaftReady() {
	// raft state 发生了变化，persist
	rf.persist()

	for i, _ := range rf.state.msgs {
		//msg := &rf.state.msgs[i]
		// 直接使用&rf.state.msgs[i]的话会踩到切片的坑：
		// 浅拷贝完后，rf.state.msgs = rf.state.msgs[:0]切片大小清零，但底层数组未变
		// 并发调rpc时msgs可能会append新的msg进来脏掉还未发送的msg
		msg := deepCopyMsg(&rf.state.msgs[i])

		go rf.sendMessage(int(msg.To), msg, &VoidArgs{})
	}
	rf.state.msgs = rf.state.msgs[:0]

	// apply日志
	for i := rf.state.logs.lastApplied + 1; i <= rf.state.logs.commitIndex; i++ {
		applyMsg := ApplyMsg{
			Command:      rf.state.logs.entries[i].Command,
			CommandValid: true,
			CommandIndex: int(i),
			CommandTerm:  rf.state.currentTerm,
		}
		rf.state.logPrint("handleRaftReady apply log: apply command, index=%v, term=%v, command=%v",
			i, rf.state.logs.entries[i].Term, rf.state.logs.entries[i].Command)
		rf.applyCh <- applyMsg
	}
	rf.state.logs.lastApplied = rf.state.logs.commitIndex
}

func deepCopyMsg(src *Message) *Message {
	res := *src
	return &res
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
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		dead:      0,
		state:     newRaftState(me, len(peers), StateFollower),
		ticker:    nil,
		msgCh:     make(chan interface{}, 4096),
		applyCh:   applyCh,
		closeCh:   make(chan struct{}),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.state.logPrint("Make Done.")

	go rf.onTick()
	go rf.eventLoop()

	return rf
}
