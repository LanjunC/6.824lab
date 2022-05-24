package raft

import (
	"fmt"
	"math/rand"
	"time"
)

type stateType int64

const (
	StateLeader stateType = iota
	StateFollower
	StateCandidate
)

const (
	// todo: 配置文件的方式初始化
	//tickDuration      = 10 * time.Millisecond // tick间隔
	//HeartBeatTimeout  = 10                    // leader心跳间隔时间(单位为tick)
	//TimeOutDurationSt = 90                    // 选举超时的随机范围的下限(单位为tick)
	//TimeOutDurationEd = 120                   // 选举超时的随机范围的上限(单位为tick)
	tickDuration      = 20 * time.Millisecond // tick间隔
	HeartBeatTimeout  = 5                     // leader心跳间隔时间(单位为tick)
	TimeOutDurationSt = 20                    // 选举超时的随机范围的下限(单位为tick)
	TimeOutDurationEd = 35                    // 选举超时的随机范围的上限(单位为tick)
)

var stateMap = map[stateType]string{
	StateLeader:    "Leader",
	StateFollower:  "Follower",
	StateCandidate: "Candidate",
}

type messageType int64

const (
	MsgTick messageType = iota
	MsgRequestVote
	MsgRequestVoteResp
	MsgBroadcast
	MsgBroadcastResp
	MsgPropose
	MsgSnapShot
)

type persistState struct {
	CurrentTerm int64
	VotedFor    int64
	Entries     []Entry
	// todo: 相比paper可新增如下字段方便快速恢复
	//e.Encode(rf.state.logs.commitIndex)
}

type Message struct {
	MsgType      messageType
	From         int64
	To           int64
	Term         int64
	LogIndex     int64
	LogTerm      int64
	Entries      []Entry
	LeaderCommit int64
	Accept       bool

	XIndex     int64
	XTerm      int64
	XLen       int64
	MatchIndex int64 // for follower to response leader`s appendentries rpc 帮助leader更新matchIndex
	SnapShot   *SnapShot
}

type RaftState struct {
	stateType  stateType
	me         int // this peer's index into peers[]
	peersCount int

	// 持久性状态 (在响应 RPC 请求之前，已经更新到了稳定的存储设备)
	currentTerm int64
	votedFor    int64
	votes       []bool
	logs        *Logs

	// leader专属的易失性状态
	nextIndex  []int64
	matchIndex []int64

	electionTimeout  int64
	electionElapsed  int64
	heartbeatElapsed int64

	msgs []Message
}

func (rs *RaftState) resetTimeout() {
	st := int64(TimeOutDurationSt)
	ed := int64(TimeOutDurationEd)
	rs.electionTimeout = st + rand.Int63n(ed-st)
}

func (rs *RaftState) step(m *Message) {
	switch rs.stateType {
	case StateLeader:
		rs.stepLeader(m)
	case StateCandidate:
		rs.stepCandidate(m)
	case StateFollower:
		rs.stepFollower(m)
	}
}

func (rs *RaftState) stepLeader(m *Message) {
	switch m.MsgType {
	case MsgTick:
		rs.handleTick()
	case MsgRequestVote:
		rs.handleRequestVote(m)
	case MsgRequestVoteResp:
	case MsgBroadcast:
		rs.handleBroadcast(m)
	case MsgBroadcastResp:
		rs.handleBroadcastResp(m)
	case MsgPropose:
		rs.handleAppendEntry(m)
	case MsgSnapShot: // todo:check
		rs.handleSnapShot(m)
	}
}
func (rs *RaftState) stepCandidate(m *Message) {
	switch m.MsgType {
	case MsgTick:
		rs.handleTick()
	case MsgRequestVote:
		rs.handleRequestVote(m)
	case MsgRequestVoteResp:
		rs.handleRequestVoteResp(m)
	case MsgBroadcast:
		rs.handleBroadcast(m)
	case MsgBroadcastResp:
	case MsgSnapShot: // todo:check
		rs.handleSnapShot(m)
	}
}

func (rs *RaftState) stepFollower(m *Message) {
	switch m.MsgType {
	case MsgTick:
		rs.handleTick()
	case MsgRequestVote:
		rs.handleRequestVote(m)
	case MsgRequestVoteResp:
	case MsgBroadcast:
		rs.handleBroadcast(m)
	case MsgBroadcastResp:
	case MsgSnapShot: // todo:check
		rs.handleSnapShot(m)
	}
}

func (rs *RaftState) handleTick() {
	switch rs.stateType {
	case StateLeader:
		rs.heartbeatElapsed++
		if rs.heartbeatElapsed >= HeartBeatTimeout {
			rs.heartbeatElapsed = 0
			rs.broadcast()
		}
	case StateCandidate, StateFollower:
		rs.electionElapsed++
		if rs.electionElapsed >= rs.electionTimeout {
			rs.logPrint("Election timeout, start election.(old electionTimeout is %v)", rs.electionTimeout)
			rs.becomeCandidate()
			rs.campaign()
		}
	}
}

func (rs *RaftState) handleRequestVote(m *Message) {
	rs.logPrint("Request vote, message=%+v", m)

	resp := Message{
		MsgType: MsgRequestVoteResp,
		From:    m.To,
		To:      m.From,
		Term:    rs.currentTerm,
		Accept:  false,
	}
	defer func(resp *Message) {
		rs.msgs = append(rs.msgs, *resp)
	}(&resp)

	// RequestVote rule 1: candidate的term小于接收者的当前term，拒绝投票
	if m.Term < rs.currentTerm {
		rs.logPrint("Request vote: refuse it because [%v]`s term [%v] is less.", m.From, m.Term)
		return
	}
	// all server rule 2: candidate的term大于接收者的currentTerm, 需更新currentTerm，状态变更为follower
	if rs.currentTerm < m.Term {
		rs.logPrint("Request vote: term is less than [%v]`s term[%v], so update to follower.", m.From, m.Term)
		rs.becomeFollower(m.Term)
	}
	// RequestVote rule 2: 如果 votedFor 为空或者为 candidateId，并且candidate的日志至少和自己一样新，那么就投票
	// 1.如果两份日志最后 entry 的 term 号不同，则 term 号大的日志更新
	// 2.如果两份日志最后 entry 的 term 号相同，则比较长的日志更新
	theLastIndex := rs.logs.getLastIndex()
	theLastTerm, err := rs.logs.getTerm(theLastIndex)
	if err != nil {
		panic("unexpected")
	}
	candidateIsNew := m.LogTerm > theLastTerm || m.LogTerm == theLastTerm && m.LogIndex >= theLastIndex
	if (rs.votedFor == -1 || rs.votedFor == m.From) && candidateIsNew {
		rs.logPrint("Request vote: agree to vote for [%v].", m.From)
		rs.votedFor = m.From
		resp.Accept = true
	}
}

// for candidate
func (rs *RaftState) handleRequestVoteResp(m *Message) {
	// 这种发送和接收解耦的情景下无法判断rpc是否被接收
	// if !ok {}

	//// 判断收到的term是否和发送时term一样，不一样说明此次rpc请求已过期，可忽略
	//if args.Term != rf.currentTerm {
	//	rf.logPrint("Candidate request votes: term has been changed from previous args`s term[%v] to [%v], so ignore it.", args.Term, rf.currentTerm)
	//	return
	//}

	// 回复的term大于当前term，转变为follower，更新term
	if !m.Accept {
		if m.Term > rs.currentTerm {
			rs.logPrint("Candidate request votes: term is less than [%v]`s term[%v], so update to follower.", m.From, m.Term)
			rs.becomeFollower(m.Term)
		}
		return
	}
	majorCount := rs.peersCount/2 + 1
	count := 0
	rs.votes[m.From] = true
	// todo: votes改成map， 在被拒次数达阈值时直接term++进入新选举轮次
	// bad case: 01234， 04被分区， 3和12隔离，隔离区间，1作为leader追加了新的日志，而3开始选举，然后隔离马上又被恢复，由于3的term更高，1会变为
	for _, b := range rs.votes {
		if b {
			count++
		}
	}
	if count >= majorCount {
		rs.logPrint("Candidate request votes: received majority. tranfer from candidate to leader.")
		rs.becomeLeader()
		rs.broadcast()
	}
}

// for leader/candidate/follower
func (rs *RaftState) handleBroadcast(m *Message) {
	resp := Message{
		MsgType: MsgBroadcastResp,
		From:    m.To,
		To:      m.From,
		Term:    rs.currentTerm,
		Accept:  false,
	}
	defer func(resp *Message) {
		rs.msgs = append(rs.msgs, *resp)
	}(&resp)

	rs.logPrint("Receive broadcast(heartbeat or appendEntries), message=%+v", m)
	// AppendEntries rule 1: leader的term小于接收者的当前term, 返回false
	if m.Term < rs.currentTerm {
		rs.logPrint("Receive broadcast(heartbeat or appendEntries): refuse it because [%v]`s term [%v] is less.", m.From, m.Term)
		resp.Accept = false
		return
	}
	// follower rule 2: AppendEntries rpc 心跳防止选举超时
	rs.electionElapsed = 0
	// candidate rule 3: 接收到了合法leader的AppendEntries，回到leader
	if rs.stateType == StateCandidate {
		rs.logPrint("Receive broadcast(heartbeat or appendEntries): received valid appendEntries, so update to follower.")
		rs.becomeFollower(m.Term)
	}
	// all server rule 2: leader的term大于接收者的currentTerm, 需更新currentTerm，状态变更为follower
	if rs.currentTerm < m.Term {
		rs.logPrint("Receive broadcast(heartbeat or appendEntries): term is less than [%v]`s term[%v], so update to follower.", m.From, m.Term)
		rs.becomeFollower(m.Term)
	}
	// AppendEntries rule 2: 该peer上找不到prevLogIndex和prevLogTerm匹配的日志则返回false（日志一致性检测）
	// 优化：paper第七页末
	theLastIndex := rs.logs.getLastIndex()
	mayPreLogTerm, err := rs.logs.getTerm(m.LogIndex)
	// leader 早期的日志到达，但follower已经非常新，m.prelogindex对应的日志已经被压缩掉了
	// 其他的err则是follower日志太短，在后面的逻辑会resp.Accpet = false
	if err != nil && err == errCompacted {
		resp.MatchIndex = rs.logs.commitIndex // 已经commit的一定能够和leader匹配，所以
		resp.Accept = true
		return
	}
	if theLastIndex < m.LogIndex || mayPreLogTerm != m.LogTerm {
		rs.logPrint("Receive broadcast(heartbeat or appendEntries): refuse it because no log matches Index=%v and Term=%v", m.LogIndex, m.LogTerm)
		conflictIndex := min(m.LogIndex, theLastIndex)
		xTerm, _ := rs.logs.getTerm(conflictIndex)
		for xIndex := conflictIndex; xIndex > 0; xIndex-- {
			//if rs.logs.entries[xIndex-1].Term != xTerm {
			//	resp.XIndex = xIndex
			//	break
			//}
			// todo check
			if term, err := rs.logs.getTerm(xIndex - 1); (err == nil) && term != xTerm {
				resp.XIndex = xIndex
				break
			}
		}
		resp.XTerm = xTerm
		//resp.XLen = int64(len(rs.logs.entries))
		resp.XLen = rs.logs.getLastIndex() + 1 // check
		resp.Accept = false
		return
	}

	// AppendEntries rule 3 & 4: 一致性检测通过，追加日志（同时覆盖冲突日志）
	// 需要注意由于网络延迟造成包无序所带来的影响
	rs.logPrint("Receive broadcast(heartbeat or appendEntries): accept log entry. preLogIndex=%v, preLogTerm=%v", m.LogIndex, m.LogTerm)
	conflictI := -1
	conflictIndex := int64(-1)
	for i, entry := range m.Entries {
		//term, err := rs.logs.getTerm(entry.Index)
		//if err != nil {
		//	rs.logPrint("DEBUG:%v", rs.logs.entries)
		//	panic(fmt.Sprintf("get term failed, index=%v, err=[%v]", entry.Index, err.Error()))
		//}
		mayConflictTerm, err := rs.logs.getTerm(entry.Index)
		if err != nil && err == errCompacted {
			panic("unexpected here(compacted log case has been handled before)")
		}
		if entry.Index > theLastIndex || entry.Term != mayConflictTerm { // ignore err provisionally
			//rs.logs.entries = rs.logs.entries[:entry.Index]
			//rs.logs.entries = append(rs.logs.entries, m.Entries[i:]...) // todo: 封装
			//rs.logs.truncateLast(entry.Index)
			//for _, e := range m.Entries[i:] {
			//	rs.logs.appendLog(e)
			//}
			conflictI = i
			conflictIndex = entry.Index
			break
		}
	}
	// find conflictEntry
	if conflictI != -1 {
		rs.logs.truncateLast(conflictIndex)
		for _, e := range m.Entries[conflictI:] {
			rs.logs.appendLog(e)
		}
	}

	// 新增paper中没有的字段， 用于leader更新matchIndex
	//resp.MatchIndex = rs.getLastLog().Index 是错误的，如新leader上任，但有比他日志更新的follower（index更大）
	resp.MatchIndex = m.LogIndex + int64(len(m.Entries))
	// AppendEntries rule 5: 更新rf.commitIndex
	old := rs.logs.commitIndex
	if m.LeaderCommit > rs.logs.commitIndex {
		// 取min(LeaderCommit,刚追加的新日志的最大Index)
		rs.logs.commitIndex = min(m.LeaderCommit, rs.logs.getLastIndex()) // why?
		rs.logPrint("Receive broadcast(heartbeat or appendEntries): update commitIndex from %v to %v", old, rs.logs.commitIndex)
		//rf.apply()
	}
	resp.Accept = true
}

// for leader
func (rs *RaftState) handleBroadcastResp(m *Message) {
	// 这种发送和接收解耦的情景下无法判断rpc是否被接收
	// if !ok {}

	// all server rule 2: leader的term小于收到的的term, 需更新term为收到的term，状态变更为follower
	if rs.currentTerm < m.Term {
		rs.logPrint("Receive broadcast(heartbeat or appendEntries) resp: term is less than [%v]`s term[%v], so update to follower.", m.From, m.Term)
		rs.becomeFollower(m.Term)
		return
	}
	// leader rule 3: 日志不一致被拒绝
	if m.Accept {
		rs.logPrint("Receive broadcast(heartbeat or appendEntries) resp: [%v] reply success.", m.From)
		match := m.MatchIndex
		next := match + 1
		rs.matchIndex[m.From] = max(rs.matchIndex[m.From], match)
		rs.nextIndex[m.From] = max(rs.nextIndex[m.From], next)
		rs.leaderCommit()
	} else {
		// todo check
		old := rs.nextIndex[m.From]
		rs.logPrint("Receive broadcast(heartbeat or appendEntries) resp: [%v] reply refuse.", m.From)
		lastLogIndexInXTerm := rs.findLastLogInXTerm(m.XTerm)
		if lastLogIndexInXTerm > 0 {
			rs.nextIndex[m.From] = lastLogIndexInXTerm
		} else {
			rs.nextIndex[m.From] = m.XIndex
		}
		if m.XLen < rs.nextIndex[m.From] {
			rs.nextIndex[m.From] = m.XLen
		}
		nIdx := rs.nextIndex[m.From]
		rs.logPrint("Receive broadcast(heartbeat or appendEntries) resp: [%v]`s nextIndex sub from %v to %v", m.From, old, nIdx)
		// todo: 日志不一致立刻重发，而不是等待下一次 rpc
	}
}

// for leader: process client`s command by append entry
func (rs *RaftState) handleAppendEntry(m *Message) {
	if len(m.Entries) != 1 {
		panic("invalid length of entries: len(m.Entries) != 1")
	}
	entry := m.Entries[0]
	if entry.Term != rs.currentTerm {
		panic("entry.Term != rs.currentTerm")
	}
	rs.logPrint("handleAppendEntry: append entry and broadcast")
	rs.logs.appendLog(entry)
	if rs.peersCount == 1 {
		rs.logPrint("handleAppendEntry: only one peer, call leaderCommit directly.")
		rs.leaderCommit()
	}
	rs.broadcast() // todo: 并发量大的情况下是否有必要？因为此时start速度远大于日志同步速度，会发送很多重复的appendEntries rpc
}

func (rs *RaftState) handleSnapShot(m *Message) {
	resp := Message{
		MsgType:    MsgBroadcastResp,
		From:       m.To,
		To:         m.From,
		Term:       rs.currentTerm,
		MatchIndex: -1, // todo: set later
		Accept:     false,
	}
	defer func(resp *Message) {
		rs.msgs = append(rs.msgs, *resp)
	}(&resp)

	rs.logPrint("Receive installSnapshot, message=%+v", m)
	// Like AppendEntries rule 1: leader的term小于接收者的当前term, 返回false
	if m.Term < rs.currentTerm {
		rs.logPrint("Receive installSnapshot: refuse it because [%v]`s term [%v] is less.", m.From, m.Term)
		resp.Accept = false
		return
	}
	// append snapshot rpc同样可以当心跳包用
	rs.electionElapsed = 0
	// candidate rule 3: 接收到了合法leader的append snapshot rpc，回到follower
	if rs.stateType == StateCandidate {
		rs.logPrint("Receive installSnapshot: received valid installSnapshot rpc, so update to follower.")
		rs.becomeFollower(m.Term)
	}
	// all server rule 2: leader的term大于接收者的currentTerm, 需更新currentTerm，状态变更为follower
	if rs.currentTerm < m.Term {
		rs.logPrint("Receive installSnapshot: term is less than [%v]`s term[%v], so update to follower.", m.From, m.Term)
		rs.becomeFollower(m.Term)
	}

	// 这种情况[有可能?]发生，如使用了优化算法使得leader在减peer.next时快速倒退
	if rs.logs.commitIndex >= m.SnapShot.Index {
		rs.logPrint("Receive installSnapshot: leader go back too match...")
		resp.MatchIndex = rs.logs.commitIndex
		resp.Accept = true
		return
	}
	// save snapshot file
	if rs.logs.pendingSnapShot == nil || rs.logs.pendingSnapShot.Index < m.SnapShot.Index {
		rs.logPrint("Receive installSnapshot: save snapshot file.")
		rs.logs.pendingSnapShot = m.SnapShot
	}
	if term, err := rs.logs.getTerm(m.SnapShot.Index); err == nil && term == m.SnapShot.Term {
		rs.logPrint("Receive installSnapshot: exists entry has same index=%v and term=%v as snapshot’s last included entry.", m.SnapShot.Index, term)
		//
		// 有两个地方会生成snapshot,都在handleRaftReady中
		// 首先是pendingSnapShot != nil时
		// 其次是阈值检测，日志压缩
		// 因此这里检测到term, err := rs.logs.getTerm(m.SnapShot.Index); err == nil && term == m.SnapShot.Term时，
		// 就可认为此时日志并没有落后于leader，至少能跟上leader快照的最后一条，
		// 因此将压缩的任务交给阈值检测时，避免在handleRaftReady中两次执行生成snapshot操作引起不必要的混乱
		// 可以尝试把这段if注释掉测试TestSnapshotRecoverManyClients3B查看bug
		// some bad case here...
		if rs.logs.pendingSnapShot == m.SnapShot {
			rs.logPrint("Receive installSnapshot: the most recent snapshot, it can? be ignored.")
			rs.logs.pendingSnapShot = nil
		}
		rs.logs.commitIndex = m.SnapShot.Index
		resp.MatchIndex = m.SnapShot.Index
		resp.Accept = true
		return
	} else {
		rs.logPrint("Receive installSnapshot: discard log entries")
		rs.logs.resetWithSnapShot(rs.logs.pendingSnapShot)
		resp.MatchIndex = rs.logs.getLastIndex()
		resp.Accept = true
		return
	}
}

func (rs *RaftState) becomeCandidate() {
	rs.stateType = StateCandidate
	rs.currentTerm++
	rs.votedFor = int64(rs.me)
	rs.votes = make([]bool, rs.peersCount)
	rs.votes[rs.me] = true
	rs.resetTimeout()
	rs.electionElapsed = 0
}

func (rs *RaftState) becomeFollower(term int64) {
	rs.stateType = StateFollower
	rs.currentTerm = term
	rs.votedFor = -1
	rs.electionElapsed = 0
	// 若不reset可能会导致以下bad case：
	// 3节点情况，C节点term较大，且尝试选举，但他没有最新日志无法当选
	//另两个节点在收到C节点请求投票时由于term较小becomefollower且拒绝投票，且恰好electiontimeout较大，C再次超时还是无法发起选举
	// 如此形成AB追赶C的term且始终无法参与选举
	// 另外还要注意electiontimeout随机范围尽可能大
	rs.resetTimeout()
}

func (rs *RaftState) becomeLeader() {
	rs.stateType = StateLeader
	rs.heartbeatElapsed = 0
	nextIndex := rs.logs.getLastIndex() + 1
	for peer := 0; peer < rs.peersCount; peer++ {
		rs.nextIndex[peer] = nextIndex
		rs.matchIndex[peer] = 0
	}
}

func (rs *RaftState) campaign() {
	if rs.peersCount == 1 {
		rs.becomeLeader()
		return
	}

	for peer := 0; peer < rs.peersCount; peer++ {
		if peer == rs.me {
			continue
		}
		theLastIndex := rs.logs.getLastIndex()
		theLastTerm, _ := rs.logs.getTerm(theLastIndex)
		msg := Message{
			MsgType:  MsgRequestVote,
			From:     int64(rs.me),
			To:       int64(peer),
			Term:     rs.currentTerm,
			LogIndex: theLastIndex,
			LogTerm:  theLastTerm,
		}
		rs.msgs = append(rs.msgs, msg)
	}
}

// for leader: heartbeat or appendEntries or installSnapshot
func (rs *RaftState) broadcast() {
	for peer := 0; peer < rs.peersCount; peer++ {
		if peer == rs.me {
			continue
		}
		nextIndex := rs.nextIndex[peer]
		if nextIndex < 1 { //nextIndex的实际意义需合理
			nextIndex = 1
		}
		if nextIndex <= rs.logs.preLogIndex {
			// 节点落后，发送 installSnapshot rpc 而不是 append entries rpc
			rs.logPrint("find [%v]`s log is behindhand(nextIndex=%v <= logs.preLogIndex=%v), "+
				"try to append snapshot file", peer, nextIndex, rs.logs.preLogIndex)
			msg := Message{
				MsgType:  MsgSnapShot,
				From:     int64(rs.me),
				To:       int64(peer),
				Term:     rs.currentTerm,
				SnapShot: nil, // set while sending rpc
			}
			rs.msgs = append(rs.msgs, msg)
			continue
		}
		rs.logPrint("try to append entries/send heartbeat to [%v]...", peer)
		entries := make([]Entry, rs.logs.getLastIndex()-nextIndex+1)
		//copy(entries, rs.logs.entries[nextIndex:])
		copy(entries, rs.logs.getEntriesFrom(nextIndex))
		preLogTerm, _ := rs.logs.getTerm(nextIndex - 1)
		msg := Message{
			MsgType:      MsgBroadcast,
			From:         int64(rs.me),
			To:           int64(peer),
			Term:         rs.currentTerm,
			LogIndex:     nextIndex - 1,
			LogTerm:      preLogTerm,
			Entries:      entries,
			LeaderCommit: rs.logs.commitIndex,
		}
		rs.msgs = append(rs.msgs, msg)
	}
}

func (rs *RaftState) findLastLogInXTerm(xterm int64) int64 {
	for i := len(rs.logs.entries) - 1; i > 0; i-- {
		term := rs.logs.entries[i].Term
		if term == xterm {
			return int64(i)
		}
		if term < xterm {
			break
		}
	}
	return -1
}

func (rs *RaftState) leaderCommit() {
	N := rs.logs.commitIndex
	for n := rs.logs.commitIndex + 1; n <= rs.logs.getLastIndex(); n++ {
		// figure 8: 不提交非当前任期的日志
		// p.s. 一旦当前任期的日志被提交，那么由于日志匹配特性，之前的日志条目也都会被间接的提交
		if term, _ := rs.logs.getTerm(n); term != rs.currentTerm {
			continue
		}
		count := 1
		for peerId := 0; peerId < rs.peersCount; peerId++ {
			if peerId != rs.me && rs.matchIndex[peerId] >= n {
				count++
			}
			if count > rs.peersCount/2 {
				N = n
				break
			}
		}
	}
	if N == rs.logs.commitIndex {
		rs.logPrint("No log to commit. Skip it.")
		return
	}
	rs.logPrint("leaderCommit: update commitIndex from %v to %v", rs.logs.commitIndex, N)
	rs.logs.commitIndex = N
}

func newRaftState(me int, peersCount int, stateType stateType, persistState *persistState, snap *SnapShot) *RaftState {
	rs := &RaftState{
		stateType:        stateType,
		me:               me,
		peersCount:       peersCount,
		currentTerm:      0,
		votedFor:         -1,
		votes:            make([]bool, peersCount),
		logs:             NewLogs(persistState, snap),
		nextIndex:        make([]int64, peersCount),
		matchIndex:       make([]int64, peersCount),
		electionTimeout:  -1, // reset later
		electionElapsed:  0,
		heartbeatElapsed: 0,
		msgs:             make([]Message, 0),
	}
	rs.resetTimeout()
	// read persist state
	if persistState != nil {
		rs.currentTerm = persistState.CurrentTerm
		rs.votedFor = persistState.VotedFor
		rs.logs.entries = persistState.Entries
	}
	return rs
}

// 非并发安全debug输出
func (rs *RaftState) logPrint(format string, args ...interface{}) {
	if true {
		return
	}
	s := fmt.Sprintf(format, args...)
	fmt.Printf("[%v][state: %v] [term: %v] [votedFor: %v] | %s\n", rs.me, stateMap[rs.stateType], rs.currentTerm, rs.votedFor, s)
}
