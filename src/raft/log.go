package raft

import (
	"errors"
	"fmt"
)

var (
	errInvalidIndex error = errors.New("invalid index when getting term")
	errCompacted    error = errors.New("the expected log has been compacted")
)

type Entry struct {
	Command interface{}
	Term    int64
	Index   int64
}

type Logs struct {
	// 持久性状态
	entries []Entry
	// 易失性状态
	commitIndex int64
	lastApplied int64

	// snapshot包含的最后一条日志
	preLogIndex int64
	preLogTerm  int64

	pendingSnapShot *SnapShot
}

type SnapShot struct {
	Index    int64
	Term     int64
	SnapData []byte
}

func (l *Logs) getLastIndex() int64 {
	if len(l.entries) == 0 {
		return l.preLogIndex
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *Logs) appendLog(entry Entry) {
	if l.getLastIndex()+1 != entry.Index {
		panic("rs.getLastLog().Index+1 != entry.Index")
	}
	l.entries = append(l.entries, entry)
}

func (l *Logs) getTerm(index int64) (int64, error) {
	if index < l.preLogIndex {
		return 0, errCompacted
	}
	// return firstLogTerm if index == firstLogIndex
	if index == l.preLogIndex {
		return l.preLogTerm, nil
	}
	if len(l.entries) == 0 {
		return 0, errInvalidIndex
	}
	firstIndex := l.entries[0].Index
	if int64(len(l.entries))+firstIndex <= index {
		return 0, errInvalidIndex
	}
	return l.entries[index-firstIndex].Term, nil
}

func (l *Logs) getEntry(index int64) (Entry, error) {
	if index <= l.preLogIndex {
		return Entry{}, errInvalidIndex
	}
	if index > l.getLastIndex() {
		return Entry{}, errInvalidIndex
	}
	return l.entries[index-l.preLogIndex-1], nil
}

func (l *Logs) truncateLast(index int64) error {
	if index <= l.preLogIndex || index > l.getLastIndex()+1 {
		return errInvalidIndex
	}
	l.entries = l.entries[:index-l.preLogIndex-1]
	return nil
}

func (l *Logs) compactLog(applied int64) {
	term, err := l.getTerm(applied)
	if err != nil {
		panic("get term failed when compacting log")
	}
	l.entries = l.entries[applied-l.preLogIndex:]
	l.preLogIndex, l.preLogTerm = applied, term
}

func (l *Logs) resetWithSnapShot(snapShot *SnapShot) {
	l.entries = l.entries[:0]
	l.commitIndex = snapShot.Index
	l.preLogIndex = snapShot.Index
	l.preLogTerm = snapShot.Term
}

func (l *Logs) getEntriesFrom(index int64) []Entry {
	if index <= l.preLogIndex || index > l.getLastIndex()+1 {
		panic(fmt.Sprintf("getEntriesFrom failed, index=%v, err=%v", index, errInvalidIndex.Error()))
	}
	return l.entries[index-l.preLogIndex-1:] // may empty
}

func NewLogs(persistState *persistState, snap *SnapShot) *Logs {
	logs := &Logs{
		entries:     []Entry{{}},
		commitIndex: 0,
		lastApplied: 0,
		preLogIndex: -1, //todo check 一次都没有压缩时
		preLogTerm:  -1, //todo check 一次都没有压缩时
	}
	if persistState != nil {
		logs.entries = persistState.Entries
	}
	if snap != nil {
		logs.preLogIndex = snap.Index
		logs.preLogTerm = snap.Term
		logs.lastApplied = snap.Index
		if len(logs.entries) > 0 && logs.preLogIndex+1 != logs.entries[0].Index {
			panic("unexpected")
		}
	}
	return logs
}
