package raft

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
}

func (rs *RaftState) getLastLog() Entry {
	return rs.logs.entries[len(rs.logs.entries)-1]
}

func (rs *RaftState) appendLog(entry Entry) {
	if rs.getLastLog().Index+1 != entry.Index {
		panic("rs.getLastLog().Index+1 != entry.Index")
	}
	rs.logs.entries = append(rs.logs.entries, entry)
}

func NewLogs() *Logs {
	logs := &Logs{
		entries:     []Entry{{}},
		commitIndex: 0,
		lastApplied: 0,
	}
	return logs
}
