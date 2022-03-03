package raft

type Log struct {
	Command interface{}
	Term    int64
	Index   int64
}

func (rf *Raft) getLastLog() Log {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) appendLog(log ...Log) {
	for _, l := range log {
		rf.log = append(rf.log, l)
	}
}
