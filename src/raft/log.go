package raft

type Log struct {
	Command interface{}
	Term    int64
	Index   int64
}

func (rf *Raft) getLastLog() Log {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) appendLog(logs ...Log) {
	rf.logs = append(rf.logs, logs...)
}
