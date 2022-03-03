package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = 0

type RaftState int

const (
	RaftStateLeader RaftState = iota
	RaftStateFollower
	RaftStateCandidate
)

var rander = rand.New(rand.NewSource(time.Now().UnixNano()))
var stateMap map[RaftState]string

const (
	SleepDuration     = 10 * time.Millisecond   // 废弃 mainLoop主循环中每次滴答间隔时间.
	HeartBeatDuration = 150 * time.Millisecond  // leader心跳间隔时间
	TimeOutDurationSt = 1200 * time.Millisecond // 选举超时的随机范围的下限
	TimeOutDurationEd = 1500 * time.Millisecond // 选举超时的随机范围的上限
)

func init() {
	stateMap = map[RaftState]string{
		RaftStateLeader:    "Leader",
		RaftStateFollower:  "Follower",
		RaftStateCandidate: "Candidate",
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func generateTimeout() time.Duration {
	st := int64(TimeOutDurationSt)
	ed := int64(TimeOutDurationEd)
	return time.Duration(st + rander.Int63n(ed-st))
}

func max(i, j int64) int64 {
	if i > j {
		return i
	}
	return j
}

func min(i, j int64) int64 {
	if i > j {
		return j
	}
	return i
}
