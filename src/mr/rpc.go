package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

//type Base struct {
//	StatusCode int
//	StatusMsg string
//}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type AssignTaskArgs struct {
	 WorkerId WorkerId
}

type AssignTaskReply struct {
	//Base
	// lab1假设网络为可信赖
	TaskInfo
	NMap int
	NReduce int
}

type EndTaskArgs struct {
	TaskInfo
}

type EndTaskReply struct {
	Accepted bool //处理结果是否被master接受
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
