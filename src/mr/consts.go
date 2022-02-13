package mr

type TaskId int
type TaskType int
type WorkerId int

const (
	WaitTimeMil       = 10
	TaskTypeMap    = 0
	TaskTypeReduce = 1
	TaskTypeAllDone = 2 //所有任务已结束，worker收到此回复后可结束进程
	TaskTypeWait   = 3 //有任务未执行完毕，但所有任务已分配，告知worker继续等待

	MaxExecuteTimeSec = 10
)
