package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
import "github.com/deckarep/golang-set"



type TaskInfo struct {
	TaskType TaskType
	TaskId   TaskId
	WorkerId WorkerId
	FileName string
	StartTime int64
}

type Master struct {
	// Your definitions here.

	files   []string
	nReduce int
	nMap    int
	allDone bool
	mapDone bool
	curWorker WorkerId //每次将workerId分配给了某一worker都递增，保证所有worker的id不重复 todo: 多线程安全？

	mapTasks    map[TaskId]*TaskInfo
	reduceTasks map[TaskId]*TaskInfo

	unassignedMapTasks    *TimeoutQueue
	assignedMapTasks      mapset.Set
	unassignedReduceTasks *TimeoutQueue
	assignedReduceTasks   mapset.Set

	mapAssignAndCheckMutex sync.Mutex //用于map任务的assgin和check互斥、reassign和check互斥 todo 用两个锁去控制效率更高
	reduceAssignAndCheckMutex sync.Mutex //用于reduce任务的assgin和check互斥、reassign和check互斥
	reducePrepareOnce sync.Once //prepareReduceTask only once
}

// AssignTask worker向master请求任务
func (m *Master)AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error{
	//assgin时首次赋值workerId，后续无其他赋值场景。 todo: 可优化为worker注册时赋值workerId
	log.Printf("AssignTask: master get assignTask request=%+v\n", args)
	reply.WorkerId = args.WorkerId
	if args.WorkerId == -1 {
		m.curWorker++
		reply.WorkerId = m.curWorker
	}

	if m.allDone { // 所有任务已完成
		reply.TaskType = TaskTypeAllDone
		log.Println("AssignTask: all task done and no task to assign.")
	} else if m.mapDone {
		if m.checkAllReduceAssigned() { //所有map任务已完成，无可以分配的reduce任务
			reply.TaskType = TaskTypeWait
			log.Println("AssignTask: all reduce task has assigned and no task to assign")
		} else { //所有map任务已完成，有可以分配的reduce任务
			taskId, ok := m.AssignReduceTask()
			if ok { //分配成功
				reply.TaskInfo = *m.reduceTasks[taskId]
				reply.NMap = m.nMap
				reply.NReduce = m.nReduce
				log.Printf("AssignTask: assign reduce task to worker %v", reply.WorkerId)
			} else { // 分配失败，原因之一为多个worker争抢任务失败
				reply.TaskType = TaskTypeWait
				log.Println("AssignTask: assign reduce task failed. worker need to wait.")
			}
		}
	} else {
		if m.checkAllMapAssigned() {
			reply.TaskType = TaskTypeWait
			log.Println("AssignTask: all map task has assigned and no task to assign")
		} else {
			taskId, ok := m.AssignMapTask()
			if ok { //分配成功
				reply.TaskInfo = *m.mapTasks[taskId]
				reply.NMap = m.nMap
				reply.NReduce = m.nReduce
				log.Printf("AssignTask: assign map task to worker %v", reply.WorkerId)
			} else { // 分配失败，原因之一为多个worker争抢任务失败
				reply.TaskType = TaskTypeWait
				log.Println("AssignTask: assign map task failed. worker need to wait.")
			}
		}
	}
	log.Printf("AssignTask: master give assignTask reply=%+v\n", reply)
	return nil
}



// EndTask worker向master报告任务结束
func (m *Master) EndTask(args *EndTaskArgs, reply *EndTaskReply) error{
	switch args.TaskType {
	case TaskTypeMap:
		m.assignedMapTasks.Remove(args.TaskId)
		if m.checkAllMapDone() {
			m.prepareReduceTask() //需保证只被调用一次
			m.mapDone = true //mapDone更新必须在prepareReduce之后，确保mapDone=true时reduceTask已经初始化完毕
		}
		reply.Accepted = true
	case TaskTypeReduce:
		m.assignedReduceTasks.Remove(args.TaskId)
		if m.checkAllReduceDone() {
			m.allDone = true
		}
		reply.Accepted = true
	default:
		log.Printf("invalid taskType:%v",  args.TaskType)
		reply.Accepted = false
	}
	return nil
}

func (m *Master) checkAllMapAssigned() bool {
	return m.unassignedMapTasks.Size() == 0
}
func (m *Master) checkAllReduceAssigned() bool {
	return m.unassignedReduceTasks.Size() == 0
}

func (m *Master) checkAllMapDone() bool {
	m.mapAssignAndCheckMutex.Lock()
	defer  m.mapAssignAndCheckMutex.Unlock()
	return m.unassignedMapTasks.Size() == 0 && m.assignedMapTasks.Cardinality() == 0
}

func (m *Master) checkAllReduceDone() bool {
	m.reduceAssignAndCheckMutex.Lock()
	defer m.reduceAssignAndCheckMutex.Unlock()
	return m.unassignedReduceTasks.Size() == 0 && m.assignedReduceTasks.Cardinality() == 0
}

func (m *Master) AssignMapTask() (taskId TaskId, ok bool){
	m.mapAssignAndCheckMutex.Lock()
	defer  m.mapAssignAndCheckMutex.Unlock()
	data,ok := m.unassignedMapTasks.Pop()
	if ok {
		m.assignedMapTasks.Add(data)
		_, nowSec := getTimeNowSecond()
		m.mapTasks[data.(TaskId)].StartTime = nowSec //分配成功，记录任务开始时间
		return data.(TaskId), true
	}
	return -1, false
}

func (m *Master) AssignReduceTask()(taskId TaskId, ok bool) {
	m.reduceAssignAndCheckMutex.Lock()
	defer m.reduceAssignAndCheckMutex.Unlock()
	data,ok := m.unassignedReduceTasks.Pop()
	if ok {
		m.assignedReduceTasks.Add(data)
		_, nowSec := getTimeNowSecond()
		m.reduceTasks[data.(TaskId)].StartTime = nowSec //分配成功，记录任务开始时间
		return data.(TaskId), true
	}
	return -1, false
}




// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.allDone

	// Your code here.


	return ret
}

// 初始化map任务
func (m *Master) prepareMapTask() {
	// 简单处理，将files的索引下标作为map任务的TaskId
	for i := 0; i < m.nMap; i++ {
		tId := TaskId(i)
		m.mapTasks[tId] = &TaskInfo{
			TaskType: TaskTypeMap,
			TaskId: tId,
			WorkerId: -1,
			FileName: m.files[i],
		}
	}
	// 简单处理，将files的索引下标作为未分配map任务的TaskId, 一次性Push
	for i := 0; i < m.nMap; i++ {
		tId := TaskId(i)
		m.unassignedMapTasks.Push(tId)
	}
}

// 初始化reduce任务
func (m *Master) prepareReduceTask() {
	m.reducePrepareOnce.Do(func() {
		log.Println("reduce prepare begin, execute only once.")
		// 初始化任务队列
	    m.unassignedReduceTasks =  NewTaskQueue(m.nReduce, WaitTimeMil * time.Millisecond)
		m.assignedReduceTasks = mapset.NewSet()
		// 简单处理
		for i := 0; i < m.nReduce; i++ {
			tId := TaskId(i)
			m.reduceTasks[tId] = &TaskInfo{
				TaskType: TaskTypeReduce,
				TaskId: tId,
				WorkerId: -1,
				FileName: "",
			}
		}
		// 简单处理
		for i := 0; i < m.nReduce; i++ {
			tId := TaskId(i)
			m.unassignedReduceTasks.Push(tId)
		}
	})
}

// 超时还未完成的任务需要定时清除并重新分配
func (m *Master) loopReassignTimeoutTasks() {
	for {
		time.Sleep(2 * time.Second)
		if m.mapDone {
			m.reassignTimeoutReduceTasks()
		} else {
			m.reassignTimeoutMapTasks()
		}
	}
}

func (m *Master) reassignTimeoutMapTasks() {
	now, nowSec := getTimeNowSecond()
	iter := m.assignedMapTasks.Iter()
	// range中进行增删改不稳定，因此使用数组记录超时任务 https://www.jianshu.com/p/4205659ca419
	timeoutTasks := make([]TaskId, 0)
	for data := range  iter {
		taskId := data.(TaskId)
		if nowSec - m.mapTasks[taskId].StartTime > MaxExecuteTimeSec {
			timeoutTasks = append(timeoutTasks, taskId)
		}
	}

	if len(timeoutTasks) == 0 {
		log.Println("no timeout map task, skip it.")
	} else {
		// 加锁，和checkDone互斥
		m.mapAssignAndCheckMutex.Lock()
		defer m.mapAssignAndCheckMutex.Unlock()
		for _, taskId := range timeoutTasks {
			task := m.mapTasks[taskId]
			log.Printf("%s reassign task=%+v\n", now.Format("2006/01/02 15:04"), task)
			m.assignedMapTasks.Remove(taskId)
			m.unassignedMapTasks.Push(taskId)
		}
	}
}

func (m *Master) reassignTimeoutReduceTasks() {
	now, nowSec := getTimeNowSecond()
	iter := m.assignedReduceTasks.Iter()
	// range中进行增删改不稳定，因此使用数组记录超时任务 https://www.jianshu.com/p/4205659ca419
	timeoutTasks := make([]TaskId, 0)
	for data := range  iter {
		taskId := data.(TaskId)
		if nowSec - m.reduceTasks[taskId].StartTime > MaxExecuteTimeSec {
			timeoutTasks = append(timeoutTasks, taskId)
		}
	}

	if len(timeoutTasks) == 0 {
		log.Println("no timeout map task, skip it.")
	} else {
		// 加锁，和checkDone互斥
		m.reduceAssignAndCheckMutex.Lock()
		defer m.reduceAssignAndCheckMutex.Unlock()
		for _, taskId := range timeoutTasks {
			task := m.reduceTasks[taskId]
			log.Printf("%s reassign task=%+v\n", now.Format("2006/01/02 15:04"), task)
			m.assignedReduceTasks.Remove(taskId)
			m.unassignedReduceTasks.Push(taskId)
		}
	}
}

func getTimeNowSecond() (time.Time,int64){
	now := time.Now()
	return now, now.UnixNano() / int64(time.Second)
}


//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Your code here.
	m := &Master{
		files: files,
		nMap : len(files),
		nReduce: nReduce,
		allDone: false,
		mapDone: false,
		curWorker: -1,
		mapTasks: make(map[TaskId]*TaskInfo),
		reduceTasks: make(map[TaskId]*TaskInfo),
		unassignedMapTasks: NewTaskQueue(len(files), WaitTimeMil * time.Millisecond), //队列大小为map任务个数，因此不会Push阻塞
		assignedMapTasks:  mapset.NewSet(), // todo: 暂时使用线程安全的set，后续评估必要性
		unassignedReduceTasks: nil,
		assignedReduceTasks: nil,
	}

	m.server()

	m.prepareMapTask()
	go m.loopReassignTimeoutTasks()
	return m
}
