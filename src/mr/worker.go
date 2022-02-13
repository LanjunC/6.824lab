package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type AWorker struct {
	workerId WorkerId
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
}

func (w *AWorker) prepare() *AWorker{
	return w
}

func (w *AWorker) process() *AWorker{
	FOR:
	for {
		args := AssignTaskArgs{
			w.workerId,
		}
		reply := AssignTaskReply{}
		call("Master.AssignTask", &args, &reply)
		w.workerId = reply.WorkerId
		switch reply.TaskType {
		case TaskTypeMap:
			w.executeMap(&reply)
		case TaskTypeReduce:
			w.executeReduce(&reply)
		case TaskTypeAllDone:
			// todo
			break FOR
		case TaskTypeWait:
			time.Sleep(1 * time.Second)
		default:
			log.Fatalf("invalid tasktype:%v", reply.TaskType)
		}
	}


	return w
}

func (w *AWorker) executeMap(reply *AssignTaskReply) {
	intermediateKvs := w.makeMapIntermediate(reply.FileName)
	w.writeMapIntermediateToFile(intermediateKvs, reply.NMap, reply.NReduce, reply.TaskId).endExecuteMap(reply.TaskInfo)
}

// 读取目标文件，执行用户定义的map任务生成中间键值对
func (w *AWorker) makeMapIntermediate(fileName string) []KeyValue {
	// 文件路径 todo: 应在master上就已确定而不是发送fileName再由worker拼接成path
	path := fileName
	f, err := os.Open(path)
	if err != nil {
		log.Panicf("open file failed, err=%v", err) //todo: 优化错误处理
	}
	defer f.Close()
	content, err := ioutil.ReadAll(f)
	if err != nil {
		log.Panicf("read content from file failed, err=%v", err)
	}
	kvs := w.mapf(fileName, string(content))
	return kvs
}

// 中间键值对写入文件 todo: 优化性能
func (w *AWorker) writeMapIntermediateToFile(intermediate []KeyValue, nMap, nReduce int, taskId TaskId) *AWorker{
	// 根据nReduce将中间值分成若干组kvs
	kvas := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		kvas[i] = make([]KeyValue, 0)
	}
	for _, kv := range intermediate {
		index := ihash(kv.Key) % nReduce
		kvas[index] = append(kvas[index], kv)
	}

	// 刚得到的各组kvs写入临时文件，防止进程中断生成不完整的中间文件，成功后重命名文件
	for i, kvs := range kvas {
		// 当前目录生成临时文件
		tmpFile, err := os.CreateTemp(".", "tmp*")
		if err != nil {
			log.Panicf("create temp file failed, err=%v", err)
		}
		defer  tmpFile.Close()
		// encode kv
		encoder := json.NewEncoder(tmpFile)
		for _, kv := range kvs {
			err := encoder.Encode(&kv)
			if err != nil {
				log.Panicf("encode kv failed, kv=%+v, err=%v", kv, err)
			}
		}

		// 成功写入临时文件后重命名成mr-m-n格式中间文件
		fileName := fmt.Sprintf("mr-%v-%v", taskId, i)
		err = os.Rename(tmpFile.Name(), fileName)
		if err != nil {
			log.Panicf("rename oldFile=[%v] to new File=[%v] failed, err=%v", tmpFile.Name(), fileName, err)
		}
		log.Printf("rename oldFile=[%v] to new File=[%v] successfully\n", tmpFile.Name(), fileName)
	}
	return w
}

// 通知master任务完成
func (w *AWorker) endExecuteMap(taskInfo TaskInfo) *AWorker{
	args := EndTaskArgs{
		taskInfo,
	}
	reply := EndTaskReply{}
	call("Master.EndTask", &args, &reply)
	if reply.Accepted {
		log.Println("map task accepted.")
	} else {
		log.Println("map task refused.")
	}
	return w
}

func (w *AWorker) executeReduce(reply *AssignTaskReply) {
	intermediateKvs := w.makeReduceIntermediate(reply.TaskId, reply.NMap)
	w.processIntermediate(intermediateKvs, reply.TaskId).endExecuteReduce(reply.TaskInfo)




	// 将结果通过临时文件最终写入mr-out-*文件

}

// 读取目标中间文件的kv，生成中间变量
func (w *AWorker) makeReduceIntermediate(taskId TaskId, nMap int) []KeyValue {
	intermediate := make([]KeyValue, 0)
	for i:= 0; i < nMap; i++ {
		// todo: 文件路径和名应由master告知
		fileName := fmt.Sprintf("mr-%v-%v", i, taskId)
		f, err := os.Open(fileName)

		if err != nil {
			log.Panicf("open file failed, err=%v", err) //todo: 优化错误处理
		}
		defer f.Close()
		decoder := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err!= nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	return intermediate
}

// 处理中间变量，执行reduce，并写入最终文件
func (w *AWorker) processIntermediate(intermediateKvs []KeyValue, taskId TaskId) *AWorker{
	// 当前目录生成临时文件
	tmpFile, err := os.CreateTemp(".", "tmp*")
	if err != nil {
		log.Panicf("create temp file failed, err=%v", err)
	}
	defer tmpFile.Close()

	// sort中间变量
	sort.Sort(ByKey(intermediateKvs))

	// 将key相同的value放一起，执行reduce并将结果写入临时文件
	for i:= 0; i < len(intermediateKvs); {
		key := intermediateKvs[i].Key
		j := i + 1
		for ;j < len(intermediateKvs) && intermediateKvs[j].Key == key; j++{}
		value := make([]string, 0)
		for k := i; k < j; k++ {
			value = append(value, intermediateKvs[k].Value)
		}
		reduceRes := w.reducef(intermediateKvs[i].Key, value)
		//reduce结果写入临时文件
		_, err = fmt.Fprintf(tmpFile, "%v %v\n", key, reduceRes)
		if err != nil {
			log.Panicf(" write reduce result failed. err=%v", err)
		}
		i = j
	}

	// 成功写入临时文件后重命名成mr-m-n格式中间文件
	fileName := fmt.Sprintf("mr-out-%v", taskId)
	err = os.Rename(tmpFile.Name(), fileName)
	if err != nil {
		log.Panicf("rename oldFile=[%v] to new File=[%v] failed, err=%v", tmpFile.Name(), fileName, err)
	}
	log.Printf("rename oldFile=[%v] to new File=[%v] successfully\n", tmpFile.Name(), fileName)

	return w
}

func (w *AWorker) endExecuteReduce(taskInfo TaskInfo) *AWorker{
	args := EndTaskArgs{
		taskInfo,
	}
	reply := EndTaskReply{}
	call("Master.EndTask", &args, &reply)
	if reply.Accepted {
		log.Println("reduce task accepted.")
	} else {
		log.Println("reduce task refused.")
	}

	return w
}


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	worker := &AWorker{
		workerId: -1,
		mapf:     mapf,
		reducef:  reducef,
	}

	worker.prepare().process()

	// uncomment to send the Example RPC to the master.
	//CallExample()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
