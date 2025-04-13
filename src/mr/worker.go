package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// 生成唯一的worker ID，用于日志区分
	workerId := os.Getpid()
	log.Printf("Worker %d: 开始运行", workerId)

	// 记录每个worker已经完成的任务，避免重复处理
	completedTasks := make(map[int]bool)

	// Your worker implementation here.
	for {
		task := requestTask()

		switch task.TaskType {
		case Map:
			// 检查是否已经处理过此任务
			if _, alreadyDone := completedTasks[task.TaskId]; alreadyDone {
				//log.Printf("Worker %d: 已经完成过Map任务 #%d，跳过", workerId, task.TaskId)
				// 再次通知Master任务已完成，以防之前的通知丢失
				callMapTaskComplete(task.TaskId)
			} else {
				//log.Printf("Worker %d: 接收到Map任务 #%d, 输入文件: %s", workerId, task.TaskId, task.InputFile)
				performMap(mapf, task, workerId)
				// 记录任务已完成
				completedTasks[task.TaskId] = true
			}
		case Reduce:
			// 检查是否已经处理过此任务
			if _, alreadyDone := completedTasks[task.TaskId+10000]; alreadyDone { // 加10000区分Map和Reduce任务ID
				log.Printf("Worker %d: 已经完成过Reduce任务 #%d，跳过", workerId, task.TaskId)
				// 再次通知Master任务已完成
				callReduceTaskComplete(task.TaskId)
			} else {
				//log.Printf("Worker %d: 接收到Reduce任务 #%d", workerId, task.TaskId)
				performReduce(reducef, task, workerId)
				// 记录任务已完成
				completedTasks[task.TaskId+10000] = true
			}
		case Wait:
			log.Printf("Worker %d: 当前没有可用任务，等待中...", workerId)
			// 等待一段时间后再次请求任务
			time.Sleep(1 * time.Second)
		case Exit:
			log.Printf("Worker %d: 所有任务已完成，退出", workerId)
			return
		}
	}
}

func requestTask() TaskReply {
	args := EmptyArgs{}
	reply := TaskReply{}

	if ok := call("Master.RequestTask", &args, &reply); !ok {
		os.Exit(0)
	}
	return reply
}

func performMap(mapf func(string, string) []KeyValue, task TaskReply, workerId int) {
	startTime := time.Now()
	log.Printf("Worker %d: 开始执行Map任务 #%d", workerId, task.TaskId)

	filename := task.InputFile
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Worker %d: 无法打开文件 %v: %v", workerId, filename, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Worker %d: 无法读取文件 %v: %v", workerId, filename, err)
	}
	file.Close()

	//log.Printf("Worker %d: Map任务 #%d 读取文件完成，文件大小: %d 字节", workerId, task.TaskId, len(content))

	kva := mapf(filename, string(content))
	//log.Printf("Worker %d: Map任务 #%d 生成了 %d 个中间键值对", workerId, task.TaskId, len(kva))

	// 将中间结果分成R个桶
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce
		intermediate[bucket] = append(intermediate[bucket], kv)
	}

	// 将中间结果写入临时文件
	for i := 0; i < task.NReduce; i++ {
		tempFile, err := ioutil.TempFile(".", "map-*")
		if err != nil {
			log.Fatalf("Worker %d: 无法创建临时文件: %v", workerId, err)
		}

		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediate[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("Worker %d: 无法编码键值对 %v: %v", workerId, kv, err)
			}
		}

		tempFile.Close()

		// 重命名为最终文件
		finalName := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		os.Rename(tempFile.Name(), finalName)
		//log.Printf("Worker %d: 创建中间文件 %s，包含 %d 个键值对", workerId, finalName, len(intermediate[i]))
	}

	// 通知Master任务完成
	callMapTaskComplete(task.TaskId)
	elapsed := time.Since(startTime)
	log.Printf("Worker %d: Map任务 #%d 完成，耗时: %.2f 秒", workerId, task.TaskId, elapsed.Seconds())
}

func performReduce(reducef func(string, []string) string, task TaskReply, workerId int) {
	startTime := time.Now()
	log.Printf("Worker %d: 开始执行Reduce任务 #%d", workerId, task.TaskId)

	// 收集该Reduce任务的所有中间文件
	var intermediate []KeyValue
	var totalFiles int
	for i := 0; i < task.NMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Worker %d: 无法打开中间文件 %v: %v", workerId, filename, err)
		}

		dec := json.NewDecoder(file)
		fileCount := 0
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
			fileCount++
		}
		totalFiles++
		//log.Printf("Worker %d: 从中间文件 %s 读取了 %d 个键值对", workerId, filename, fileCount)
		file.Close()
	}

	//log.Printf("Worker %d: Reduce任务 #%d 从 %d 个中间文件读取了 %d 个键值对",
	//	workerId, task.TaskId, totalFiles, len(intermediate))

	// 按键排序
	sort.Sort(ByKey(intermediate))
	//log.Printf("Worker %d: Reduce任务 #%d 已对键值对排序完成", workerId, task.TaskId)

	// 创建输出文件
	tempFile, err := ioutil.TempFile(".", "reduce-*")
	if err != nil {
		log.Fatalf("Worker %d: 无法创建临时输出文件: %v", workerId, err)
	}

	// 对每个key调用reducef函数
	i := 0
	uniqueKeys := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		uniqueKeys++

		// 将结果写入文件
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	tempFile.Close()

	// 重命名为最终文件
	finalName := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tempFile.Name(), finalName)
	//log.Printf("Worker %d: Reduce任务 #%d 处理了 %d 个唯一键，结果保存到 %s",
		//workerId, task.TaskId, uniqueKeys, finalName)

	// 通知Master任务完成
	callReduceTaskComplete(task.TaskId)
	elapsed := time.Since(startTime)
	log.Printf("Worker %d: Reduce任务 #%d 完成，耗时: %.2f 秒", workerId, task.TaskId, elapsed.Seconds())
}

func callMapTaskComplete(taskId int) {
	args := MapTaskCompleteArgs{TaskId: taskId}
	reply := EmptyReply{}
	// 增加重试机制，确保完成通知能够送达
	success := false
	for retries := 0; retries < 3 && !success; retries++ {
		success = call("Master.ReceiveFinishedMap", &args, &reply)
		if !success {
			log.Printf("通知Map任务 #%d 完成失败，重试中 (%d/3)", taskId, retries+1)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func callReduceTaskComplete(taskId int) {
	args := ReduceTaskCompleteArgs{TaskId: taskId}
	reply := EmptyReply{}
	// 增加重试机制，确保完成通知能够送达
	success := false
	for retries := 0; retries < 3 && !success; retries++ {
		success = call("Master.ReceiveFinishedReduce", &args, &reply)
		if !success {
			log.Printf("通知Reduce任务 #%d 完成失败，重试中 (%d/3)", taskId, retries+1)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
    sockname := masterSock()
    c, err := rpc.DialHTTP("unix", sockname)
    if err != nil {
        log.Printf("**********RPC连接失败: %v************", err)
        return false
    }
    
    // 在可能返回的地方添加 defer，避免提前返回时也执行 c.Close()
    defer func() {
        if c != nil {
            c.Close()
        }
    }()

    err = c.Call(rpcname, args, reply)
    if err == nil {
        return true
    }

    // fmt.Println(err)
    return false
}