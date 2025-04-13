package mr

import (
	"log"
	"math/rand" // 导入随机数包
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	mutex                sync.Mutex // 添加互斥锁保护共享数据
	mapTasks             []Task
	reduceTasks          []Task
	nReduce              int
	nMap                 int
	completedMapTasks    int
	completedReduceTasks int
	phase                Phase     // 添加阶段标记，用于区分Map和Reduce阶段
	startTime            time.Time // 添加开始时间字段，用于统计整体任务执行时间
	mapPhaseStartTime    time.Time // 添加Map阶段开始时间
	reducePhaseStartTime time.Time // 添加Reduce阶段开始时间
}

type Task struct {
	TaskId    int
	InputFile string
	Status    TaskStatus
	StartTime time.Time
}

type TaskStatus int
type Phase int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

const (
	MapPhase Phase = iota
	ReducePhase
	CompletePhase
)

const MapTaskTimeout = 15 * time.Second
const ReduceTaskTimeout = 3 * time.Second

// Your code here -- RPC handlers for the worker to call.
func (m *Master) ReceiveFinishedMap(args *MapTaskCompleteArgs, reply *EmptyReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	taskId := args.TaskId

	// 无论当前状态如何，都将任务标记为已完成
	// 这样可以确保即使通知到达时任务因超时而被重置，我们仍然接受完成状态
	if m.mapTasks[taskId].Status != Completed {
		m.mapTasks[taskId].Status = Completed
		m.completedMapTasks++

		log.Printf("Master: Map任务 #%d 已完成，当前完成 %d/%d", taskId, m.completedMapTasks, m.nMap)

		// 如果所有Map任务完成，切换到Reduce阶段
		if m.completedMapTasks == m.nMap && m.phase == MapPhase {
			mapPhaseDuration := time.Since(m.mapPhaseStartTime)
			log.Printf("******************Master: Map阶段完成，用时: %v*************", mapPhaseDuration)

			m.phase = ReducePhase
			m.reducePhaseStartTime = time.Now()
			log.Printf("Master: 所有Map任务完成，切换到Reduce阶段")
		}
	} else {
		log.Printf("Master: 收到Map任务 #%d 的重复完成通知，忽略", taskId)
	}

	return nil
}

func (m *Master) ReceiveFinishedReduce(args *ReduceTaskCompleteArgs, reply *EmptyReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	taskId := args.TaskId

	// 无论当前状态如何，都将任务标记为已完成
	if m.reduceTasks[taskId].Status != Completed {
		m.reduceTasks[taskId].Status = Completed
		m.completedReduceTasks++

		log.Printf("Master: Reduce任务 #%d 已完成，当前完成 %d/%d", taskId, m.completedReduceTasks, m.nReduce)

		// 如果所有Reduce任务完成，切换到Complete阶段
		if m.completedReduceTasks == m.nReduce && m.phase == ReducePhase {
			reducePhaseDuration := time.Since(m.reducePhaseStartTime)
			totalDuration := time.Since(m.startTime)
			log.Printf("***************Master: Reduce阶段完成，用时: %v*************", reducePhaseDuration)
			log.Printf("*****************Master: 整个MapReduce任务完成，总用时: %v**********************", totalDuration)

			m.phase = CompletePhase
			log.Printf("Master: 所有Reduce任务完成，MapReduce任务全部完成")
		}
	} else {
		log.Printf("Master: 收到Reduce任务 #%d 的重复完成通知，忽略", taskId)
	}

	return nil
}

func (m *Master) RequestTask(args *EmptyArgs, reply *TaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// 根据当前阶段分配任务
	if m.phase == MapPhase {
		// 收集所有空闲的Map任务索引
		idleTasks := []int{}
		for i, task := range m.mapTasks {
			if task.Status == Idle {
				idleTasks = append(idleTasks, i)
			}
		}

		// 如果有空闲任务，随机选择一个
		if len(idleTasks) > 0 {
			// 随机选择一个任务索引
			taskIdx := idleTasks[rand.Intn(len(idleTasks))]

			m.mapTasks[taskIdx].Status = InProgress
			m.mapTasks[taskIdx].StartTime = time.Now()

			reply.TaskType = Map
			reply.TaskId = taskIdx
			reply.InputFile = m.mapTasks[taskIdx].InputFile
			reply.NReduce = m.nReduce
			reply.NMap = m.nMap

			log.Printf("Master: 随机分配Map任务 #%d，输入文件: %s", taskIdx, m.mapTasks[taskIdx].InputFile)
			return nil
		}

		// 检查是否所有Map任务都已完成或正在处理中
		allMapsInProgressOrCompleted := true
		for _, task := range m.mapTasks {
			if task.Status == Idle {
				allMapsInProgressOrCompleted = false
				break
			}
		}

		if allMapsInProgressOrCompleted {
			log.Printf("Master: 所有Map任务已分配或完成，让worker等待")
			reply.TaskType = Wait
			return nil
		}

		// 没有可用的Map任务，让worker等待
		reply.TaskType = Wait
		log.Printf("Master: 当前没有可用的Map任务，让worker等待")
		return nil
	} else if m.phase == ReducePhase {
		// 收集所有空闲的Reduce任务索引
		idleTasks := []int{}
		for i, task := range m.reduceTasks {
			if task.Status == Idle {
				idleTasks = append(idleTasks, i)
			}
		}

		// 如果有空闲任务，随机选择一个
		if len(idleTasks) > 0 {
			// 随机选择一个任务索引
			taskIdx := idleTasks[rand.Intn(len(idleTasks))]

			m.reduceTasks[taskIdx].Status = InProgress
			m.reduceTasks[taskIdx].StartTime = time.Now()

			reply.TaskType = Reduce
			reply.TaskId = taskIdx
			reply.NReduce = m.nReduce
			reply.NMap = m.nMap

			log.Printf("Master: 随机分配Reduce任务 #%d", taskIdx)
			return nil
		}

		// 检查是否所有Reduce任务都已完成或正在处理中
		allReducesInProgressOrCompleted := true
		for _, task := range m.reduceTasks {
			if task.Status == Idle {
				allReducesInProgressOrCompleted = false
				break
			}
		}

		if allReducesInProgressOrCompleted {
			log.Printf("Master: 所有Reduce任务已分配或完成，让worker等待")
			reply.TaskType = Wait
			return nil
		}

		// 没有可用的Reduce任务，让worker等待
		reply.TaskType = Wait
		log.Printf("Master: 当前没有可用的Reduce任务，让worker等待")
		return nil
	} else {
		// 任务已完成，通知worker退出
		reply.TaskType = Exit
		log.Printf("Master: 任务已全部完成，通知worker退出")
		return nil
	}
}

// start a thread that listens for RPCs from worker.go
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.phase == CompletePhase
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	// 初始化随机数种子，确保每次运行生成不同的随机序列
	rand.Seed(time.Now().UnixNano())

	m := Master{}

	// 初始化Master
	m.nMap = len(files)
	m.nReduce = nReduce
	m.phase = MapPhase
	m.completedMapTasks = 0
	m.completedReduceTasks = 0
	m.startTime = time.Now()         // 记录总开始时间
	m.mapPhaseStartTime = time.Now() // 记录Map阶段开始时间

	log.Printf("Master: 初始化，Map任务数: %d, Reduce任务数: %d", m.nMap, m.nReduce)
	log.Printf("**************Master: 开始执行MapReduce任务，开始时间: %v*****************", m.startTime.Format("2006-01-02 15:04:05"))

	// 初始化Map任务
	m.mapTasks = make([]Task, m.nMap)
	for i, file := range files {
		m.mapTasks[i] = Task{
			TaskId:    i,
			InputFile: file,
			Status:    Idle,
		}
		log.Printf("Master: 创建Map任务 #%d，输入文件: %s", i, file)
	}

	// 初始化Reduce任务
	m.reduceTasks = make([]Task, nReduce)
	for i := 0; i < nReduce; i++ {
		m.reduceTasks[i] = Task{
			TaskId: i,
			Status: Idle,
		}
		log.Printf("Master: 创建Reduce任务 #%d", i)
	}

	// 启动后台任务检查，重新分配超时任务
	go m.checkTaskTimeouts()

	m.server()
	log.Printf("Master: 服务启动完成，等待worker连接")
	return &m
}

// 后台检查超时任务
func (m *Master) checkTaskTimeouts() {
	for {
		time.Sleep(1 * time.Second)

		m.mutex.Lock()
		if m.phase == CompletePhase {
			m.mutex.Unlock()
			return
		}

		// 检查Map任务超时
		if m.phase == MapPhase {
			for i := range m.mapTasks {
				// 只检查处于InProgress状态的任务，已完成的任务不检查
				if m.mapTasks[i].Status == InProgress &&
					time.Since(m.mapTasks[i].StartTime) > MapTaskTimeout {
					// 确保任务尚未收到完成通知
					log.Printf("Master: 检测到Map任务 #%d 超时，重置为待分配状态", i)
					m.mapTasks[i].Status = Idle
				}
			}

			// 检查是否所有Map任务已完成
			allMapsDone := true
			for _, task := range m.mapTasks {
				if task.Status != Completed {
					allMapsDone = false
					break
				}
			}

			if allMapsDone && m.phase == MapPhase {
				mapPhaseDuration := time.Since(m.mapPhaseStartTime)
				log.Printf("********************Master: Map阶段完成，用时: %v*******************", mapPhaseDuration)

				m.phase = ReducePhase
				m.reducePhaseStartTime = time.Now()
				log.Printf("Master: 所有Map任务完成，切换到Reduce阶段")
			}
		}

		// 检查Reduce任务超时和完成情况
		if m.phase == ReducePhase {
			for i := range m.reduceTasks {
				// 只检查处于InProgress状态的任务
				if m.reduceTasks[i].Status == InProgress &&
					time.Since(m.reduceTasks[i].StartTime) > ReduceTaskTimeout {
					log.Printf("Master: 检测到Reduce任务 #%d 超时，重置为待分配状态", i)
					m.reduceTasks[i].Status = Idle
				}
			}

			// 检查是否所有Reduce任务已完成
			allReducesDone := true
			for _, task := range m.reduceTasks {
				if task.Status != Completed {
					allReducesDone = false
					break
				}
			}

			if allReducesDone && m.phase == ReducePhase {
				reducePhaseDuration := time.Since(m.reducePhaseStartTime)
				totalDuration := time.Since(m.startTime)
				log.Printf("Master: Reduce阶段完成，用时: %v", reducePhaseDuration)
				log.Printf("*****************Master: 整个MapReduce任务完成，总用时: %v*************************", totalDuration)

				m.phase = CompletePhase
				log.Printf("Master: 所有Reduce任务完成，MapReduce任务全部完成")
			}
		}
		m.mutex.Unlock()
	}
}
