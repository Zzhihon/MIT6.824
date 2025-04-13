package mr

import (
	"log"
	"math/rand" // 添加随机数包
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Master struct {
	nReduce              int
	nMap                 int
	phase                Phase
	mapTasks             []Task
	reduceTasks          []Task
	completedMapTasks    int
	completedReduceTasks int
	startTime            time.Time
	mapPhaseStartTime    time.Time
	reducePhaseStartTime time.Time

	// 定义channel用于消息传递
	requestTaskCh    chan requestTaskMsg
	mapCompleteCh    chan mapCompleteMsg
	reduceCompleteCh chan reduceCompleteMsg
	timeoutCheckCh   chan struct{}  // 新增专用于超时检查的通道
	statusCh         chan chan bool // 增加一个状态标志通道，用于检查状态而不触发任务分配
}

type Task struct {
	TaskId    int
	InputFile string
	Status    TaskStatus
	StartTime time.Time
}

// 定义消息结构体
type requestTaskMsg struct {
	response *TaskReply
	done     chan struct{}
}

type mapCompleteMsg struct {
	taskId int
	done   chan struct{}
}

type reduceCompleteMsg struct {
	taskId int
	done   chan struct{}
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

const MapTaskTimeout = 25 * time.Second
const ReduceTaskTimeout = 3 * time.Second

// RPC处理器 - 请求任务
func (m *Master) RequestTask(args *EmptyArgs, reply *TaskReply) error {
	msg := requestTaskMsg{
		response: reply,
		done:     make(chan struct{}),
	}

	// 发送消息到通道并等待处理完成
	m.requestTaskCh <- msg
	<-msg.done

	return nil
}

// RPC处理器 - Map任务完成
func (m *Master) ReceiveFinishedMap(args *MapTaskCompleteArgs, reply *EmptyReply) error {
	msg := mapCompleteMsg{
		taskId: args.TaskId,
		done:   make(chan struct{}),
	}

	m.mapCompleteCh <- msg
	<-msg.done

	return nil
}

// RPC处理器 - Reduce任务完成
func (m *Master) ReceiveFinishedReduce(args *ReduceTaskCompleteArgs, reply *EmptyReply) error {
	msg := reduceCompleteMsg{
		taskId: args.TaskId,
		done:   make(chan struct{}),
	}

	m.reduceCompleteCh <- msg
	<-msg.done

	return nil
}

// 主调度循环
func (m *Master) schedule() {
	// 启动后台超时检查
	go m.checkTaskTimeouts()

	for {
		select {
		case msg := <-m.requestTaskCh:
			m.handleRequestTask(msg)

		case msg := <-m.mapCompleteCh:
			m.handleMapComplete(msg)

		case msg := <-m.reduceCompleteCh:
			m.handleReduceComplete(msg)

		case <-m.timeoutCheckCh:
			// 专门处理超时检查，不分配任务
			m.handleTimeoutCheck()

		case ch := <-m.statusCh:
			// 仅返回当前阶段是否已完成，不执行任何状态变更
			ch <- (m.phase == CompletePhase)
		}
	}
}

// 处理请求任务消息
func (m *Master) handleRequestTask(msg requestTaskMsg) {
	reply := msg.response

	// 根据当前阶段分配任务
	if m.phase == MapPhase {
		// 使用随机顺序分配Map任务
		availableTasks := []int{}
		for i := range m.mapTasks {
			if m.mapTasks[i].Status == Idle {
				availableTasks = append(availableTasks, i)
			}
		}

		if len(availableTasks) > 0 {
			// 随机选择一个可用任务
			taskIdx := availableTasks[rand.Intn(len(availableTasks))]

			m.mapTasks[taskIdx].Status = InProgress
			m.mapTasks[taskIdx].StartTime = time.Now()

			reply.TaskType = Map
			reply.TaskId = taskIdx
			reply.InputFile = m.mapTasks[taskIdx].InputFile
			reply.NReduce = m.nReduce
			reply.NMap = m.nMap

			log.Printf("Master: 分配Map任务 #%d，输入文件: %s", taskIdx, m.mapTasks[taskIdx].InputFile)
			msg.done <- struct{}{}
			return
		}

		// 检查是否所有Map任务都已完成或正在处理中
		allMapsInProgressOrCompleted := true
		for _, task := range m.mapTasks {
			if task.Status == Idle || task.Status == InProgress {
				// 如果有任何Map任务处于Idle或InProgress状态，则表示还有未完成的任务
				allMapsInProgressOrCompleted = false
				break
			}
		}

		if allMapsInProgressOrCompleted {
			log.Printf("Master: 所有Map任务已分配或完成，让worker等待")
			reply.TaskType = Wait
			msg.done <- struct{}{}
			return
		}

		// 没有可用的Map任务，让worker等待
		reply.TaskType = Wait
		log.Printf("Master: 当前没有可用的Map任务，让worker等待")
		msg.done <- struct{}{}
		return
	} else if m.phase == ReducePhase {
		// 使用随机顺序分配Reduce任务
		availableTasks := []int{}
		for i := range m.reduceTasks {
			if m.reduceTasks[i].Status == Idle {
				availableTasks = append(availableTasks, i)
			}
		}

		if len(availableTasks) > 0 {
			// 随机选择一个可用任务
			taskIdx := availableTasks[rand.Intn(len(availableTasks))]

			m.reduceTasks[taskIdx].Status = InProgress
			m.reduceTasks[taskIdx].StartTime = time.Now()

			reply.TaskType = Reduce
			reply.TaskId = taskIdx
			reply.NReduce = m.nReduce
			reply.NMap = m.nMap

			log.Printf("Master: 分配Reduce任务 #%d", taskIdx)
			msg.done <- struct{}{}
			return
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
			msg.done <- struct{}{}
			return
		}

		// 没有可用的Reduce任务，让worker等待
		reply.TaskType = Wait
		log.Printf("Master: 当前没有可用的Reduce任务，让worker等待")
		msg.done <- struct{}{}
		return
	} else {
		// 任务已完成，通知worker退出
		reply.TaskType = Exit
		log.Printf("Master: 任务已全部完成，通知worker退出")
		msg.done <- struct{}{}
		return
	}
}

// 处理Map任务完成消息
func (m *Master) handleMapComplete(msg mapCompleteMsg) {
	taskId := msg.taskId

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

	msg.done <- struct{}{}
}

// 处理Reduce任务完成消息
func (m *Master) handleReduceComplete(msg reduceCompleteMsg) {
	taskId := msg.taskId

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

	msg.done <- struct{}{}
}

// 新增专门处理超时检查的函数
func (m *Master) handleTimeoutCheck() {
	// 只检查超时，不分配任务

	// 检查Map任务超时
	if m.phase == MapPhase {
		for i := range m.mapTasks {
			if m.mapTasks[i].Status == InProgress &&
				time.Since(m.mapTasks[i].StartTime) > MapTaskTimeout {
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

	// 检查Reduce任务超时
	if m.phase == ReducePhase {
		for i := range m.reduceTasks {
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
			log.Printf("***************Master: Reduce阶段完成，用时: %v*************", reducePhaseDuration)
			log.Printf("*****************Master: 整个MapReduce任务完成，总用时: %v**********************", totalDuration)

			m.phase = CompletePhase
			log.Printf("Master: 所有Reduce任务完成，MapReduce任务全部完成")
		}
	}
}

// 修改后台检查超时任务的方法，使用专门的通道
func (m *Master) checkTaskTimeouts() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		<-ticker.C

		// 先检查是否已经完成，避免不必要的通道操作
		isDone := false
		statusCh := make(chan bool)

		// 利用状态通道检查当前状态
		select {
		case m.statusCh <- statusCh:
			isDone = <-statusCh
		default:
			// 如果状态通道阻塞，跳过检查
			continue
		}

		if isDone {
			return
		}

		// 发送超时检查信号
		select {
		case m.timeoutCheckCh <- struct{}{}:
			// 成功发送
		default:
			// 通道阻塞，跳过此次检查
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if err := e; err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	// 创建一个布尔型通道，用于获取状态而不触发任务分配
	statusCh := make(chan bool)

	// 发送通道到master，并等待回复
	m.statusCh <- statusCh
	isDone := <-statusCh

	return isDone
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	// 初始化随机数种子
	rand.Seed(time.Now().UnixNano())

	m := Master{}

	// 初始化通道
	m.requestTaskCh = make(chan requestTaskMsg)
	m.mapCompleteCh = make(chan mapCompleteMsg)
	m.reduceCompleteCh = make(chan reduceCompleteMsg)
	m.timeoutCheckCh = make(chan struct{}) // 初始化新的超时检查通道
	m.statusCh = make(chan chan bool)      // 初始化状态检查通道

	// 初始化Master
	m.nMap = len(files)
	m.nReduce = nReduce
	m.phase = MapPhase
	m.completedMapTasks = 0
	m.completedReduceTasks = 0
	m.startTime = time.Now()
	m.mapPhaseStartTime = time.Now()

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

	m.server()
	log.Printf("Master: 服务启动完成，等待worker连接")

	// 启动主调度循环
	go m.schedule()

	return &m
}
