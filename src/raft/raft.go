package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

// ApplyMsg 是 Raft 发送到应用层的消息结构
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// 对于 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 服务器角色常量
const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

// LogEntry 表示日志条目
type LogEntry struct {
	Term    int         // 该条目被领导者接收时的任期
	Command interface{} // 状态机的命令
}

// Raft结构体扩展
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 所有服务器持久化状态
	currentTerm int        // 服务器知道的最新任期
	votedFor    int        // 当前任期内收到投票的候选人ID，如果没有投给任何候选人则为-1
	log         []LogEntry // 日志条目

	// 所有服务器易失性状态
	commitIndex int // 已知已提交的最高日志条目索引
	lastApplied int // 已应用到状态机的最高日志条目索引

	// 领导人易失性状态（重新选举后重置）
	nextIndex  []int // 对每个服务器，发送到该服务器的下一个日志条目的索引
	matchIndex []int // 对每个服务器，已知在该服务器上复制的最高日志条目索引

	// 额外状态
	role              int           // 服务器角色：Follower, Candidate 或 Leader
	electionTimer     *time.Timer   // 选举超时计时器
	heartbeatInterval time.Duration // 心跳间隔
	applyCh           chan ApplyMsg // 应用消息的通道

	// 选举相关
	votesReceived int       // 获得的选票数
	lastHeartbeat time.Time // 上次收到心跳的时间
}

// 修改GetState函数实现
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := (rf.role == Leader)

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // 候选人的任期号
	CandidateId  int // 请求投票的候选人ID
	LastLogIndex int // 候选人最后日志条目的索引
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // 当前任期号，候选人用来更新自己
	VoteGranted bool // 如果候选人收到投票则为true
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	log.Printf("Server %d [Term %d, Role %v]: Received RequestVote from Server %d [Term %d]",
		rf.me, rf.currentTerm, roleToString(rf.role), args.CandidateId, args.Term)

	// 如果候选人的任期小于自己的任期，拒绝投票
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		log.Printf("Server %d: Rejected vote for Server %d (higher term: %d > %d)",
			rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}

	// 如果收到更高的任期，转为follower
	if args.Term > rf.currentTerm {
		log.Printf("Server %d: Discovered higher term %d from Server %d, becoming follower",
			rf.me, args.Term, args.CandidateId)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}

	reply.Term = rf.currentTerm

	// 检查是否可以投票（没投过票或已经投给了请求者）
	// 并且候选人的日志至少和自己一样新
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	logOk := (args.LastLogTerm > lastLogTerm) ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logOk {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now() // 重置选举计时器
		rf.persist()
		log.Printf("Server %d: Granted vote to Server %d for term %d",
			rf.me, args.CandidateId, rf.currentTerm)
	} else {
		reply.VoteGranted = false
		log.Printf("Server %d: Denied vote to Server %d (votedFor=%d, logOk=%v)",
			rf.me, args.CandidateId, rf.votedFor, logOk)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// example AppendEntries RPC参数和回复结构体
type AppendEntriesArgs struct {
	Term         int        // 领导人的任期
	LeaderId     int        // 领导人ID，跟随者可以重定向客户端
	PrevLogIndex int        // 新日志条目之前的索引
	PrevLogTerm  int        // PrevLogIndex条目的任期
	Entries      []LogEntry // 要存储的日志条目（为空时是心跳）
	LeaderCommit int        // 领导人的commitIndex
}

type AppendEntriesReply struct {
	Term    int  // 当前任期，领导人用来更新自己
	Success bool // 如果follower包含匹配prevLogIndex和prevLogTerm的条目则为true
}

// example AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isHeartbeat := len(args.Entries) == 0
	if isHeartbeat {
		log.Printf("Server %d [Term %d, Role %v]: Received heartbeat from Leader %d [Term %d]",
			rf.me, rf.currentTerm, roleToString(rf.role), args.LeaderId, args.Term)
	} else {
		log.Printf("Server %d [Term %d]: Received %d log entries from Leader %d [Term %d]",
			rf.me, rf.currentTerm, len(args.Entries), args.LeaderId, args.Term)
	}

	// 如果领导人的任期小于自己的任期，拒绝附加日志
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		log.Printf("Server %d: Rejected AppendEntries from Server %d (higher term: %d > %d)",
			rf.me, args.LeaderId, rf.currentTerm, args.Term)
		return
	}

	// 重置选举超时计时器，因为收到了有效的领导人消息
	rf.lastHeartbeat = time.Now()

	// 如果收到更高任期的RPC，更新自己的任期并转为follower
	if args.Term > rf.currentTerm {
		log.Printf("Server %d: Discovered higher term %d from Leader %d, becoming follower",
			rf.me, args.Term, args.LeaderId)
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
	}

	// 只要是收到AppendEntries RPC，就认可发送者是领导人
	if rf.role == Candidate && args.Term >= rf.currentTerm {
		log.Printf("Server %d: Stepping down from candidate to follower for term %d",
			rf.me, rf.currentTerm)
		rf.role = Follower
	}

	reply.Term = rf.currentTerm
	reply.Success = true

	// 此处简化处理心跳逻辑，完整日志复制部分在2B中实现
}

// example code to send a AppendEntries RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 修改ticker函数实现选举超时检测
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// 检查是否需要开始选举
		ms := 300 + (rand.Int63() % 300) // 随机超时时间：300-600ms
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()

		// 如果长时间未收到心跳（超过选举超时），启动选举
		electionTimeout := time.Duration(ms) * time.Millisecond
		if time.Since(rf.lastHeartbeat) > electionTimeout && rf.role != Leader {
			log.Printf("Server %d [Term %d, Role %v]: Election timeout after %v ms, starting election",
				rf.me, rf.currentTerm, roleToString(rf.role), ms)
			rf.startElection()
		}

		rf.mu.Unlock()
	}
}

// 启动选举过程
func (rf *Raft) startElection() {
	oldTerm := rf.currentTerm
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesReceived = 1 // 投给自己
	rf.persist()

	log.Printf("Server %d: Starting election for term %d (was term %d)",
		rf.me, rf.currentTerm, oldTerm)

	// 重置选举计时器
	rf.lastHeartbeat = time.Now()

	// 准备请求投票的参数
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// 向所有其他服务器发送RequestVote RPC
	for server := range rf.peers {
		if server != rf.me {
			go func(server int) {
				log.Printf("Server %d: Sending RequestVote to Server %d for term %d",
					rf.me, server, args.Term)
				reply := RequestVoteReply{}
				if rf.sendRequestVote(server, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					log.Printf("Server %d: Received RequestVote reply from Server %d, granted=%v, term=%d",
						rf.me, server, reply.VoteGranted, reply.Term)

					// 如果收到更高的任期，转为follower
					if reply.Term > rf.currentTerm {
						log.Printf("Server %d: Discovered higher term %d from reply, becoming follower",
							rf.me, reply.Term)
						rf.currentTerm = reply.Term
						rf.role = Follower
						rf.votedFor = -1
						rf.persist()
						return
					}

					// 如果仍是候选人状态并且是当前任期的选票
					if rf.role == Candidate && rf.currentTerm == args.Term && reply.VoteGranted {
						rf.votesReceived++
						log.Printf("Server %d: Vote count for term %d: %d/%d",
							rf.me, rf.currentTerm, rf.votesReceived, len(rf.peers))
						// 如果获得多数票，成为领导人
						if rf.votesReceived > len(rf.peers)/2 {
							log.Printf("Server %d: Won election for term %d with %d/%d votes",
								rf.me, rf.currentTerm, rf.votesReceived, len(rf.peers))
							rf.becomeLeader()
						}
					}
				} else {
					log.Printf("Server %d: RequestVote RPC to Server %d failed", rf.me, server)
				}
			}(server)
		}
	}
}

// 成为领导人
func (rf *Raft) becomeLeader() {
	if rf.role != Candidate {
		return
	}

	rf.role = Leader
	log.Printf("Server %d: Becoming leader for term %d", rf.me, rf.currentTerm)

	// 初始化领导人状态
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}

	// 立即发送心跳
	rf.sendHeartbeats()

	// 定期发送心跳
	go rf.heartbeatTicker()
}

// 心跳定时器
func (rf *Raft) heartbeatTicker() {
	for rf.killed() == false {
		time.Sleep(100 * time.Millisecond) // 每100ms发送一次心跳（满足测试要求：不超过10次/秒）

		rf.mu.Lock()
		if rf.role == Leader {
			rf.sendHeartbeats()
		}
		rf.mu.Unlock()
	}
}

// 发送心跳到所有follower
func (rf *Raft) sendHeartbeats() {
	log.Printf("Server %d [Leader, Term %d]: Sending heartbeats to all peers",
		rf.me, rf.currentTerm)

	for server := range rf.peers {
		if server != rf.me {
			go func(server int) {
				rf.mu.Lock()

				// 如果不再是leader，停止发送
				if rf.role != Leader {
					rf.mu.Unlock()
					return
				}

				prevLogIndex := rf.nextIndex[server] - 1
				prevLogTerm := 0
				if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
					prevLogTerm = rf.log[prevLogIndex].Term
				}

				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  prevLogTerm,
					Entries:      []LogEntry{}, // 心跳没有日志条目
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				if rf.sendAppendEntries(server, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()

					log.Printf("Server %d [Leader]: Received heartbeat reply from Server %d, success=%v, term=%d",
						rf.me, server, reply.Success, reply.Term)

					// 如果收到更高的任期，转为follower
					if reply.Term > rf.currentTerm {
						log.Printf("Server %d: Discovered higher term %d from Server %d, stepping down from leader",
							rf.me, reply.Term, server)
						rf.currentTerm = reply.Term
						rf.role = Follower
						rf.votedFor = -1
						rf.persist()
					}
				} else {
					log.Printf("Server %d [Leader]: Heartbeat to Server %d failed", rf.me, server)
				}
			}(server)
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 添加辅助函数，将角色整数转换为字符串，便于日志输出
func roleToString(role int) string {
	switch role {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 初始化Raft状态
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.role = Follower
	rf.lastHeartbeat = time.Now()
	rf.applyCh = applyCh

	log.Printf("Server %d: Starting Raft server with %d peers", me, len(peers))

	// 初始化随机数生成器
	rand.Seed(time.Now().UnixNano() + int64(me))

	// 从持久化状态恢复
	rf.readPersist(persister.ReadRaftState())

	// 启动选举检查器
	go rf.ticker()

	return rf
}
