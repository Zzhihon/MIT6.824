package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskType int

const (
	Map TaskType = iota
	Reduce
	Wait
	Exit
)

// Add your RPC definitions here.
type EmptyArgs struct{}

type EmptyReply struct{}

type TaskReply struct {
	TaskType  TaskType
	TaskId    int
	InputFile string
	NReduce   int
	NMap      int
}

type MapTaskCompleteArgs struct {
	TaskId int
}

type ReduceTaskCompleteArgs struct {
	TaskId int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
