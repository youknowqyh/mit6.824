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

// Add your RPC definitions here.
type Request struct {
	Message string
}

type Response struct {
	Message string
}

type Task struct {
	NoTask bool

	TaskType int // 0: MapTask, 1: Reduce Task

	TaskID          int
	MapTaskFileName string
	R               int
	M               int
}

type TaskCompletedMessage struct {
	TaskType int // 0: MapTask, 1: Reduce Task
	TaskID   int
}

// type Err string
// type MapTaskArgs struct {
// 	Id int
// }
// type MapTaskReply struct {
// 	Filename string
// 	NumReduce int
// 	Err Err
// }

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
