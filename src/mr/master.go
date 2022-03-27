package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"fmt"
	"time"
)

type Master struct {
	Mu sync.Mutex
	R  int // # total reduce tasks
	M  int // # total map tasks

	RCompleted int // # Completed Reduce Tasks
	MCompleted int // # Completed Map Tasks

	id2filename map[int]string 
	MState map[int]int // 0: idle 1: in-progess 2: completed
	RState map[int]int
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GiveMeTask(args *Request, reply *Task) error {
	// Master的每一个RPC Handler分发任务后，为了判断Worker是否crash了，
	// 可以让Master等待10秒钟，这之后，就可以判断worker已死，可以re-issue task
	m.Mu.Lock()
	defer m.Mu.Unlock()
	reply.R = m.R
	reply.M = m.M

	assignedTaskID := -1
	// 如果M==MCompleted，说明进入Reduce Phase了，否则就是Map Phase
	if m.MCompleted == m.M {
		// reduce phase
		// 遍历1 - R，将空闲的任务分配出去
		for i := 1; i <= m.R; i++ {
			if m.RState[i] != 0 {
				// in-progess或者completed
				continue
			}
			reply.NoTask = false
			reply.TaskType = 1
			reply.TaskID = i
			m.RState[i] = 1
			assignedTaskID = i
			go func(m *Master, id int) {
				// 等待10秒钟，查看一下assignedTaskID是否completed，如果没有，将它置为0
				time.Sleep(10 * time.Second)
				m.Mu.Lock()
				defer m.Mu.Unlock()
				if m.RState[id] != 2 {
					m.RState[id] = 0
				}
			}(m, i)
			break
		}
	} else {
		// map phase
		// 遍历1 - M，将空闲的任务分配出去
		for i := 1; i <= m.M; i++ {
			if m.MState[i] != 0 {
				// in-progess或者completed
				continue
			}
			reply.NoTask = false
			reply.TaskType = 0
			reply.TaskID = i
			reply.MapTaskFileName = m.id2filename[i]
			m.MState[i] = 1
			assignedTaskID = i
			go func(m *Master, id int) {
				// 等待10秒钟，查看一下assignedTaskID是否completed，如果没有，将它置为0
				time.Sleep(10 * time.Second)
				m.Mu.Lock()
				defer m.Mu.Unlock()
				if m.MState[id] != 2 {
					m.MState[id] = 0
				}
			}(m, i)
			break
		}
	}
	if assignedTaskID == -1 {
		// 没分配到任务
		reply.NoTask = true
	}
	return nil

}

func (m *Master) TaskCompleted(args *TaskCompletedMessage, reply *Response) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	id := args.TaskID
	taskType := args.TaskType

	if taskType == 0 {
		m.MState[id] = 2
		m.MCompleted++
	} else if taskType == 1{
		m.RState[id] = 2
		m.RCompleted++
	}
	fmt.Println(m)

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
	m.Mu.Lock()
	defer m.Mu.Unlock()
	ret := false
	if m.R == m.RCompleted && m.M == m.MCompleted {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// Master作为一个RPC Server，肯定是并发的，别忘了lock共享数据

	m := Master{R:nReduce, M:len(files), RCompleted:0, MCompleted:0}
	m.id2filename = map[int]string{}
	m.MState = map[int]int{}
	m.RState = map[int]int{}
	// # Map tasks = # files, 一开始都是idle
	for i := 0; i < len(files); i++ {
		m.id2filename[i+1] = files[i]
		m.MState[i+1] = 0 // idle
	}
	for i := 1; i <= nReduce; i++ {
		m.RState[i] = 0 // idle
	}
	fmt.Println(m)
	m.server()
	return &m
}
