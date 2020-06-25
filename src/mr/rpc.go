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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type MapAssignArgs struct {
	TASKID int
}

type MapAssignReply struct {
	TaskType  string
	TaskId    int
	TaskIndex int
	FileName  string
}

// CompletedArgs when a task has done
// the worker tells the master it has completed which task
type CompletedArgs struct {
	TaskType  string
	TaskId    int
	TaskIndex int
	FileName  string
}

type CompletedReply struct {
	Message string
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
