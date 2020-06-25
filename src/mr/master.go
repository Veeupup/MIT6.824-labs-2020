package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const (
	IDLE       = 0
	INPROGRESS = 1
	COMPLETED  = 2
	WAIT_TIME  = 5 // time to wait for a task finish
)

type MapTask struct {
	filename string // map filename
	mapid    int    // map id
	state    int    // idle, in-progress, completed
}

type Master struct {
	// Your definitions here.
	mapTasks        []MapTask
	mapCompleted    bool
	reduceCompleted bool
}

var (
	mapId        int
	mapTasksTodo int // remember how many tasks needs to do
	reduceId     int
	mutex        sync.Mutex
	baton        chan int
	chanarr      [10]chan int
)

// AssignTask a interface to assign map or reduce task
func (m *Master) AssignTask(args *MapAssignArgs, reply *MapAssignReply) error {
	// to check if all map works hava done
	mutex.Lock()

	// map works have not done yet
	if !m.mapCompleted {
		reply.TaskType = "map"
		reply.TaskId = -1 // negative means that no map work to do
		for index, task := range m.mapTasks {
			// to find the first idle map task
			if task.state == IDLE {
				m.mapTasks[index].state = INPROGRESS
				m.mapTasks[index].mapid = mapId
				reply.FileName = task.filename
				reply.TaskId = mapId
				reply.TaskIndex = index // to remember which task has done
				mapId++
				go limitTimer(m, index, chanarr[index])
				break
			}
		}
	} else if !m.reduceCompleted {
		// all map works have done and it's time to assign reduce works
		reply.TaskType = "reduce"

	} else {
		reply.TaskType = "close"
	}

	mutex.Unlock()

	return nil
}

// to limit a task in 10s, if a task runs beyond 10s, change the state of the task to idle
func limitTimer(m *Master, index int, baton chan int) {

	select {
	case x := <-baton:
		if x == index {
			fmt.Printf("Task %v has finished in 10s", index)
		}
		mutex.Lock()
		mapTasksTodo--
		m.mapTasks[index].state = COMPLETED
		mutex.Unlock()
		fmt.Println(m.mapTasks)
	case <-time.After(time.Second * WAIT_TIME):
		fmt.Printf("Task %v Timeout", index)
		mutex.Lock()
		m.mapTasks[index].state = IDLE
		m.mapTasks[index].mapid++ // increment to discard previous messages
		mutex.Unlock()
		fmt.Println(m.mapTasks)
	}

}

//
func (m *Master) DoneTask(args *CompletedArgs, reply *CompletedReply) error {

	reply.Message = "Your should be faster, keep Going"
	// if taskId is less than the mapTasks[index].mapid
	// it means that this task has assigned to another worker to do
	// this worker has worked timeout
	if args.TaskId >= m.mapTasks[args.TaskIndex].mapid {
		chanarr[args.TaskIndex] <- args.TaskId // tell the timer to stop
		reply.Message = "You finished " + strconv.Itoa(args.TaskId) + " Map Task. Keep Going"
	}

	mutex.Lock()
	// if all map tasks have been done, it's time to reduce tasks
	// and it will only run once when the first time map tasks have done
	if !m.reduceCompleted && mapTasksTodo == 0 {
		m.mapCompleted = true
	}
	mutex.Unlock()

	return nil
}

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
	http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// init master state
	m.mapTasks = []MapTask{}
	m.mapCompleted = false
	m.reduceCompleted = false

	// init channel
	for i := range chanarr {
		chanarr[i] = make(chan int, 1)
	}

	// start server
	go m.server()

	// init mapid and reduceid
	mapId = 0
	reduceId = 0

	// get filenames in a struct
	mapTasksTodo = len(files)
	fmt.Println("map tasks: ", mapTasksTodo)
	for _, fn := range files {
		mt := MapTask{filename: fn, mapid: 0, state: IDLE}
		m.mapTasks = append(m.mapTasks, mt)
	}

	fmt.Println(m.mapTasks)

	return &m
}
