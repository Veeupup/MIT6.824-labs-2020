package mr

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	IDLE       = 0
	INPROGRESS = 1
	COMPLETED  = 2
	WAIT_TIME  = 10 // time to wait for a task finish
)

type MapTask struct {
	filename string // map filename
	mapid    int    // map id
	state    int    // idle, in-progress, completed
}

type ReduceTask struct {
	filename string // reduce filename
	reduceid int    // reduce id
	state    int    // idle, in-progress, completed
}

type Master struct {
	// Your definitions here.
	mapTasks        []MapTask
	reduceTasks     []ReduceTask
	mapCompleted    bool
	reduceCompleted bool
}

var (
	mapId             int
	mapTasksTodo      int // remember how many tasks needs to do
	reduceTasksTodo   int // reduce tasks need to do
	intermediateFiles []string
	reduceId          int
	mutex             sync.Mutex
	mapChanarr        [10]chan int
	reduceChanarr     [10]chan int
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
				go limitMapTimer(m, index, mapChanarr[index])
				break
			}
		}
	} else if !m.reduceCompleted {
		// all map works have done and it's time to assign reduce works
		reply.TaskType = "reduce"
		reply.TaskId = -1
		for index, task := range m.reduceTasks {
			// to find the first idle reduce task
			if task.state == IDLE {
				m.reduceTasks[index].state = INPROGRESS
				m.reduceTasks[index].reduceid = reduceId
				reply.FileName = task.filename
				reply.TaskId = reduceId
				reply.TaskIndex = index
				reduceId++
				go limitReduceTimer(m, index, reduceChanarr[index])
				break
			}
		}

	} else {
		reply.TaskType = "close"
	}

	mutex.Unlock()

	return nil
}

// to limit a task in 10s, if a task runs beyond 10s, change the state of the task to idle
func limitMapTimer(m *Master, index int, tochan chan int) {

	select {
	case x := <-tochan:
		if x == index {
			// fmt.Printf("Map Task %v has finished in 10s\n", index)
		}
		mutex.Lock()
		mapTasksTodo--
		if mapTasksTodo == 0 {
			m.mapCompleted = true
			// fmt.Println("all Map Tasks Has done!")
			splitNBuckets(m)
		}
		m.mapTasks[index].state = COMPLETED
		mutex.Unlock()

	case <-time.After(time.Second * WAIT_TIME):
		// fmt.Printf("Map Task %v Timeout\n", index)
		mutex.Lock()
		m.mapTasks[index].state = IDLE
		m.mapTasks[index].mapid++ // increment to discard previous messages
		mutex.Unlock()
		// fmt.Println(m.mapTasks)
	}
}

// to limit a reduce task in 10 s
func limitReduceTimer(m *Master, index int, tochan chan int) {
	select {
	case x := <-tochan:
		if x == index {
			// fmt.Printf("Reduce Task %v has finished in 10s\n", index)
		}
		mutex.Lock()
		reduceTasksTodo--
		if reduceTasksTodo == 0 {
			m.reduceCompleted = true
			// fmt.Println("All Reduce Tasks Has done!")
		}
		m.reduceTasks[index].state = COMPLETED
		mutex.Unlock()
	case <-time.After(time.Second * WAIT_TIME):
		// fmt.Printf("Reduce Task %v Timeout\n", index)
		mutex.Lock()
		m.reduceTasks[index].state = IDLE
		m.reduceTasks[index].reduceid++ // increment to discard previous messages
		mutex.Unlock()
	}

}

//
func (m *Master) DoneTask(args *CompletedArgs, reply *CompletedReply) error {

	reply.Message = "Your should be faster, keep Going"
	// if taskId is less than the mapTasks[index].mapid
	// it means that this task has assigned to another worker to do
	// this worker has worked timeout
	if args.TaskType == "map" {
		if args.TaskId >= m.mapTasks[args.TaskIndex].mapid {
			mapChanarr[args.TaskIndex] <- args.TaskIndex // tell the timer to stop
			intermediateFiles = append(intermediateFiles, args.FileName)
			reply.Message = "You finished " + strconv.Itoa(args.TaskId) + " Map Task. Keep Going"
		}
	} else if args.TaskType == "reduce" {
		if args.TaskId >= m.reduceTasks[args.TaskIndex].reduceid {
			reduceChanarr[args.TaskIndex] <- args.TaskIndex
			reply.Message = "You finished " + strconv.Itoa(args.TaskId) + " Reduce Task. Keep Going"
		}
	}

	// fmt.Println(intermediateFiles)
	return nil
}

// split all intermediate kvs into n Buckets
func splitNBuckets(m *Master) {
	// fmt.Printf("All map tasks hava done, it's time to split into %v buckets\n", reduceTasksTodo)
	// fmt.Println(intermediateFiles)
	kva := []KeyValue{}
	for _, filename := range intermediateFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		// fmt.Println(filename, "has read")
		// fmt.Println(len(kva))
		// fmt.Println(kva[100], kva[1234])
	}

	sort.Sort(ByKey(kva))

	length := len(kva)
	bucketSize := length / reduceTasksTodo
	if bucketSize < 1 {
		bucketSize = 1
	}
	// fmt.Println("Bucket Size: ", bucketSize)

	start := 0
	endPos := 0
	index := 0

	for {
		if start+bucketSize <= length-1 {
			endPos = start + bucketSize

			// same key should assign into the same reduce task
			for endPos < length-1 && kva[endPos].Key == kva[endPos+1].Key {
				endPos++
			}
		} else {
			endPos = length - 1
		}
		endPos++

		// fmt.Println("start: ", start, ", end:", endPos)
		tempFileName := "mr-tmp-" + strconv.Itoa(index)

		file, err := os.Create(tempFileName)
		if err != nil {
			log.Fatal(err)
		}

		enc := json.NewEncoder(file)
		// fmt.Println("bucket ", index, " : ", kva[start:endPos])
		for _, kv := range kva[start:endPos] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}

		file.Close()

		m.reduceTasks[index].filename = tempFileName
		m.reduceTasks[index].state = IDLE

		start = endPos
		index++
		if index == reduceTasksTodo {
			break
		}
	}
	// fmt.Println(m.reduceTasks)
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

	if m.mapCompleted && m.reduceCompleted {
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
	m := Master{}

	// init master state
	m.mapTasks = []MapTask{}
	m.reduceTasks = make([]ReduceTask, nReduce)
	m.mapCompleted = false
	m.reduceCompleted = false

	// init channel
	for i := range mapChanarr {
		mapChanarr[i] = make(chan int, 1)
	}
	for i := range reduceChanarr {
		reduceChanarr[i] = make(chan int, 1)
	}

	// start server
	go m.server()

	// init mapid and reduceid
	mapId = 0
	reduceId = 0

	// get filenames in a struct
	mapTasksTodo = len(files)
	reduceTasksTodo = nReduce
	// fmt.Println("map tasks: ", mapTasksTodo)
	// fmt.Println("reduce tasks: ", reduceTasksTodo)
	for _, fn := range files {
		mt := MapTask{filename: fn, mapid: 0, state: IDLE}
		m.mapTasks = append(m.mapTasks, mt)
	}

	// fmt.Println(m.mapTasks)

	return &m
}
