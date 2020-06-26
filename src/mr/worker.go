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
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
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

var (
	taskId int // remember taskId
)

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// init
	taskId = 9999

	//
	for {
		time.Sleep(time.Second)

		reply := CallAssign()

		fmt.Println(reply)

		if reply.TaskId < 0 {
			fmt.Println("Waiting for assigning a work...")
			continue
		}

		// modify taskId and later will tell master who i am
		taskId = reply.TaskId

		if reply.TaskType == "map" {
			file, err := os.Open(reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", reply.FileName)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.FileName)
			}
			file.Close()
			kva := mapf(reply.FileName, string(content))

			// sort
			// sort.Sort(ByKey(kva))

			// store intermediate kvs in tempFile
			tempFileName := "./mr-tmp/tmp-" + reply.TaskType + "-" + strconv.Itoa(reply.TaskId)

			file, err = os.Create(tempFileName)
			if err != nil {
				log.Fatal("cannot create %v", tempFileName)
			}

			// transform k,v into json
			enc := json.NewEncoder(file)
			for _, kv := range kva {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatal(err)
				}
			}
			//
			file.Close()

			// try to delay sometime
			// ran := rand.Intn(4)
			// fmt.Printf("Sleep %v s\n", ran)
			// d := time.Second * time.Duration(ran)
			// time.Sleep(d)

			// tell the master the mapwork has done
			CallDoneTask(reply, tempFileName)

		} else if reply.TaskType == "reduce" {
			fmt.Println(reply.TaskType)

			kva := []KeyValue{}

			file, err := os.Open(reply.FileName)
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

			outputFileName := "mr-out-" + strconv.Itoa(reply.TaskIndex)
			ofile, _ := os.Create(outputFileName)

			// sort
			sort.Sort(ByKey(kva))

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			ofile.Close()

			fmt.Printf("Reduce task %v has finished.\n", reply.TaskIndex)

			// ran := rand.Intn(4)
			// fmt.Printf("Sleep %v s\n", ran)
			// d := time.Second * time.Duration(ran)
			// time.Sleep(d)

			CallDoneTask(reply, outputFileName)
		} else if reply.TaskType == "close" {
			fmt.Println("MapReduce has done. Exiting...")
			break
		} else {
			fmt.Println("UnExcepted TaskType")
		}

	}

}

// CallDoneTask, to tell the master has done
func CallDoneTask(task MapAssignReply, tempFileName string) {

	args := CompletedArgs{}

	args.TaskId = task.TaskId
	args.TaskIndex = task.TaskIndex
	args.FileName = tempFileName
	args.TaskType = task.TaskType

	reply := CompletedReply{}

	call("Master.DoneTask", &args, &reply)

	fmt.Println(reply.Message)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// CallAssign to get a job from master
// it may be a map task or a reduce task
func CallAssign() MapAssignReply {

	args := MapAssignArgs{}

	args.TASKID = taskId

	reply := MapAssignReply{}

	call("Master.AssignTask", &args, &reply)

	// fmt.Println(reply)

	return reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
