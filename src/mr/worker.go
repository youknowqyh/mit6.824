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

	// 搞错了tmd，这就是一个Worker，且只有一个，要想分布式计算，就要多开几个Worker
	for {
		request := Request{}
		assignedTask := Task{}
		// Worker向Master请求一个Task
		call("Master.GiveMeTask", &request, &assignedTask)
		fmt.Println(assignedTask)
		if !assignedTask.NoTask {
			// NoTask == false, 说明有Task可以执行
			// 请求到的Task可能是Map Task，也可能是Reduce Task，为了区分，reply里肯定要携带Task类别信息
			fmt.Println("I got a job!")
			if assignedTask.TaskType == 0 {
				// Map Task
				// 如果是Map Task，就需要读取对应的文件，然后调用Map function
				// 通过ihash(key)%nReduce来选择对应的reduce task。
				filename := assignedTask.MapTaskFileName
				R := assignedTask.R
				mapTaskID := assignedTask.TaskID
				fmt.Printf("Start processing Map Task, id: %v, filename: %v\n", mapTaskID, filename)
				// 创建nReduce个文件
				ofiles := []*os.File{}
				for i := 1; i <= R; i++ {
					// intermediate files命名为`mr-mapTaskID-reduceTaskID`的格式
					oname := "mr-" + strconv.Itoa(mapTaskID) + "-" + strconv.Itoa(i)
					ofile, _ := os.Create(oname)
					ofiles = append(ofiles, ofile)
				}

				// Worker先读取Map split file
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))

				// 把key/value pair放到对应的partition region中，就像Grace Hash Join那样。
				for _, kv := range kva {
					reduceTaskID := ihash(kv.Key) % R
					enc := json.NewEncoder(ofiles[reduceTaskID])
					enc.Encode(&kv)
				}

				for _, ofile := range ofiles {
					ofile.Close()
				}
				// 执行完后，应该要给Master发个TaskCompletedMessage，告知主人俺完成Map Task啦！
				// 好像是需要把所有的intermediate files的名字都告诉master，但是因为现在的文件名完全可以根据MapTaskID推导出来，就先不传了。
				request := TaskCompletedMessage{TaskType: 0, TaskID: mapTaskID}
				response := Response{}
				// Worker向Master请求一个Task
				call("Master.TaskCompleted", &request, &response)
			} else if assignedTask.TaskType == 1 {
				// Reduce Task
				// 读取对应该Task的所有intermediate files，每个reduce task应该有M个对应文件
				M := assignedTask.M
				reduceTaskID := assignedTask.TaskID
				intermediate := []KeyValue{}
				fmt.Printf("Start processing Reduce Task, id: %v", reduceTaskID)

				// 读取所有的intermediate file中对的kv pairs
				for i := 1; i <= M; i++ {
					filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceTaskID)
					file, err := os.Open(filename)
					if err != nil {
						log.Fatalf("cannot open %v", filename)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						intermediate = append(intermediate, kv)
					}
					file.Close()
					// 这个intermediate file已经没用了，删掉！
					os.Remove(filename)
				}
				sort.Sort(ByKey(intermediate))
				// 为了确保nobody obeserves partially written files当Worker执行到一半崩溃了
				// 可以使用temporary file并且当它完成后改名 `ioutil.TempFile` 和 `os.Rename`（mr-out-X X'th reduce Task）
				// A reduce task produces one such file, and a map task produces R such files (one per reduce task)
				oname := "mr-out-" + strconv.Itoa(reduceTaskID)
				tmpfile, err := ioutil.TempFile("./", "temp"+oname)
				tmpfilename := tmpfile.Name()
				if err != nil {
					log.Fatal(err)
				}
				//
				// call Reduce on each distinct key in intermediate[],
				// and print the result to mr-out-*.
				//
				i := 0
				for i < len(intermediate) {
					j := i + 1
					for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediate[k].Value)
					}
					output := reducef(intermediate[i].Key, values)

					// this is the correct format for each line of Reduce output.
					fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, output)

					i = j
				}
				tmpfile.Close()
				os.Rename(tmpfilename, oname)
				request := TaskCompletedMessage{TaskType: 1, TaskID: reduceTaskID}
				response := Response{}
				// Worker向Master请求一个Task
				call("Master.TaskCompleted", &request, &response)
			}
		}
		// 因为需要等待所有的Map Task执行结束后，才可以执行Reduce Task，
		// 所以Worker可能需要等待，可以在两次request间隔中进行`time.Sleep()`来等待
		fmt.Println("Waiting 2 seconds to request another task...")
		time.Sleep(2 * time.Second)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c -> client
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	// dial: 很形象啊，就是输对方的“电话号码”
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	// remote call: 输入完“电话号码”后，就可以“打电话了/Call”了。
	err = c.Call(rpcname, args, reply)

	// 结果都在reply中。
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
