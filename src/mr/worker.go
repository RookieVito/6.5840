package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"
import "time"
import "sync"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator


// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	// 开启工作，重复的向协调者请求任务
	for{
		// 请求任务
		reply := RequestTask()
		switch reply.Task.TaskType{
		case MapTask:
			// 处理map任务
			mapFunction(reply.Task, mapf)
		case ReduceTask:
			// 处理reduce任务
			reduceFunction(reply.Task, reducef)
		case WaitTask:
			// 等待任务
			time.Sleep(1 * time.Second)
			continue
		case ExitTask:
			// 退出任务
			return
		default:
			log.Fatalf("wrong task type")
		}
		// 通知任务完成
		TaskDone(reply.Task.TaskId, reply.Task.TaskType)
	}
}

// map任务流程

// 处理map任务
func mapFunction(task Task, mapf func(string, string) []KeyValue) {
	// 处理任务
	// 获取文件内容
	file, err := os.Open(task.InputFiles)
	if err != nil {
		log.Fatalf("map cannot open %v", task.InputFiles)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("map cannot read %v", task.InputFiles)
	}
	file.Close()


	kva := mapf(task.InputFiles, string(content))

	// 按照reduce数量划分中间文件
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceTaskNum := ihash(kv.Key) % task.NReduce
		buckets[reduceTaskNum] = append(buckets[reduceTaskNum], kv)
	}

	// 输出中间文件(串行)
	// for i := 0; i < task.NReduce; i++ {
	// 	midFileName := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
	// 	midFile, err := os.Create(midFileName)
	// 	if err != nil {
	// 		log.Fatalf("cannot create %v", midFileName)
	// 	}
	// 	enc := json.NewEncoder(midFile)
	// 	for _, kv := range buckets[i] {
	// 		err := enc.Encode(&kv)
	// 		if err != nil {
	// 			log.Fatalf("cannot encode %v", midFileName)
	// 		}
	// 	}
	// 	midFile.Close()
	// }
	// 输出中间文件(并发)
	var wg sync.WaitGroup
	errChan := make(chan error, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		wg.Add(1)
		go func(reduceIdx int) {
			defer wg.Done()
			
			midFileName := fmt.Sprintf("mr-%d-%d", task.TaskId, reduceIdx)
			midFile, err := os.Create(midFileName)
			if err != nil {
				errChan <- fmt.Errorf("cannot create %v: %w", midFileName, err)
				return
			}
			defer midFile.Close()
			
			enc := json.NewEncoder(midFile)
			for _, kv := range buckets[reduceIdx] {
				if err := enc.Encode(&kv); err != nil {
					errChan <- fmt.Errorf("cannot encode %v: %w", midFileName, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errChan)

	// 检查是否有错误
	if len(errChan) > 0 {
		for err := range errChan {
			log.Printf("Error: %v", err)
		}
		log.Fatalf("encountered errors during file writing")
	}

}

// 处理reduce任务
func reduceFunction(task Task, reducef func(string, []string) string) {
	// 处理任务
	reduceNum := task.TaskId
	intermediate := make([]KeyValue, 0)
	// 读取中间文件
	for i := 0; i < task.NMap; i++ {
		midFileName := fmt.Sprintf("mr-%d-%d", i, reduceNum)
		midFile, err := os.Open(midFileName)
		if err != nil {
			log.Fatalf("cannot open %v", midFileName)
		}
		dec := json.NewDecoder(midFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		midFile.Close()
	}
	// 按key排序
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// 归并输出
	oname := fmt.Sprintf("mr-out-%d", reduceNum)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	return
}



// ****************************** RPC *****************************
// 通知coordinator map任务完成
func TaskDone(taskId int, taskType TaskType) {
	args:= TaskDoneArgs{
		TaskId: taskId,
		TaskType: taskType,
	}
	reply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &args, &reply)
	if ok {
		return
	}else{
		fmt.Printf("taskDone call failed!\n")
		return
	}
}

// worker -> coordinator 请求任务
func RequestTask() RequestTaskReply {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok := call("Coordinator.RequestTask", &args, &reply)
	if ok {
		return reply
	}else{
		fmt.Printf("requestTask call failed!\n")
		return RequestTaskReply{}
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
