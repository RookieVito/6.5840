package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type TaskStatus int
const (
    Idle TaskStatus = iota      // 任务未分配
    InProgress                   // 任务进行中
    Completed                    // 任务已完成
)

type TaskType int
const (
    MapTask TaskType = iota
    ReduceTask
    WaitTask        // 让worker等待
    ExitTask        // 通知worker退出
)

type Task struct {
    TaskId      int
    TaskType    TaskType
    InputFiles  string    // Map任务的输入文件
    ReduceId    int         // Reduce任务的ID
    NReduce     int         // Reduce任务的总数（Map任务需要知道）
    NMap        int         // Map任务的总数（Reduce任务需要知道）
    StartTime   time.Time   // 任务开始时间（用于超时检测）
}

type Coordinator struct {
    mu sync.Mutex
    
    // 任务状态跟踪
    mapTasks    []Task
    reduceTasks []Task
    
    mapTaskStatus    []TaskStatus
    reduceTaskStatus []TaskStatus
    
    // 阶段控制
    phase      Phase    // Map阶段还是Reduce阶段
    
    nMap       int      // Map任务总数
    nReduce    int      // Reduce任务总数
    
    // 完成计数
    mapCompleted    int
    reduceCompleted int
    
    done bool
}

type Phase int

const (
    MapPhase Phase = iota
    ReducePhase
    AllDone
)

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// c.done有数据竞争问题
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := c.done
	return ret
}

// ********************** RPC handlers **********************
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error{
	// TODO: 暂时没有对超时的任务进行处理
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == MapPhase {
		// 分配Map任务
		for i, status := range c.mapTaskStatus {
			if status == Idle {
				// 分配任务
				c.mapTaskStatus[i] = InProgress
				c.mapTasks[i].StartTime = time.Now()
				reply.Task = c.mapTasks[i]
				return nil
			}
		}
		// 没有空闲任务，要求worker等待
		reply.Task = Task{TaskType: WaitTask}
		return nil
	}else if c.phase == ReducePhase {
		// 分配Reduce任务
		for i, status := range c.reduceTaskStatus {
			if status == Idle {
				// 分配任务
				c.reduceTaskStatus[i] = InProgress
				c.reduceTasks[i].StartTime = time.Now()
				reply.Task = c.reduceTasks[i]
				return nil
			}
		}
		// 没有空闲任务，要求worker等待
		reply.Task = Task{TaskType: WaitTask}
		return nil
	}else {
		// 全部完成，通知worker退出
		reply.Task = Task{TaskType: ExitTask}
		return nil
	}
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error{
	c.mu.Lock()
	defer c.mu.Unlock()
	taskId := args.TaskId
	taskType := args.TaskType
	if taskType == MapTask {
		c.mapTaskStatus[taskId] = Completed
		c.mapCompleted++
		if c.mapCompleted == c.nMap {
			c.phase = ReducePhase
		}
	} else if taskType == ReduceTask {
		c.reduceTaskStatus[taskId] = Completed
		c.reduceCompleted++
		if c.reduceCompleted == c.nReduce {
			c.phase = AllDone
			c.done = true
		}
	}
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// 初始化
	c.nMap = len(files)
	c.nReduce = nReduce
	c.phase = MapPhase
	c.done = false
	c.mapCompleted = 0
	c.reduceCompleted = 0

	// 初始化map任务
	c.mapTasks = make([]Task, c.nMap)
	c.mapTaskStatus = make([]TaskStatus, c.nMap)
	for i := 0; i < c.nMap; i++ {
	    c.mapTasks[i] = Task{
	        TaskId: i,
			TaskType: MapTask,
			InputFiles: files[i],
			NReduce: nReduce,
			NMap: c.nMap,
	    }
	    c.mapTaskStatus[i] = Idle
	}
	// 初始化reduce任务
	c.reduceTasks = make([]Task, c.nReduce)
	c.reduceTaskStatus = make([]TaskStatus, c.nReduce)
	for i := 0; i < c.nReduce; i++ {
	    c.reduceTasks[i] = Task{
	        TaskId: i,
			TaskType: ReduceTask,
			ReduceId: i,
			NReduce: nReduce,
			NMap: c.nMap,
	    }
	    c.reduceTaskStatus[i] = Idle
	}

	go c.checkTimeouts()

	c.server(sockname)
	return &c
}


// 在Coordinator中添加后台检测,如果某个任务超时，设置任务为未分配
func (c *Coordinator) checkTimeouts() {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        c.mu.Lock()
        now := time.Now()
        
        for i, status := range c.mapTaskStatus {
            if status == InProgress && now.Sub(c.mapTasks[i].StartTime) > 10*time.Second {
                c.mapTaskStatus[i] = Idle
            }
        }
        
        for i, status := range c.reduceTaskStatus {
            if status == InProgress && now.Sub(c.reduceTasks[i].StartTime) > 10*time.Second {
                c.reduceTaskStatus[i] = Idle
            }
        }
        c.mu.Unlock()
    }
}