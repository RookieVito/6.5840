package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// Add your RPC definitions here.

// 请求任务
type RequestTaskArgs struct {
    // WorkerId int  // 可选
}

type RequestTaskReply struct {
    Task Task
}

// 通知任务完成
type TaskDoneArgs struct {
    TaskId   int
    TaskType TaskType
}

type TaskDoneReply struct {
    // 空的就行
}
