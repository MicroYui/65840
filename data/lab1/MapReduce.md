# MapReduce Lab1 实现过程详细分析

## 概述

本文档详细分析了从初始空代码框架到完整MapReduce系统的具体实现过程，展示了每一步的设计思路和代码演进。

## 初始状态分析

### 初始代码框架
```go
// coordinator.go - 初始状态
type Coordinator struct {
    // Your definitions here.
}

// worker.go - 初始状态
func Worker(mapf func(string, string) []KeyValue,
    reducef func(string, []string) string) {
    // Your worker implementation here.
}

// rpc.go - 初始状态
// Add your RPC definitions here.
```

**问题分析**: 初始代码只提供了基本的函数签名和空结构体，需要从零开始设计整个系统。

## 第一步：设计系统架构

### 1.1 定义核心数据结构

**任务类型定义**:
```go
type TaskType int
const (
    Wait TaskType = iota
    Map
    Reduce
    Exit
)

type TaskState int
const (
    Idle TaskState = iota
    InProgress
    Completed
)
```

**设计思路**:
- 使用枚举定义任务类型，符合MapReduce的三个阶段：Map、Reduce、Exit
- 添加Wait状态处理worker等待的情况
- 任务状态跟踪任务的生命周期

### 1.2 设计Task结构体

```go
type Task struct {
    ID        int
    State     TaskState
    Type      TaskType
    StartTime time.Time  // 用于超时检测
    InputFile string     // Map任务的输入文件
    NMap      int        // 总Map任务数
    NReduce   int        // 总Reduce任务数
}
```

**设计思路**:
- ID用于唯一标识任务
- StartTime记录任务开始时间，用于实现超时机制
- InputFile只对Map任务有意义
- NMap和NReduce帮助worker确定文件数量

### 1.3 设计Coordinator状态机

```go
type CoordinatorPhase int
const (
    MapPhase CoordinatorPhase = iota
    ReducePhase
    ExitPhase
)

type Coordinator struct {
    mapTasks    []Task
    reduceTasks []Task
    mutex       sync.Mutex
    nReduce     int
    nMap        int
    Phase       CoordinatorPhase
}
```

**设计思路**:
- 使用Phase状态机控制整个MapReduce流程
- 分离map和reduce任务队列，便于管理
- 使用mutex保证并发安全

## 第二步：实现RPC通信机制

### 2.1 定义RPC接口

```go
// rpc.go
type ReportTaskArgs struct {
    ID   int
    Type TaskType
}
```

**设计思路**:
- Worker需要向Coordinator请求任务
- Worker完成任务后需要报告给Coordinator
- 使用简单的参数结构，避免复杂性

### 2.2 实现任务请求机制

```go
func (coordinator *Coordinator) RequestTask(args *struct{}, reply *Task) error {
    coordinator.mutex.Lock()
    defer coordinator.mutex.Unlock()

    reply.Type = Wait  // 默认返回等待

    switch coordinator.Phase {
    case MapPhase:
        // 查找空闲的Map任务
        for index := range coordinator.mapTasks {
            if coordinator.mapTasks[index].State == Idle {
                coordinator.mapTasks[index].StartTime = time.Now()
                coordinator.mapTasks[index].State = InProgress
                *reply = coordinator.mapTasks[index]
                return nil
            }
        }
    case ReducePhase:
        // 查找空闲的Reduce任务
        for index := range coordinator.reduceTasks {
            if coordinator.reduceTasks[index].State == Idle {
                coordinator.reduceTasks[index].StartTime = time.Now()
                coordinator.reduceTasks[index].State = InProgress
                *reply = coordinator.reduceTasks[index]
                return nil
            }
        }
    case ExitPhase:
        reply.Type = Exit
    }
    return nil
}
```

**实现分析**:
- 使用状态机根据当前阶段分配不同类型的任务
- 找到空闲任务后立即标记为InProgress并记录开始时间
- 没有可用任务时返回Wait，让worker等待

## 第三步：实现Worker端逻辑

### 3.1 主工作循环

```go
func Worker(mapf func(string, string) []KeyValue,
    reducef func(string, []string) string) {
    for {
        args := struct{}{}
        reply := Task{}
        ok := call("Coordinator.RequestTask", &args, &reply)
        if ok {
            switch reply.Type {
            case Wait:
                time.Sleep(time.Second)
            case Map:
                doMapTask(mapf, reply)
                // 报告任务完成
            case Reduce:
                doReduceTask(reducef, reply)
                // 报告任务完成
            case Exit:
                return
            }
        } else {
            return // RPC失败，退出
        }
    }
}
```

**实现思路**:
- 持续循环请求任务
- 根据任务类型执行不同的处理逻辑
- 完成任务后立即报告给Coordinator

### 3.2 Map任务实现

```go
func doMapTask(mapf func(string, string) []KeyValue, task Task) {
    // 1. 读取输入文件
    filename := task.InputFile
    contentBytes, err := os.ReadFile(filename)
    content := string(contentBytes)

    // 2. 执行map函数
    keyValues := mapf(filename, content)

    // 3. 创建临时文件用于分区输出
    encoders := make([]*json.Encoder, task.NReduce)
    tempFiles := make([]*os.File, task.NReduce)

    for i := 0; i < task.NReduce; i++ {
        tempFile, _ := os.CreateTemp("", "mr-map-temp")
        tempFiles[i] = tempFile
        encoders[i] = json.NewEncoder(tempFile)
    }

    // 4. 根据key的hash值分区写入
    for _, keyValue := range keyValues {
        partition := ihash(keyValue.Key) % task.NReduce
        encoders[partition].Encode(&keyValue)
    }

    // 5. 原子重命名到最终文件名
    for i := 0; i < task.NReduce; i++ {
        tempName := tempFiles[i].Name()
        finalName := fmt.Sprintf("mr-%d-%d", task.ID, i)
        tempFiles[i].Close()
        os.Rename(tempName, finalName)
    }
}
```

**关键实现点**:
- 使用JSON编码器序列化键值对
- 使用hash函数将输出分区到R个文件
- 使用临时文件+原子重命名保证文件完整性
- 文件命名格式：mr-mapID-reducePartition

### 3.3 Reduce任务实现

```go
func doReduceTask(reducef func(string, []string) string, task Task) {
    kva := []KeyValue{}

    // 1. 读取所有相关的中间文件
    for i := 0; i < task.NMap; i++ {
        filename := fmt.Sprintf("mr-%d-%d", i, task.ID)
        file, err := os.Open(filename)
        if err != nil {
            continue // 文件可能不存在
        }

        // 2. 解码JSON数据
        dec := json.NewDecoder(file)
        for {
            var kv KeyValue
            if err = dec.Decode(&kv); err != nil {
                break
            }
            kva = append(kva, kv)
        }
        file.Close()
    }

    // 3. 按key排序
    sort.Slice(kva, func(i, j int) bool {
        return kva[i].Key < kva[j].Key
    })

    // 4. 创建临时输出文件
    tempFile, _ := os.CreateTemp("", "mr-reduce-temp-")

    // 5. 对每个唯一key执行reduce
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
        fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

        i = j
    }

    // 6. 原子重命名到最终输出文件
    finalName := fmt.Sprintf("mr-out-%d", task.ID)
    tempFile.Close()
    os.Rename(tempFile.Name(), finalName)
}
```

**关键实现点**:
- 读取所有map任务产生的相关中间文件
- 排序确保相同key的值聚集在一起
- 使用临时文件+原子重命名保证输出完整性
- 输出格式：每行一个"key value"

## 第四步：实现故障检测和处理

### 4.1 任务完成报告

```go
func (coordinator *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *struct{}) error {
    coordinator.mutex.Lock()
    defer coordinator.mutex.Unlock()

    switch args.Type {
    case Map:
        if coordinator.mapTasks[args.ID].State != InProgress {
            return nil // 重复报告，忽略
        }
        coordinator.mapTasks[args.ID].State = Completed

        // 检查是否所有Map任务完成
        allMapTasksDone := true
        for _, task := range coordinator.mapTasks {
            if task.State != Completed {
                allMapTasksDone = false
                break
            }
        }
        if allMapTasksDone {
            coordinator.Phase = ReducePhase
        }

    case Reduce:
        if coordinator.reduceTasks[args.ID].State != InProgress {
            return nil
        }
        coordinator.reduceTasks[args.ID].State = Completed

        // 检查是否所有Reduce任务完成
        allReduceTasksDone := true
        for _, task := range coordinator.reduceTasks {
            if task.State != Completed {
                allReduceTasksDone = false
                break
            }
        }
        if allReduceTasksDone {
            coordinator.Phase = ExitPhase
        }
    }
    return nil
}
```

**实现思路**:
- 验证任务状态，防止重复报告
- 检查阶段完成情况，自动推进状态机
- 原子性地更新状态和检查完成条件

### 4.2 超时检测机制

```go
func (coordinator *Coordinator) checkTasksPeriodically() {
    const timeout = 20 * time.Second
    for {
        coordinator.mutex.Lock()
        switch coordinator.Phase {
        case ExitPhase:
            coordinator.mutex.Unlock()
            return
        case MapPhase:
            for index := range coordinator.mapTasks {
                task := &coordinator.mapTasks[index]
                if task.State == InProgress && time.Since(task.StartTime) > timeout {
                    task.State = Idle // 重新调度
                }
            }
        case ReducePhase:
            for index := range coordinator.reduceTasks {
                task := &coordinator.reduceTasks[index]
                if task.State == InProgress && time.Since(task.StartTime) > timeout {
                    task.State = Idle // 重新调度
                }
            }
        }
        coordinator.mutex.Unlock()
        time.Sleep(2 * time.Second)
    }
}
```

**关键设计**:
- 20秒超时时间符合论文建议
- 2秒检查间隔保证及时发现故障
- 超时任务重置为Idle状态，可被重新分配

## 第五步：系统初始化

### 5.1 Coordinator初始化

```go
func MakeCoordinator(files []string, nReduce int) *Coordinator {
    c := Coordinator{
        Phase:   MapPhase,
        nReduce: nReduce,
        nMap:    len(files),
    }

    // 创建Map任务
    c.mapTasks = make([]Task, c.nMap)
    for index, filename := range files {
        c.mapTasks[index] = Task{
            ID:        index,
            Type:      Map,
            State:     Idle,
            InputFile: filename,
            NReduce:   c.nReduce,
        }
    }

    // 创建Reduce任务
    c.reduceTasks = make([]Task, c.nReduce)
    for index := 0; index < c.nReduce; index++ {
        c.reduceTasks[index] = Task{
            ID:    index,
            Type:  Reduce,
            State: Idle,
            NMap:  c.nMap,
        }
    }

    c.server()
    go c.checkTasksPeriodically()

    return &c
}
```

**初始化流程**:
1. 从输入文件列表创建Map任务
2. 根据nReduce参数创建Reduce任务
3. 启动RPC服务器
4. 启动故障检测协程

## 实现总结

### 核心设计原则
1. **状态机驱动**: 使用Phase状态机控制整个流程
2. **故障容错**: 超时重试机制保证可靠性
3. **原子操作**: 临时文件+重命名保证数据一致性
4. **并发安全**: 合理使用mutex保护共享状态

### 实现亮点
1. **简洁的架构**: 清晰的任务分配和状态管理
2. **有效的容错**: 20秒超时和重新调度机制
3. **正确的文件处理**: 避免部分写入问题
4. **符合论文设计**: 忠实实现了MapReduce模型

### 从空代码到完整系统的关键步骤
1. **第一步**: 设计数据结构和状态机
2. **第二步**: 实现RPC通信机制
3. **第三步**: 实现Map和Reduce处理逻辑
4. **第四步**: 添加故障检测和恢复
5. **第五步**: 完善初始化和清理逻辑

这个实现展示了如何从基本的函数签名出发，逐步构建一个完整的分布式计算系统，体现了良好的系统设计能力和对MapReduce模型的深刻理解。