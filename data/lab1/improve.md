# MapReduce Lab1 实现改进建议

## 概述

本文档详细分析了当前MapReduce实现中存在的潜在bug和可改进的点，并提供了具体的修复方案。

## 发现的问题分类

### 严重问题（可能导致程序崩溃）

#### 1. 数组越界风险 ⚠️ **高优先级**

**位置**: `coordinator.go:99` 和 `coordinator.go:115`

**问题代码**:
```go
func (coordinator *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *struct{}) error {
    switch args.Type {
    case Map:
        if coordinator.mapTasks[args.ID].State != InProgress {  // 潜在越界
            return nil
        }
    case Reduce:
        if coordinator.reduceTasks[args.ID].State != InProgress {  // 潜在越界
            return nil
        }
    }
}
```

**问题分析**:
- 没有验证 `args.ID` 的有效性
- 如果传入负数或超出数组长度的ID，会导致数组越界panic
- 恶意或错误的客户端可能导致整个coordinator崩溃

**修复方案**:
```go
func (coordinator *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *struct{}) error {
    coordinator.mutex.Lock()
    defer coordinator.mutex.Unlock()

    switch args.Type {
    case Map:
        // 添加边界检查
        if args.ID < 0 || args.ID >= len(coordinator.mapTasks) {
            return fmt.Errorf("invalid map task ID: %d", args.ID)
        }
        if coordinator.mapTasks[args.ID].State != InProgress {
            return nil
        }
        // ... 其余代码不变
    case Reduce:
        // 添加边界检查
        if args.ID < 0 || args.ID >= len(coordinator.reduceTasks) {
            return fmt.Errorf("invalid reduce task ID: %d", args.ID)
        }
        if coordinator.reduceTasks[args.ID].State != InProgress {
            return nil
        }
        // ... 其余代码不变
    }
    return nil
}
```

#### 2. 错误处理导致程序终止 ⚠️ **高优先级**

**位置**: `worker.go:76`

**问题代码**:
```go
func doMapTask(mapf func(string, string) []KeyValue, task Task) {
    filename := task.InputFile
    contentBytes, err := os.ReadFile(filename)
    if err != nil {
        log.Fatalf("cannot read %v", filename)  // 直接终止程序
    }
}
```

**问题分析**:
- 使用 `log.Fatalf` 会立即终止整个worker程序
- 应该允许worker继续运行，让coordinator通过超时机制重新分配任务
- 影响系统的容错能力

**修复方案**:
```go
func doMapTask(mapf func(string, string) []KeyValue, task Task) error {
    filename := task.InputFile
    contentBytes, err := os.ReadFile(filename)
    if err != nil {
        return fmt.Errorf("cannot read %v: %w", filename, err)
    }
    content := string(contentBytes)
    keyValues := mapf(filename, content)

    // ... 其余逻辑
    return nil
}

// 在Worker函数中相应修改
case Map:
    err := doMapTask(mapf, reply)
    if err != nil {
        fmt.Printf("Map task %d failed: %v\n", reply.ID, err)
        // 不报告完成，让coordinator超时重新分配
        continue
    }
    // 只有成功才报告完成
    reportArgs := ReportTaskArgs{ID: reply.ID, Type: Map}
    reportReply := struct{}{}
    call("Coordinator.ReportTaskDone", &reportArgs, &reportReply)
```

### 中等问题（影响健壮性）

#### 3. 文件资源管理不当 🔧 **中优先级**

**位置**: `worker.go:84-87` 和 `worker.go:130`

**问题代码**:
```go
// Map任务中
tempFile, _ := os.CreateTemp("", "mr-map-temp")  // 忽略错误
tempFiles[i] = tempFile
encoders[i] = json.NewEncoder(tempFile)

// Reduce任务中
tempFile, _ := os.CreateTemp("", "mr-reduce-temp-")  // 忽略错误
```

**问题分析**:
- 忽略 `CreateTemp` 的错误返回值
- 没有适当的资源清理机制
- 如果操作失败，可能导致临时文件泄漏

**修复方案**:
```go
func doMapTask(mapf func(string, string) []KeyValue, task Task) error {
    // ... 前面的代码不变

    encoders := make([]*json.Encoder, task.NReduce)
    tempFiles := make([]*os.File, task.NReduce)

    // 确保清理资源
    defer func() {
        for _, tempFile := range tempFiles {
            if tempFile != nil {
                tempFile.Close()
                // 如果重命名失败，清理临时文件
                os.Remove(tempFile.Name())
            }
        }
    }()

    for i := 0; i < task.NReduce; i++ {
        tempFile, err := os.CreateTemp("", "mr-map-temp")
        if err != nil {
            return fmt.Errorf("failed to create temp file: %w", err)
        }
        tempFiles[i] = tempFile
        encoders[i] = json.NewEncoder(tempFile)
    }

    // ... 其余逻辑
}
```

#### 4. 文件操作错误处理缺失 🔧 **中优先级**

**位置**: `worker.go:95-99`

**问题代码**:
```go
for i := 0; i < task.NReduce; i++ {
    tempName := tempFiles[i].Name()
    finalName := fmt.Sprintf("mr-%d-%d", task.ID, i)
    tempFiles[i].Close()                    // 未检查错误
    os.Rename(tempName, finalName)          // 未检查错误
}
```

**问题分析**:
- 文件关闭和重命名操作可能失败
- 如果操作失败，可能导致数据丢失或不一致

**修复方案**:
```go
for i := 0; i < task.NReduce; i++ {
    tempName := tempFiles[i].Name()
    finalName := fmt.Sprintf("mr-%d-%d", task.ID, i)

    if err := tempFiles[i].Close(); err != nil {
        return fmt.Errorf("failed to close temp file %s: %w", tempName, err)
    }

    if err := os.Rename(tempName, finalName); err != nil {
        return fmt.Errorf("failed to rename %s to %s: %w", tempName, finalName, err)
    }

    // 标记文件已成功处理，避免defer中重复删除
    tempFiles[i] = nil
}
```

#### 5. Worker RPC失败处理不当 🔧 **中优先级**

**位置**: `worker.go:45-48` 和 `worker.go:53-56`

**问题代码**:
```go
if !call("Coordinator.ReportTaskDone", &reportArgs, &reportReply) {
    // fmt.Printf("Worker: failed to report Map task %d. Exiting.\n", reply.ID)
    return
}
```

**问题分析**:
- RPC调用失败就直接退出worker
- Coordinator不知道worker已退出，可能等到超时才重新分配任务
- 降低了系统的容错能力

**修复方案**:
```go
// 方案1: 重试机制
func reportTaskWithRetry(taskType TaskType, taskID int, maxRetries int) bool {
    reportArgs := ReportTaskArgs{ID: taskID, Type: taskType}
    reportReply := struct{}{}

    for i := 0; i < maxRetries; i++ {
        if call("Coordinator.ReportTaskDone", &reportArgs, &reportReply) {
            return true
        }
        time.Sleep(time.Millisecond * 100) // 短暂等待后重试
    }
    return false
}

// 方案2: 继续运行而不是退出
case Map:
    err := doMapTask(mapf, reply)
    if err != nil {
        fmt.Printf("Map task %d failed: %v\n", reply.ID, err)
        continue // 继续请求新任务
    }

    if !reportTaskWithRetry(Map, reply.ID, 3) {
        fmt.Printf("Failed to report Map task %d completion after retries\n", reply.ID)
        // 不退出，继续运行
    }
```

### 轻微问题（代码质量改进）

#### 6. 代码重复 📝 **低优先级**

**位置**: `coordinator.go` 中的任务完成检查逻辑

**问题**: Map和Reduce任务完成检查逻辑几乎相同，存在代码重复

**修复方案**:
```go
func (coordinator *Coordinator) checkAllTasksCompleted(taskType TaskType) bool {
    switch taskType {
    case Map:
        for _, task := range coordinator.mapTasks {
            if task.State != Completed {
                return false
            }
        }
        return true
    case Reduce:
        for _, task := range coordinator.reduceTasks {
            if task.State != Completed {
                return false
            }
        }
        return true
    }
    return false
}

func (coordinator *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *struct{}) error {
    // ... 边界检查代码 ...

    switch args.Type {
    case Map:
        // ... 状态更新代码 ...
        if coordinator.checkAllTasksCompleted(Map) {
            coordinator.Phase = ReducePhase
        }
    case Reduce:
        // ... 状态更新代码 ...
        if coordinator.checkAllTasksCompleted(Reduce) {
            coordinator.Phase = ExitPhase
        }
    }
    return nil
}
```

#### 7. 注释掉的调试代码 📝 **低优先级**

**位置**: 多处 `// fmt.Printf` 语句

**问题**: 大量注释掉的调试语句影响代码可读性

**修复方案**:
```go
// 添加配置化的日志系统
type Logger struct {
    enabled bool
}

func (l *Logger) Debug(format string, args ...interface{}) {
    if l.enabled {
        fmt.Printf("[DEBUG] "+format+"\n", args...)
    }
}

var logger = &Logger{enabled: false} // 可通过环境变量控制

// 在代码中使用
logger.Debug("Worker: Starting Map task %d", task.ID)
```

## 修复优先级建议

### 立即修复（高优先级）
1. **数组越界检查** - 防止程序崩溃
2. **错误处理改进** - 提高系统容错能力

### 短期修复（中优先级）
3. **文件资源管理** - 防止资源泄漏
4. **文件操作错误处理** - 保证数据一致性
5. **RPC失败处理** - 提高系统健壮性

### 长期改进（低优先级）
6. **代码重构** - 提高代码质量
7. **日志系统** - 便于调试和维护

## 测试建议

为了验证修复效果，建议添加以下测试：

1. **边界测试**: 传入无效的task ID
2. **错误注入测试**: 模拟文件读取失败、磁盘满等情况
3. **网络故障测试**: 模拟RPC调用失败
4. **资源限制测试**: 在低内存、磁盘空间不足等条件下测试

## 总结

当前实现的核心逻辑是正确的，但在错误处理和边界条件方面存在一些问题。最关键的是修复数组越界问题，这可能导致程序崩溃。其他改进主要是提高系统的健壮性和可维护性。

建议按照优先级逐步修复这些问题，每次修复后进行充分测试以确保系统稳定性。