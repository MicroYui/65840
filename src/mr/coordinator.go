package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

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

type Task struct {
	ID        int
	State     TaskState
	Type      TaskType
	StartTime time.Time
	InputFile string
	NMap      int
	NReduce   int
}

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

// Your code here -- RPC handlers for the worker to call.
func (coordinator *Coordinator) RequestTask(args *struct{}, reply *Task) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()

	reply.Type = Wait

	switch coordinator.Phase {
	case MapPhase:
		for index := range coordinator.mapTasks {
			if coordinator.mapTasks[index].State == Idle {
				// fmt.Printf("assign map task %d\n", index)
				coordinator.mapTasks[index].StartTime = time.Now()
				coordinator.mapTasks[index].State = InProgress
				*reply = coordinator.mapTasks[index]
				return nil
			}
		}
	case ReducePhase:
		for index := range coordinator.reduceTasks {
			if coordinator.reduceTasks[index].State == Idle {
				// fmt.Printf("assign reduce task %d\n", index)
				coordinator.reduceTasks[index].StartTime = time.Now()
				coordinator.reduceTasks[index].State = InProgress
				*reply = coordinator.reduceTasks[index]
				return nil
			}
		}
	case ExitPhase:
		reply.Type = Exit
	default:
		return nil
	}
	return nil
}

func (coordinator *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *struct{}) error {
	coordinator.mutex.Lock()
	defer coordinator.mutex.Unlock()

	switch args.Type {
	case Map:
		if coordinator.mapTasks[args.ID].State != InProgress {
			return nil
		}
		coordinator.mapTasks[args.ID].State = Completed
		// fmt.Printf("map task %d completed\n", args.ID)
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
		// fmt.Printf("reduce task %d completed\n", args.ID)
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
	default:
		return nil
	}
	return nil
}

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
					// fmt.Printf("Coordinator: Map task %d timed out. Re-scheduling.\n", index)
					task.State = Idle
				}
			}
		case ReducePhase:
			for index := range coordinator.reduceTasks {
				task := &coordinator.reduceTasks[index]
				if task.State == InProgress && time.Since(task.StartTime) > timeout {
					// fmt.Printf("Coordinator: Reduce task %d timed out. Re-scheduling.\n", index)
					task.State = Idle
				}
			}
		default:
			panic("undefined coordinator phase")
		}
		coordinator.mutex.Unlock()
		time.Sleep(2 * time.Second)
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.Phase == ExitPhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		Phase:   MapPhase,
		nReduce: nReduce,
		nMap:    len(files),
	}

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

	c.reduceTasks = make([]Task, c.nReduce)
	for index := 0; index < c.nReduce; index++ {
		c.reduceTasks[index] = Task{
			ID:    index,
			Type:  Reduce,
			State: Idle,
			NMap:  c.nMap,
		}
	}

	// fmt.Println("Coordinator: Initialization complete.")
	// fmt.Printf("Coordinator: %d map tasks and %d reduce tasks created.\n", c.nMap, c.nReduce)

	c.server()

	go c.checkTasksPeriodically()

	return &c
}
