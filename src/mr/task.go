package mr

import "fmt"

type Task struct {
	TaskType   TaskType
	Inputfiles []string
	TaskId     int
}

func (t Task) toString() string {
	return fmt.Sprintf("%v", t)
}

type TaskStatus int

const (
	Unknown TaskStatus = iota
	Idle
	InProgress
	Completed
)

func (ts TaskStatus) String() string {
	switch ts {
	case Idle:
		return "Idle"
	case InProgress:
		return "In progress"
	case Completed:
		return "Completed"
	}
	return "Unknown"

}

type TaskType int

const (
	Undefined TaskType = iota
	Map
	Reduce
)

func (tt TaskType) String() string {
	switch tt {
	case Map:
		return "Map"
	case Reduce:
		return "Reduce"
	}
	return "Undefined"
}
