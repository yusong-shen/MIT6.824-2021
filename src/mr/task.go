package mr

import (
	"fmt"
	"time"
)

type Task struct {
	TaskType   TaskType
	Inputfiles []string
	TaskId     int
	StartTime  time.Time
}

func (t Task) toString() string {
	// ignore the StartTime
	return fmt.Sprintf("%v-%v-%v", t.TaskType, t.TaskId, t.Inputfiles)
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
