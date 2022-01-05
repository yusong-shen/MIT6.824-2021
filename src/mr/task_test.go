package mr

import "testing"

func TestTaskStatus(t *testing.T) {
	tests := []struct {
		status TaskStatus
		want   string
	}{
		{
			status: Unknown,
			want:   "Unknown",
		},
		{
			status: Idle,
			want:   "Idle",
		},
		{
			status: InProgress,
			want:   "In progress",
		},
		{
			status: Completed,
			want:   "Completed",
		},
	}

	for _, tc := range tests {
		if got := tc.status.String(); got != tc.want {
			t.Errorf("got String(%v) = %v, want %v", tc.status, got, tc.want)
		}
	}

}

func TestTaskType(t *testing.T) {
	tests := []struct {
		taskType TaskType
		want     string
	}{
		{
			taskType: Undefined,
			want:     "Undefined",
		},
		{
			taskType: Map,
			want:     "Map",
		},
		{
			taskType: Reduce,
			want:     "Reduce",
		},
	}

	for _, tc := range tests {
		if got := tc.taskType.String(); got != tc.want {
			t.Errorf("got String(%v) = %v, want %v", tc.taskType, got, tc.want)
		}
	}

}
