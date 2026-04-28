package gasket

import (
	"time"

	"github.com/z46-dev/gomysql"
)

type (
	// WARNING: It is inadvisable to edit any fields yourself.
	Task struct {
		ID         int       `gomysql:"id,primary,increment"`
		TaskType   string    `gomysql:"task_type"`
		Payload    []byte    `gomysql:"payload"`
		CreatedAt  time.Time `gomysql:"created_at"`
		EnqueuedAt time.Time `gomysql:"enqueued_at"`
		Active     bool      `gomysql:"active"`

		// These are for creation, not used elsewhere
		_schedule    *TaskSchedule
		_retryPolicy *TaskRetryPolicy
	}

	// WARNING: It is inadvisable to edit any fields yourself.
	TaskSchedule struct {
		TaskID         int       `gomysql:"task_id,primary,fkey:task.id,ondelete:cascade"`
		ScheduledFor   time.Time `gomysql:"scheduled_for"`
		TimeIsDeadline bool      `gomysql:"time_is_deadline"`
	}

	// WARNING: It is inadvisable to edit any fields yourself.
	TaskRetryPolicy struct {
		TaskID         int           `gomysql:"task_id,primary,fkey:task.id,ondelete:cascade"`
		MaximumRetries int           `gomysql:"maximum_retries"`
		RetryDelay     time.Duration `gomysql:"retry_delay"`
		RetryCount     int           `gomysql:"retry_count"`
		LastTriedAt    time.Time     `gomysql:"last_tried_at"`
	}

	// TaskInfo exists to tell people about the task without passing them a mutable Task struct, so we can keep the fields of Task private.
	TaskInfo struct {
		ID              int
		TaskType        string
		Payload         []byte
		CreatedAt       time.Time
		EnqueuedAt      time.Time
		SchedulingInfo  *TaskScheduleInfo
		RetryPolicyInfo *TaskRetryPolicyInfo
	}

	TaskScheduleInfo struct {
		ScheduledFor   time.Time
		TimeIsDeadline bool
	}

	TaskRetryPolicyInfo struct {
		MaximumRetries int
		RetryCount     int
		RetryDelay     time.Duration
		LastTriedAt    time.Time
	}

	TaskConsumerResult struct {
		Success bool
		Error   error
	}

	TaskConsumerFunc func(id int, payload []byte) (result TaskConsumerResult)

	TaskOption func(task *Task) (err error)

	Client struct {
		driver              *gomysql.Driver
		tasksDB             *gomysql.RegisteredStruct[Task]
		taskSchedulesDB     *gomysql.RegisteredStruct[TaskSchedule]
		taskRetryPoliciesDB *gomysql.RegisteredStruct[TaskRetryPolicy]
		consumers           map[string]TaskConsumerFunc
	}
)
