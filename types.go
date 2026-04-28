package gasket

import (
	"time"

	"github.com/z46-dev/gomysql"
)

type (
	Task struct {
		id         int       `gomysql:"id,primary,increment"`
		taskType   string    `gomysql:"task_type"`
		payload    []byte    `gomysql:"payload"`
		createdAt  time.Time `gomysql:"created_at"`
		enqueuedAt time.Time `gomysql:"enqueued_at"`

		// These are for creation, not used elsewhere
		_schedule    *TaskSchedule
		_retryPolicy *TaskRetryPolicy
	}

	TaskSchedule struct {
		id             int       `gomysql:"id,primary,increment"`
		taskID         int       `gomysql:"task_id,fkey:task.id"`
		scheduledFor   time.Time `gomysql:"scheduled_for"`
		timeIsDeadline bool      `gomysql:"time_is_deadline"`
	}

	TaskRetryPolicy struct {
		id             int           `gomysql:"id,primary,increment"`
		taskID         int           `gomysql:"task_id,fkey:task.id"`
		maximumRetries int           `gomysql:"maximum_retries"`
		retryCount     int           `gomysql:"retry_count"`
		retryDelay     time.Duration `gomysql:"retry_delay"`
		lastTriedAt    time.Time     `gomysql:"last_tried_at"`
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
