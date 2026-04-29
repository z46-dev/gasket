package gasket

import (
	"time"

	"github.com/z46-dev/gomysql"
)

type (
	TaskState string

	// WARNING: It is inadvisable to edit any fields yourself. Fields are public because of database mapping.
	task struct {
		ID          int        `gomysql:"id,primary,increment"`
		TaskType    string     `gomysql:"task_type"`
		Payload     []byte     `gomysql:"payload"`
		State       TaskState  `gomysql:"state"`
		CreatedAt   time.Time  `gomysql:"created_at"`
		NextRunAt   *time.Time `gomysql:"next_run_at"`
		EnqueuedAt  *time.Time `gomysql:"enqueued_at"`
		StartedAt   *time.Time `gomysql:"started_at"`
		CompletedAt *time.Time `gomysql:"completed_at"`
		LastError   *string    `gomysql:"last_error"`
		Active      bool       `gomysql:"active"`

		// These are for creation, not used elsewhere
		_schedule    *taskSchedule
		_result      *taskResult
		_retryPolicy *taskRetryPolicy
		_client      *Client
	}

	// WARNING: It is inadvisable to edit any fields yourself.
	taskSchedule struct {
		TaskID         int       `gomysql:"task_id,primary,fkey:task.id,ondelete:cascade"`
		ScheduledFor   time.Time `gomysql:"scheduled_for"`
		TimeIsDeadline bool      `gomysql:"time_is_deadline"`
		IsImperative   bool      `gomysql:"is_imperative"`
	}

	// WARNING: It is inadvisable to edit any fields yourself.
	taskRetryPolicy struct {
		TaskID         int           `gomysql:"task_id,primary,fkey:task.id,ondelete:cascade"`
		MaximumRetries int           `gomysql:"maximum_retries"`
		RetryDelay     time.Duration `gomysql:"retry_delay"`
		RetryCount     int           `gomysql:"retry_count"`
		LastTriedAt    time.Time     `gomysql:"last_tried_at"`
	}

	// WARNING: It is inadvisable to edit any fields yourself.
	taskResult struct {
		TaskID       int     `gomysql:"task_id,primary,fkey:task.id,ondelete:cascade"`
		Success      bool    `gomysql:"success"`
		ErrorMessage *string `gomysql:"error_message"`
		Data         []byte  `gomysql:"data"`
	}

	// TaskInfo exists to tell people about the task without passing them a mutable Task struct, so we can keep the fields of Task private.
	TaskInfo struct {
		task   *task
		client *Client
	}

	TaskScheduleInfo struct {
		scheduleInfo *taskSchedule
	}

	TaskRetryPolicyInfo struct {
		retryPolicyInfo *taskRetryPolicy
	}

	TaskConsumerResult struct {
		Success bool
		Error   error
		Data    []byte
	}

	TaskConsumerFunc func(id int, payload []byte) (result TaskConsumerResult)

	ClientOption func(client *Client) (err error)

	TaskOption func(task *task) (err error)

	Client struct {
		driver              *gomysql.Driver
		tasksDB             *gomysql.RegisteredStruct[task]
		taskResultsDB       *gomysql.RegisteredStruct[taskResult]
		taskSchedulesDB     *gomysql.RegisteredStruct[taskSchedule]
		taskRetryPoliciesDB *gomysql.RegisteredStruct[taskRetryPolicy]
		consumers           map[string]TaskConsumerFunc
		runPollInterval     time.Duration
		lockRetryCount      int
		lockRetryDelay      time.Duration
		taskRecoveryTimeout time.Duration
	}
)
