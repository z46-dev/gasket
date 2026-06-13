package gasket

import (
	"database/sql"
	"time"

	"github.com/z46-dev/gosqlite"
)

type (
	TaskState string

	// WARNING: It is inadvisable to edit any fields yourself. Fields are public because of database mapping.
	task struct {
		ID          int        `gosqlite:"id,primary,increment"`
		TaskType    string     `gosqlite:"task_type"`
		Payload     []byte     `gosqlite:"payload"`
		State       TaskState  `gosqlite:"state"`
		CreatedAt   time.Time  `gosqlite:"created_at"`
		NextRunAt   *time.Time `gosqlite:"next_run_at"`
		EnqueuedAt  *time.Time `gosqlite:"enqueued_at"`
		StartedAt   *time.Time `gosqlite:"started_at"`
		CompletedAt *time.Time `gosqlite:"completed_at"`
		LastError   *string    `gosqlite:"last_error"`
		Active      bool       `gosqlite:"active"`

		// These are for creation, not used elsewhere
		_schedule    *taskSchedule
		_result      *taskResult
		_retryPolicy *taskRetryPolicy
		_client      *Client
	}

	// WARNING: It is inadvisable to edit any fields yourself.
	taskSchedule struct {
		TaskID         int       `gosqlite:"task_id,primary,fkey:task.id,ondelete:cascade"`
		ScheduledFor   time.Time `gosqlite:"scheduled_for"`
		TimeIsDeadline bool      `gosqlite:"time_is_deadline"`
		IsImperative   bool      `gosqlite:"is_imperative"`
	}

	// WARNING: It is inadvisable to edit any fields yourself.
	taskRetryPolicy struct {
		TaskID         int           `gosqlite:"task_id,primary,fkey:task.id,ondelete:cascade"`
		MaximumRetries int           `gosqlite:"maximum_retries"`
		RetryDelay     time.Duration `gosqlite:"retry_delay"`
		RetryCount     int           `gosqlite:"retry_count"`
		LastTriedAt    time.Time     `gosqlite:"last_tried_at"`
	}

	// WARNING: It is inadvisable to edit any fields yourself.
	taskResult struct {
		TaskID       int     `gosqlite:"task_id,primary,fkey:task.id,ondelete:cascade"`
		Success      bool    `gosqlite:"success"`
		ErrorMessage *string `gosqlite:"error_message"`
		Data         []byte  `gosqlite:"data"`
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
		sqlDB               *sql.DB
		driver              *gosqlite.Driver
		tasksDB             *gosqlite.RegisteredStruct[task]
		taskResultsDB       *gosqlite.RegisteredStruct[taskResult]
		taskSchedulesDB     *gosqlite.RegisteredStruct[taskSchedule]
		taskRetryPoliciesDB *gosqlite.RegisteredStruct[taskRetryPolicy]
		consumers           map[string]TaskConsumerFunc
		runPollInterval     time.Duration
		lockRetryCount      int
		lockRetryDelay      time.Duration
		taskRecoveryTimeout time.Duration
	}
)
