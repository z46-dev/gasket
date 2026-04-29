package gasket

const (
	errCannotOverwriteSchedule    = "cannot overwrite existing schedule for task"
	errConsumerAlreadyExists      = "consumer already registered for task type %s"
	errConsumerFailed             = "task consumer reported failure without an error"
	errConsumerNil                = "task consumer cannot be nil"
	errConsumerPanic              = "task consumer panicked: %v"
	errDatabaseLockedRetryDelay   = "database lock retry delay must be greater than zero"
	errDatabaseLockedRetryCount   = "database lock retry count must be greater than zero"
	errNoTaskWithID               = "no task found with ID %d"
	errPollInterval               = "poll interval must be greater than zero"
	errTaskCannotBeCancelled      = "task %d cannot be cancelled from state %s"
	errTaskCancelled              = "task %d was cancelled"
	errTaskDeadlineExceeded       = "task deadline exceeded"
	errTaskFinishedWithoutEnqueue = "task %d finished before being enqueued"
	errTaskFinishedWithoutData    = "task %d finished without stored result data"
	errTaskInterrupted            = "task interrupted before completion"
	errTaskTypeEmpty              = "task type cannot be empty"
	errTaskRecoveryTimeout        = "task recovery timeout cannot be negative"
)

const (
	TaskStatePending   TaskState = "pending"
	TaskStateEnqueued  TaskState = "enqueued"
	TaskStateRunning   TaskState = "running"
	TaskStateCompleted TaskState = "completed"
	TaskStateFailed    TaskState = "failed"
	TaskStateCancelled TaskState = "cancelled"
)
