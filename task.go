package gasket

import (
	"fmt"
	"time"
)

func NewTask(taskType string, payload []byte, opts ...TaskOption) (task *Task, err error) {
	task = &Task{
		taskType:  taskType,
		payload:   payload,
		createdAt: time.Now(),
	}

	for _, opt := range opts {
		if err = opt(task); err != nil {
			return
		}
	}

	return
}

func MustNewTask(taskType string, payload []byte, opts ...TaskOption) (task *Task) {
	var err error
	if task, err = NewTask(taskType, payload, opts...); err != nil {
		panic(fmt.Sprintf("Failed to create task: %v", err))
	}

	return
}

func ScheduleIn(duration time.Duration) TaskOption {
	return func(task *Task) (err error) {
		if task._schedule != nil {
			err = fmt.Errorf(errCannotOverwriteSchedule)
			return
		}

		task._schedule = &TaskSchedule{
			scheduledFor:   time.Now().Add(duration),
			timeIsDeadline: false,
		}

		return
	}
}

func RunBy(futureDuration time.Duration) TaskOption {
	return func(task *Task) (err error) {
		if task._schedule != nil {
			err = fmt.Errorf(errCannotOverwriteSchedule)
			return
		}

		task._schedule = &TaskSchedule{
			scheduledFor:   time.Now().Add(futureDuration),
			timeIsDeadline: true,
		}

		return
	}
}

// We allow overwriting an existing retry policy since this is is the only way to set the retry policy, avoiding any ambiguity around half-set options.
func RetryPolicy(maxRetries int, retryDelay time.Duration) TaskOption {
	return func(task *Task) (err error) {
		task._retryPolicy = &TaskRetryPolicy{
			maximumRetries: maxRetries,
			retryDelay:     retryDelay,
		}

		return
	}
}

func (t *Task) GetInfo() (info *TaskInfo) {
	info = &TaskInfo{
		ID:         t.id,
		TaskType:   t.taskType,
		Payload:    t.payload,
		CreatedAt:  t.createdAt,
		EnqueuedAt: t.enqueuedAt,
	}

	if t._schedule != nil {
		info.SchedulingInfo = &TaskScheduleInfo{
			ScheduledFor:   t._schedule.scheduledFor,
			TimeIsDeadline: t._schedule.timeIsDeadline,
		}
	}

	if t._retryPolicy != nil {
		info.RetryPolicyInfo = &TaskRetryPolicyInfo{
			MaximumRetries: t._retryPolicy.maximumRetries,
			RetryCount:     t._retryPolicy.retryCount,
		}
	}

	return
}
