package gasket

import (
	"fmt"
	"time"
)

func (c *Client) NewTask(taskType string, payload []byte, opts ...TaskOption) (task *Task, err error) {
	task = &Task{
		TaskType:  taskType,
		Payload:   payload,
		CreatedAt: time.Now(),
		Active:    false,
	}

	for _, opt := range opts {
		if err = opt(task); err != nil {
			return
		}
	}

	// Add it to the DB (which sets ID)
	err = c.storeTask(task)
	return
}

func (c *Client) MustNewTask(taskType string, payload []byte, opts ...TaskOption) (task *Task) {
	var err error
	if task, err = c.NewTask(taskType, payload, opts...); err != nil {
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
			ScheduledFor:   time.Now().Add(duration),
			TimeIsDeadline: false,
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
			ScheduledFor:   time.Now().Add(futureDuration),
			TimeIsDeadline: true,
		}

		return
	}
}

// We allow overwriting an existing retry policy since this is is the only way to set the retry policy, avoiding any ambiguity around half-set options.
func RetryPolicy(maxRetries int, retryDelay time.Duration) TaskOption {
	return func(task *Task) (err error) {
		task._retryPolicy = &TaskRetryPolicy{
			MaximumRetries: maxRetries,
			RetryDelay:     retryDelay,
			RetryCount:     0,
			LastTriedAt:    time.Time{},
		}

		return
	}
}

func (t *Task) GetInfo() (info *TaskInfo) {
	info = &TaskInfo{
		ID:         t.ID,
		TaskType:   t.TaskType,
		Payload:    t.Payload,
		CreatedAt:  t.CreatedAt,
		EnqueuedAt: t.EnqueuedAt,
	}

	if t._schedule != nil {
		info.SchedulingInfo = &TaskScheduleInfo{
			ScheduledFor:   t._schedule.ScheduledFor,
			TimeIsDeadline: t._schedule.TimeIsDeadline,
		}
	}

	if t._retryPolicy != nil {
		info.RetryPolicyInfo = &TaskRetryPolicyInfo{
			MaximumRetries: t._retryPolicy.MaximumRetries,
			RetryCount:     t._retryPolicy.RetryCount,
		}
	}

	return
}

func (c *Client) storeTask(task *Task) (err error) {
	if err = c.tasksDB.Insert(task); err != nil {
		return
	}

	if task._schedule != nil {
		task._schedule.TaskID = task.ID

		if err = c.taskSchedulesDB.Insert(task._schedule); err != nil {
			return
		}
	}

	if task._retryPolicy != nil {
		task._retryPolicy.TaskID = task.ID

		if err = c.taskRetryPoliciesDB.Insert(task._retryPolicy); err != nil {
			return
		}
	}

	return
}

// Warning: this only loads the task, not the schedule or retry policy. We will "comlete" the task in Client.finishLoadingTask.
func (c *Client) loadTask(id int) (task *Task, err error) {
	if task, err = c.tasksDB.Select(id); err != nil {
		return
	}

	if task._schedule, err = c.taskSchedulesDB.Select(task.ID); err != nil {
		return
	}

	if task._retryPolicy, err = c.taskRetryPoliciesDB.Select(task.ID); err != nil {
		return
	}

	return
}

// Cascading deletes will take care of the schedule and retry policy, so we only need to delete the task!
func (c *Client) deleteTask(id int) (err error) {
	err = c.tasksDB.Delete(id)
	return
}
