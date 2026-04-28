package gasket

import (
	"fmt"
	"time"
)

func (c *Client) NewTask(taskType string, payload []byte, opts ...TaskOption) (info *TaskInfo, err error) {
	var t = &task{
		TaskType:  taskType,
		Payload:   payload,
		CreatedAt: time.Now(),
		Active:    false,
	}

	for _, opt := range opts {
		if err = opt(t); err != nil {
			return
		}
	}

	// Add it to the DB (which sets ID)
	if err = c.storeTask(t); err != nil {
		return
	}

	info = t.GetInfo()
	return
}

func (c *Client) MustNewTask(taskType string, payload []byte, opts ...TaskOption) (info *TaskInfo) {
	var err error
	if info, err = c.NewTask(taskType, payload, opts...); err != nil {
		panic(fmt.Sprintf("Failed to create task: %v", err))
	}

	return
}

func ScheduleIn(duration time.Duration) TaskOption {
	return func(t *task) (err error) {
		if t._schedule != nil {
			err = fmt.Errorf(errCannotOverwriteSchedule)
			return
		}

		t._schedule = &taskSchedule{
			ScheduledFor:   time.Now().Add(duration),
			TimeIsDeadline: false,
		}

		return
	}
}

func RunBy(futureDuration time.Duration) TaskOption {
	return func(t *task) (err error) {
		if t._schedule != nil {
			err = fmt.Errorf(errCannotOverwriteSchedule)
			return
		}

		t._schedule = &taskSchedule{
			ScheduledFor:   time.Now().Add(futureDuration),
			TimeIsDeadline: true,
		}

		return
	}
}

// We allow overwriting an existing retry policy since this is is the only way to set the retry policy, avoiding any ambiguity around half-set options.
func RetryPolicy(maxRetries int, retryDelay time.Duration) TaskOption {
	return func(t *task) (err error) {
		t._retryPolicy = &taskRetryPolicy{
			MaximumRetries: maxRetries,
			RetryDelay:     retryDelay,
			RetryCount:     0,
			LastTriedAt:    time.Time{},
		}

		return
	}
}

func (t *task) GetInfo() (info *TaskInfo) {
	info = &TaskInfo{
		task: t,
	}

	return
}

func (ti *TaskInfo) ID() int {
	return ti.task.ID
}

func (ti *TaskInfo) TaskType() string {
	return ti.task.TaskType
}

func (ti *TaskInfo) Payload() []byte {
	return ti.task.Payload
}

func (ti *TaskInfo) CreatedAt() time.Time {
	return ti.task.CreatedAt
}

func (ti *TaskInfo) EnqueuedAt() *time.Time {
	return ti.task.EnqueuedAt
}

func (ti *TaskInfo) IsActive() bool {
	return ti.task.Active
}

func (ti *TaskInfo) SchedulingInfo() (info *TaskScheduleInfo) {
	if ti.task._schedule != nil {
		info = &TaskScheduleInfo{
			scheduleInfo: ti.task._schedule,
		}
	}

	return
}

func (ti *TaskInfo) RetryPolicyInfo() (info *TaskRetryPolicyInfo) {
	if ti.task._retryPolicy != nil {
		info = &TaskRetryPolicyInfo{
			retryPolicyInfo: ti.task._retryPolicy,
		}
	}

	return
}

func (tsi *TaskScheduleInfo) ScheduledFor() time.Time {
	return tsi.scheduleInfo.ScheduledFor
}

func (tsi *TaskScheduleInfo) TimeIsDeadline() bool {
	return tsi.scheduleInfo.TimeIsDeadline
}

func (trpi *TaskRetryPolicyInfo) MaximumRetries() int {
	return trpi.retryPolicyInfo.MaximumRetries
}

func (trpi *TaskRetryPolicyInfo) RetryDelay() time.Duration {
	return trpi.retryPolicyInfo.RetryDelay
}

func (trpi *TaskRetryPolicyInfo) RetryCount() int {
	return trpi.retryPolicyInfo.RetryCount
}

func (trpi *TaskRetryPolicyInfo) LastTriedAt() time.Time {
	return trpi.retryPolicyInfo.LastTriedAt
}

func (c *Client) storeTask(t *task) (err error) {
	if err = c.tasksDB.Insert(t); err != nil {
		return
	}

	if t._schedule != nil {
		t._schedule.TaskID = t.ID

		if err = c.taskSchedulesDB.Insert(t._schedule); err != nil {
			return
		}
	}

	if t._retryPolicy != nil {
		t._retryPolicy.TaskID = t.ID

		if err = c.taskRetryPoliciesDB.Insert(t._retryPolicy); err != nil {
			return
		}
	}

	return
}

// Warning: this only loads the task, not the schedule or retry policy. We will "comlete" the task in Client.finishLoadingTask.
func (c *Client) loadTask(id int) (t *task, err error) {
	if t, err = c.tasksDB.Select(id); err != nil {
		return
	}

	if t._schedule, err = c.taskSchedulesDB.Select(t.ID); err != nil {
		return
	}

	if t._retryPolicy, err = c.taskRetryPoliciesDB.Select(t.ID); err != nil {
		return
	}

	return
}

// Cascading deletes will take care of the schedule and retry policy, so we only need to delete the task!
func (c *Client) deleteTask(id int) (err error) {
	err = c.tasksDB.Delete(id)
	return
}
