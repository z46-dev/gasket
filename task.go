package gasket

import (
	"fmt"
	"time"

	"github.com/z46-dev/gomysql"
)

func (c *Client) NewTask(taskType string, payload []byte, opts ...TaskOption) (info *TaskInfo, err error) {
	var createdAt time.Time = time.Now()

	var t = &task{
		TaskType:  taskType,
		Payload:   payload,
		State:     TaskStatePending,
		CreatedAt: createdAt,
		NextRunAt: &createdAt,
		Active:    false,
		_client:   c,
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

		var scheduledFor time.Time = time.Now().Add(duration)

		t._schedule = &taskSchedule{
			ScheduledFor:   scheduledFor,
			TimeIsDeadline: false,
			IsImperative:   false,
		}

		t.NextRunAt = &scheduledFor

		return
	}
}

func RunIn(duration time.Duration) TaskOption {
	return func(t *task) (err error) {
		if t._schedule != nil {
			err = fmt.Errorf(errCannotOverwriteSchedule)
			return
		}

		var scheduledFor time.Time = time.Now().Add(duration)

		t._schedule = &taskSchedule{
			ScheduledFor:   scheduledFor,
			TimeIsDeadline: false,
			IsImperative:   true,
		}

		t.NextRunAt = &scheduledFor

		return
	}
}

func RunBy(futureDuration time.Duration) TaskOption {
	return func(t *task) (err error) {
		if t._schedule != nil {
			err = fmt.Errorf(errCannotOverwriteSchedule)
			return
		}

		var scheduledFor time.Time = time.Now().Add(futureDuration)

		t._schedule = &taskSchedule{
			ScheduledFor:   scheduledFor,
			TimeIsDeadline: true,
			IsImperative:   false,
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
		task:   t,
		client: t._client,
	}

	return
}

func (ti *TaskInfo) Refresh() (err error) {
	var t *task
	if t, err = ti.client.loadTask(ti.ID()); err != nil {
		return
	}

	ti.task = t
	return
}

func (ti *TaskInfo) WaitForCompletion() (result TaskConsumerResult, err error) {
	for {
		if err = ti.Refresh(); err != nil {
			return
		}

		switch ti.State() {
		case TaskStateCompleted, TaskStateFailed:
			if ti.task._result != nil {
				result = ti.task._result.GetConsumerResult()
				return
			}

			if ti.State() == TaskStateCompleted {
				result.Success = true
				return
			}

			result.Success = false
			if ti.task.LastError != nil {
				result.Error = fmt.Errorf("%s", *ti.task.LastError)
				return
			}

			err = fmt.Errorf(errTaskFinishedWithoutData, ti.ID())
			return
		case TaskStateCancelled:
			err = fmt.Errorf(errTaskCancelled, ti.ID())
			return
		}

		time.Sleep(ti.client.runPollInterval)
	}
}

func (ti *TaskInfo) WaitForEnqueue() (err error) {
	for {
		if err = ti.Refresh(); err != nil {
			return
		}

		if ti.EnqueuedAt() != nil {
			return
		}

		switch ti.State() {
		case TaskStateCancelled:
			err = fmt.Errorf(errTaskCancelled, ti.ID())
			return
		case TaskStateCompleted, TaskStateFailed:
			err = fmt.Errorf(errTaskFinishedWithoutEnqueue, ti.ID())
			return
		}

		time.Sleep(ti.client.runPollInterval)
	}
}

func (ti *TaskInfo) Cancel() (err error) {
	var completedAt time.Time = time.Now()

	var filter *gomysql.Filter = gomysql.NewFilter().
		KeyCmp(ti.client.tasksDB.FieldByGoName("ID"), gomysql.OpEqual, ti.ID()).
		And().
		KeyCmp(ti.client.tasksDB.FieldByGoName("State"), gomysql.OpEqual, TaskStatePending)

	var rows []gomysql.ReturnedValues
	if rows, err = ti.client.updateTaskWithRetry(
		filter,
		[]*gomysql.RegisteredStructField{ti.client.tasksDB.FieldByGoName("ID")},
		gomysql.SetField(ti.client.tasksDB.FieldByGoName("State"), TaskStateCancelled),
		gomysql.SetField(ti.client.tasksDB.FieldByGoName("NextRunAt"), nil),
		gomysql.SetField(ti.client.tasksDB.FieldByGoName("CompletedAt"), completedAt),
		gomysql.SetField(ti.client.tasksDB.FieldByGoName("Active"), false),
	); err != nil {
		return
	}

	if len(rows) == 0 {
		if err = ti.Refresh(); err != nil {
			return
		}

		err = fmt.Errorf(errTaskCannotBeCancelled, ti.ID(), ti.State())
		return
	}

	ti.task.State = TaskStateCancelled
	ti.task.NextRunAt = nil
	ti.task.CompletedAt = &completedAt
	ti.task.Active = false
	ti.task.LastError = nil

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

func (ti *TaskInfo) State() TaskState {
	return ti.task.State
}

func (ti *TaskInfo) NextRunAt() *time.Time {
	return ti.task.NextRunAt
}

func (ti *TaskInfo) EnqueuedAt() *time.Time {
	return ti.task.EnqueuedAt
}

func (ti *TaskInfo) StartedAt() *time.Time {
	return ti.task.StartedAt
}

func (ti *TaskInfo) CompletedAt() *time.Time {
	return ti.task.CompletedAt
}

func (ti *TaskInfo) LastError() *string {
	return ti.task.LastError
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

func (tsi *TaskScheduleInfo) IsImperative() bool {
	return tsi.scheduleInfo.IsImperative
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

func (tr *taskResult) GetConsumerResult() (result TaskConsumerResult) {
	result.Success = tr.Success
	result.Data = tr.Data

	if tr.ErrorMessage != nil {
		result.Error = fmt.Errorf("%s", *tr.ErrorMessage)
	}

	return
}

func (c *Client) loadTask(id int) (t *task, err error) {
	return c.loadTaskWithDetails(id, true)
}

func (c *Client) loadTaskForExecution(id int) (t *task, err error) {
	return c.loadTaskWithDetails(id, false)
}

func (c *Client) loadTaskWithDetails(id int, includeResult bool) (t *task, err error) {
	if t, err = c.tasksDB.Select(id); err != nil {
		return
	}

	if t == nil {
		err = fmt.Errorf(errNoTaskWithID, id)
		return
	}

	if t._schedule, err = c.selectTaskScheduleWithRetry(t.ID); err != nil {
		return
	}

	if t._retryPolicy, err = c.selectTaskRetryPolicyWithRetry(t.ID); err != nil {
		return
	}

	if includeResult {
		if t._result, err = c.selectTaskResultWithRetry(t.ID); err != nil {
			return
		}
	}

	t._client = c
	return
}
