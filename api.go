package gasket

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/z46-dev/gomysql"
)

const (
	defaultRunPollInterval time.Duration = 10 * time.Millisecond
	defaultLockRetryCount  int           = 20
	defaultLockRetryDelay  time.Duration = 5 * time.Millisecond
)

func NewClient(sqlDatabaseFilePath string, opts ...ClientOption) (client *Client, err error) {
	client = &Client{
		consumers:           map[string]TaskConsumerFunc{},
		runPollInterval:     defaultRunPollInterval,
		lockRetryCount:      defaultLockRetryCount,
		lockRetryDelay:      defaultLockRetryDelay,
		taskRecoveryTimeout: 0,
	}

	for _, opt := range opts {
		if err = opt(client); err != nil {
			return
		}
	}

	if client.driver, err = gomysql.Begin(sqlDatabaseFilePath); err != nil {
		return
	}

	if err = client.register(); err != nil {
		return
	}

	return
}

func (c *Client) Close() (err error) {
	if c.driver != nil {
		err = c.driver.Close()
	}

	return
}

func (c *Client) register() (err error) {
	if c.tasksDB, err = gomysql.Register(c.driver, task{}); err != nil {
		return
	}

	if c.taskResultsDB, err = gomysql.Register(c.driver, taskResult{}); err != nil {
		return
	}

	if c.taskSchedulesDB, err = gomysql.Register(c.driver, taskSchedule{}); err != nil {
		return
	}

	if c.taskRetryPoliciesDB, err = gomysql.Register(c.driver, taskRetryPolicy{}); err != nil {
		return
	}

	return
}

func (c *Client) GetActiveDatabaseFilePath() (filePath string) {
	if c.driver != nil {
		filePath = c.driver.GetFilePath()
	}

	return
}

func (c *Client) RegisterConsumer(taskType string, consumer TaskConsumerFunc) (err error) {
	if taskType == "" {
		err = fmt.Errorf(errTaskTypeEmpty)
		return
	}

	if consumer == nil {
		err = fmt.Errorf(errConsumerNil)
		return
	}

	var exists bool
	if _, exists = c.consumers[taskType]; exists {
		err = fmt.Errorf(errConsumerAlreadyExists, taskType)
		return
	}

	c.consumers[taskType] = consumer
	return
}

func (c *Client) Run(ctx context.Context) (err error) {
	var ticker *time.Ticker = time.NewTicker(c.runPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var progressed bool
		if progressed, err = c.recoverInterruptedTasks(); err != nil {
			return
		} else if progressed {
			continue
		}

		if progressed, err = c.runNextDueTask(); err != nil {
			return
		} else if progressed {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Client) runNextDueTask() (ranTask bool, err error) {
	var dueTasks []*task
	if dueTasks, err = c.loadDueTasks(); err != nil {
		return
	}

	for _, task := range dueTasks {
		var consumer TaskConsumerFunc = c.consumers[task.TaskType]
		if consumer == nil {
			continue
		}

		var claimed bool
		if claimed, err = c.claimTask(task); err != nil {
			return
		} else if !claimed {
			continue
		}

		if task, err = c.loadTask(task.ID); err != nil {
			return
		}

		if c.taskDeadlineExceeded(task) {
			if err = c.markTaskDeadlineExceeded(task); err != nil {
				return
			}

			ranTask = true
			return
		}

		if err = c.markTaskRunning(task); err != nil {
			return
		}

		if err = c.markTaskFinished(task, c.callTaskConsumer(consumer, task.ID, task.Payload), true); err != nil {
			return
		}

		ranTask = true
		return
	}

	return
}

func (c *Client) recoverInterruptedTasks() (recovered bool, err error) {
	if c.taskRecoveryTimeout <= 0 {
		return
	}

	var interruptedTasks []*task
	if interruptedTasks, err = c.loadInterruptedTasks(); err != nil {
		return
	}

	for _, task := range interruptedTasks {
		if err = c.markTaskFinished(task, TaskConsumerResult{
			Success: false,
			Error:   fmt.Errorf(errTaskInterrupted),
		}, true); err != nil {
			return
		}

		recovered = true
	}

	return
}

func (c *Client) taskDeadlineExceeded(task *task) bool {
	return task._schedule != nil && task._schedule.TimeIsDeadline && time.Now().After(task._schedule.ScheduledFor)
}

func (c *Client) claimTask(task *task) (claimed bool, err error) {
	var enqueuedAt time.Time = time.Now()

	var filter *gomysql.Filter = gomysql.NewFilter().
		KeyCmp(c.tasksDB.FieldByGoName("ID"), gomysql.OpEqual, task.ID).
		And().
		KeyCmp(c.tasksDB.FieldByGoName("State"), gomysql.OpEqual, TaskStatePending)

	var rows []gomysql.ReturnedValues
	if rows, err = c.updateTaskWithRetry(filter,
		[]*gomysql.RegisteredStructField{c.tasksDB.FieldByGoName("ID")},
		gomysql.SetField(c.tasksDB.FieldByGoName("State"), TaskStateEnqueued),
		gomysql.SetField(c.tasksDB.FieldByGoName("EnqueuedAt"), enqueuedAt),
		gomysql.SetField(c.tasksDB.FieldByGoName("Active"), true),
	); err != nil {
		return
	}

	if len(rows) == 0 {
		return
	}

	task.State = TaskStateEnqueued
	task.EnqueuedAt = &enqueuedAt
	task.Active = true
	claimed = true
	return
}

func (c *Client) loadDueTasks() (tasks []*task, err error) {
	var filter *gomysql.Filter = gomysql.NewFilter().
		KeyCmp(c.tasksDB.FieldByGoName("State"), gomysql.OpEqual, TaskStatePending).
		And().
		KeyCmp(c.tasksDB.FieldByGoName("NextRunAt"), gomysql.OpLessThanOrEqual, time.Now()).
		Ordering(c.tasksDB.FieldByGoName("NextRunAt"), true).
		Ordering(c.tasksDB.FieldByGoName("ID"), true)

	if tasks, err = c.selectAllTasksWithRetry(filter); err != nil {
		return
	}

	for _, task := range tasks {
		if task._schedule, err = c.selectTaskScheduleWithRetry(task.ID); err != nil {
			return
		}

		task._client = c
	}

	sort.SliceStable(tasks, func(i int, j int) bool {
		var (
			iIsImperative bool = taskIsImperativeDelay(tasks[i])
			jIsImperative bool = taskIsImperativeDelay(tasks[j])
		)

		if iIsImperative != jIsImperative {
			return iIsImperative
		}

		if tasks[i].NextRunAt == nil || tasks[j].NextRunAt == nil {
			return tasks[i].ID < tasks[j].ID
		}

		if !tasks[i].NextRunAt.Equal(*tasks[j].NextRunAt) {
			return tasks[i].NextRunAt.Before(*tasks[j].NextRunAt)
		}

		return tasks[i].ID < tasks[j].ID
	})

	return
}

func (c *Client) loadInterruptedTasks() (tasks []*task, err error) {
	var cutoff time.Time = time.Now().Add(-c.taskRecoveryTimeout)

	var filter *gomysql.Filter = gomysql.NewFilter().
		OpenGroup().
		KeyCmp(c.tasksDB.FieldByGoName("State"), gomysql.OpEqual, TaskStateEnqueued).
		And().
		KeyCmp(c.tasksDB.FieldByGoName("EnqueuedAt"), gomysql.OpLessThanOrEqual, cutoff).
		CloseGroup().
		Or().
		OpenGroup().
		KeyCmp(c.tasksDB.FieldByGoName("State"), gomysql.OpEqual, TaskStateRunning).
		And().
		KeyCmp(c.tasksDB.FieldByGoName("StartedAt"), gomysql.OpLessThanOrEqual, cutoff).
		CloseGroup().
		Ordering(c.tasksDB.FieldByGoName("ID"), true)

	if tasks, err = c.selectAllTasksWithRetry(filter); err != nil {
		return
	}

	for _, task := range tasks {
		if task._schedule, err = c.selectTaskScheduleWithRetry(task.ID); err != nil {
			return
		}

		if task._retryPolicy, err = c.selectTaskRetryPolicyWithRetry(task.ID); err != nil {
			return
		}

		task._client = c
	}

	return
}

func taskIsImperativeDelay(task *task) bool {
	return task._schedule != nil && !task._schedule.TimeIsDeadline && task._schedule.IsImperative
}

func (c *Client) markTaskRunning(task *task) (err error) {
	var startedAt time.Time = time.Now()

	task.State = TaskStateRunning
	task.StartedAt = &startedAt
	task.Active = true

	if err = c.updateSingleTaskWithRetry(task); err != nil {
		return
	}

	if task._retryPolicy != nil {
		task._retryPolicy.LastTriedAt = startedAt
		err = c.updateRetryPolicyWithRetry(task._retryPolicy)
	}

	return
}

func (c *Client) markTaskFinished(task *task, result TaskConsumerResult, allowRetry bool) (err error) {
	if allowRetry && c.taskCanRetry(task, result) {
		err = c.markTaskRetryPending(task)
		return
	}

	var completedAt time.Time = time.Now()

	task.CompletedAt = &completedAt
	task.NextRunAt = nil
	task.Active = false

	if result.Success && result.Error == nil {
		task.State = TaskStateCompleted
		task.LastError = nil
	} else {
		var lastError string = errConsumerFailed

		task.State = TaskStateFailed
		if result.Error != nil {
			lastError = result.Error.Error()
		}

		task.LastError = &lastError
	}

	if err = c.storeTaskResult(task, result); err != nil {
		return
	}

	err = c.updateSingleTaskWithRetry(task)
	return
}

func (c *Client) callTaskConsumer(consumer TaskConsumerFunc, id int, payload []byte) (result TaskConsumerResult) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = TaskConsumerResult{
				Success: false,
				Error:   fmt.Errorf(errConsumerPanic, recovered),
			}
		}
	}()

	result = consumer(id, payload)
	return
}

func (c *Client) updateSingleTaskWithRetry(task *task) (err error) {
	for range c.lockRetryCount {
		if err = c.tasksDB.Update(task); err == nil {
			return
		} else if !isDatabaseLockedError(err) {
			return
		}

		time.Sleep(c.lockRetryDelay)
	}

	return
}

func (c *Client) updateRetryPolicyWithRetry(policy *taskRetryPolicy) (err error) {
	for range c.lockRetryCount {
		if err = c.taskRetryPoliciesDB.Update(policy); err == nil {
			return
		} else if !isDatabaseLockedError(err) {
			return
		}

		time.Sleep(c.lockRetryDelay)
	}

	return
}

func (c *Client) insertTaskResultWithRetry(result *taskResult) (err error) {
	for range c.lockRetryCount {
		if err = c.taskResultsDB.Insert(result); err == nil {
			return
		} else if !isDatabaseLockedError(err) {
			return
		}

		time.Sleep(c.lockRetryDelay)
	}

	return
}

func (c *Client) selectTaskResultWithRetry(id int) (result *taskResult, err error) {
	for range c.lockRetryCount {
		if result, err = c.taskResultsDB.Select(id); err == nil {
			return
		} else if !isDatabaseLockedError(err) {
			return
		}

		time.Sleep(c.lockRetryDelay)
	}

	return
}

func (c *Client) selectTaskScheduleWithRetry(id int) (schedule *taskSchedule, err error) {
	for range c.lockRetryCount {
		if schedule, err = c.taskSchedulesDB.Select(id); err == nil {
			return
		} else if !isDatabaseLockedError(err) {
			return
		}

		time.Sleep(c.lockRetryDelay)
	}

	return
}

func (c *Client) selectTaskRetryPolicyWithRetry(id int) (policy *taskRetryPolicy, err error) {
	for range c.lockRetryCount {
		if policy, err = c.taskRetryPoliciesDB.Select(id); err == nil {
			return
		} else if !isDatabaseLockedError(err) {
			return
		}

		time.Sleep(c.lockRetryDelay)
	}

	return
}

func (c *Client) selectAllTasksWithRetry(filter *gomysql.Filter) (tasks []*task, err error) {
	for range c.lockRetryCount {
		if tasks, err = c.tasksDB.SelectAllWithFilter(filter); err == nil {
			return
		} else if !isDatabaseLockedError(err) {
			return
		}

		time.Sleep(c.lockRetryDelay)
	}

	return
}

func (c *Client) taskCanRetry(task *task, result TaskConsumerResult) bool {
	return !result.Success && task._retryPolicy != nil && task._retryPolicy.RetryCount < task._retryPolicy.MaximumRetries
}

func (c *Client) markTaskRetryPending(task *task) (err error) {
	var nextRunAt time.Time = time.Now().Add(task._retryPolicy.RetryDelay)

	task.State = TaskStatePending
	task.NextRunAt = &nextRunAt
	task.Active = false
	task.StartedAt = nil
	task.CompletedAt = nil
	task.LastError = nil

	if err = c.updateSingleTaskWithRetry(task); err != nil {
		return
	}

	task._retryPolicy.RetryCount++
	err = c.updateRetryPolicyWithRetry(task._retryPolicy)
	return
}

func (c *Client) markTaskDeadlineExceeded(task *task) (err error) {
	err = c.markTaskFinished(task, TaskConsumerResult{
		Success: false,
		Error:   fmt.Errorf(errTaskDeadlineExceeded),
	}, false)
	return
}

func (c *Client) storeTaskResult(task *task, result TaskConsumerResult) (err error) {
	var errorMessage *string
	if result.Error != nil {
		var message string = result.Error.Error()
		errorMessage = &message
	} else if !result.Success {
		var message string = errConsumerFailed
		errorMessage = &message
	}

	task._result = &taskResult{
		TaskID:       task.ID,
		Success:      result.Success,
		ErrorMessage: errorMessage,
		Data:         result.Data,
	}

	err = c.insertTaskResultWithRetry(task._result)
	return
}

func (c *Client) updateTaskWithRetry(filter *gomysql.Filter, returning []*gomysql.RegisteredStructField, assignments ...gomysql.UpdateAssignment) (rows []gomysql.ReturnedValues, err error) {
	for range c.lockRetryCount {
		if rows, err = c.tasksDB.UpdateWithFilterReturning(filter, returning, assignments...); err == nil {
			return
		} else if !isDatabaseLockedError(err) {
			return
		}

		time.Sleep(c.lockRetryDelay)
	}

	return
}

func isDatabaseLockedError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "database is locked")
}
