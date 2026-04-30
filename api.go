package gasket

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/gob"
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
	sqlTimeLayout          string        = "2006-01-02T15:04:05.000000000Z"
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

	if sqlDatabaseFilePath != ":memory:" {
		if client.sqlDB, err = sql.Open("sqlite", sqlDatabaseFilePath); err != nil {
			return
		}
		client.sqlDB.SetMaxOpenConns(1)
		client.sqlDB.SetMaxIdleConns(1)

		if _, err = client.sqlDB.Exec("PRAGMA foreign_keys = ON;"); err != nil {
			_ = client.sqlDB.Close()
			return
		}
	}

	if client.driver, err = gomysql.Begin(sqlDatabaseFilePath); err != nil {
		if client.sqlDB != nil {
			_ = client.sqlDB.Close()
		}
		return
	}

	if err = client.register(); err != nil {
		_ = client.Close()
		return
	}

	if err = client.prepareDatabase(); err != nil {
		_ = client.Close()
		return
	}

	return
}

func (c *Client) Close() (err error) {
	if c.sqlDB != nil {
		err = c.sqlDB.Close()
	}

	if c.driver != nil {
		if closeErr := c.driver.Close(); err == nil {
			err = closeErr
		}
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

func (c *Client) prepareDatabase() (err error) {
	var statements []string = []string{
		"CREATE INDEX IF NOT EXISTS idx_gasket_task_due ON task (state, next_run_at, task_type, id);",
		"CREATE INDEX IF NOT EXISTS idx_gasket_task_recovery_enqueued ON task (state, enqueued_at, id);",
		"CREATE INDEX IF NOT EXISTS idx_gasket_task_recovery_started ON task (state, started_at, id);",
	}

	if c.sqlDB == nil {
		return
	}

	for _, statement := range statements {
		if _, err = c.execSQLWithRetry(statement); err != nil {
			return
		}
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
	var (
		taskID   int
		task     *task
		consumer TaskConsumerFunc
		claimed  bool
	)

	if c.sqlDB == nil {
		return c.runNextDueTaskPortable()
	}

	if taskID, claimed, err = c.claimNextDueTaskID(); err != nil || !claimed {
		return
	}

	if task, err = c.loadTaskForExecution(taskID); err != nil {
		return
	}

	consumer = c.consumers[task.TaskType]
	if consumer == nil {
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

func (c *Client) runNextDueTaskPortable() (ranTask bool, err error) {
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

		if task, err = c.loadTaskForExecution(task.ID); err != nil {
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

func (c *Client) claimNextDueTaskID() (id int, claimed bool, err error) {
	var (
		taskTypes  []string
		enqueuedAt time.Time = time.Now()
		query      string
		args       []any
	)

	taskTypes = c.registeredTaskTypes()
	if len(taskTypes) == 0 {
		return
	}

	query, args = claimNextDueTaskSQL(taskTypes, enqueuedAt)
	for range c.lockRetryCount {
		err = c.sqlDB.QueryRow(query, args...).Scan(&id)
		if err == nil {
			claimed = true
			return
		}

		if err == sql.ErrNoRows {
			err = nil
			return
		}

		if !isDatabaseLockedError(err) {
			return
		}

		time.Sleep(c.lockRetryDelay)
	}

	return
}

func (c *Client) registeredTaskTypes() (taskTypes []string) {
	var taskType string

	taskTypes = make([]string, 0, len(c.consumers))
	for taskType = range c.consumers {
		taskTypes = append(taskTypes, taskType)
	}

	sort.Strings(taskTypes)
	return
}

func claimNextDueTaskSQL(taskTypes []string, enqueuedAt time.Time) (query string, args []any) {
	var placeholders string = strings.TrimSuffix(strings.Repeat("?, ", len(taskTypes)), ", ")

	query = fmt.Sprintf(`
UPDATE task
SET state = ?, enqueued_at = ?, active = ?
WHERE id = (
	SELECT task.id
	FROM task
	LEFT JOIN taskSchedule ON taskSchedule.task_id = task.id
	WHERE task.state = ?
		AND task.next_run_at <= ?
		AND task.task_type IN (%s)
	ORDER BY
		CASE
			WHEN taskSchedule.time_is_deadline = ? AND taskSchedule.is_imperative = ? THEN 0
			ELSE 1
		END,
		task.next_run_at ASC,
		task.id ASC
	LIMIT 1
)
AND state = ?
RETURNING id;`, placeholders)

	args = []any{
		string(TaskStateEnqueued),
		formatTaskSQLTime(enqueuedAt),
		true,
		string(TaskStatePending),
		formatTaskSQLTime(enqueuedAt),
	}

	for _, taskType := range taskTypes {
		args = append(args, taskType)
	}

	args = append(args, false, true, string(TaskStatePending))
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

	if c.sqlDB == nil {
		if _, err = c.updateTaskFieldsWithRetry(task.ID,
			gomysql.SetField(c.tasksDB.FieldByGoName("State"), task.State),
			gomysql.SetField(c.tasksDB.FieldByGoName("StartedAt"), task.StartedAt),
			gomysql.SetField(c.tasksDB.FieldByGoName("Active"), task.Active),
		); err != nil {
			return
		}

		if task._retryPolicy != nil {
			task._retryPolicy.LastTriedAt = startedAt
			err = c.updateRetryPolicyWithRetry(task._retryPolicy)
		}

		return
	}

	err = c.markTaskRunningWithRetry(task)
	return
}

func (c *Client) markTaskRunningWithRetry(task *task) (err error) {
	var lastTriedAt time.Time

	if task._retryPolicy == nil {
		_, err = c.execSQLWithRetry(
			"UPDATE task SET state = ?, started_at = ?, active = ? WHERE id = ?;",
			string(task.State),
			formatTaskSQLTime(*task.StartedAt),
			task.Active,
			task.ID,
		)
		return
	}

	lastTriedAt = *task.StartedAt
	err = c.withSQLTxRetry(func(tx *sql.Tx) (txErr error) {
		if _, txErr = tx.Exec(
			"UPDATE task SET state = ?, started_at = ?, active = ? WHERE id = ?;",
			string(task.State),
			formatTaskSQLTime(*task.StartedAt),
			task.Active,
			task.ID,
		); txErr != nil {
			return
		}

		_, txErr = tx.Exec(
			"UPDATE taskRetryPolicy SET last_tried_at = ? WHERE task_id = ?;",
			formatTaskSQLTime(lastTriedAt),
			task.ID,
		)
		return
	})

	if err == nil {
		task._retryPolicy.LastTriedAt = lastTriedAt
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

	if c.sqlDB == nil {
		if err = c.storeTaskResult(task, result); err != nil {
			return
		}

		_, err = c.updateTaskFieldsWithRetry(task.ID,
			gomysql.SetField(c.tasksDB.FieldByGoName("State"), task.State),
			gomysql.SetField(c.tasksDB.FieldByGoName("NextRunAt"), task.NextRunAt),
			gomysql.SetField(c.tasksDB.FieldByGoName("CompletedAt"), task.CompletedAt),
			gomysql.SetField(c.tasksDB.FieldByGoName("LastError"), task.LastError),
			gomysql.SetField(c.tasksDB.FieldByGoName("Active"), task.Active),
		)
		return
	}

	err = c.finishTaskWithRetry(task, result)
	return
}

func (c *Client) finishTaskWithRetry(task *task, result TaskConsumerResult) (err error) {
	var (
		errorMessage *string
		data         []byte
	)

	if result.Error != nil {
		var message string = result.Error.Error()
		errorMessage = &message
	} else if !result.Success {
		var message string = errConsumerFailed
		errorMessage = &message
	}

	if data, err = encodeTaskResultData(result.Data); err != nil {
		return
	}

	task._result = &taskResult{
		TaskID:       task.ID,
		Success:      result.Success,
		ErrorMessage: errorMessage,
		Data:         result.Data,
	}

	err = c.withSQLTxRetry(func(tx *sql.Tx) (txErr error) {
		if _, txErr = tx.Exec(
			"INSERT OR REPLACE INTO taskResult (task_id, success, error_message, data) VALUES (?, ?, ?, ?);",
			task.ID,
			result.Success,
			stringPtrSQLValue(errorMessage),
			data,
		); txErr != nil {
			return
		}

		_, txErr = tx.Exec(
			"UPDATE task SET state = ?, next_run_at = ?, completed_at = ?, last_error = ?, active = ? WHERE id = ?;",
			string(task.State),
			timePtrSQLValue(task.NextRunAt),
			timePtrSQLValue(task.CompletedAt),
			stringPtrSQLValue(task.LastError),
			task.Active,
			task.ID,
		)
		return
	})
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
	var retryCount int = task._retryPolicy.RetryCount + 1

	task.State = TaskStatePending
	task.NextRunAt = &nextRunAt
	task.Active = false
	task.StartedAt = nil
	task.CompletedAt = nil
	task.LastError = nil

	if c.sqlDB == nil {
		if _, err = c.updateTaskFieldsWithRetry(task.ID,
			gomysql.SetField(c.tasksDB.FieldByGoName("State"), task.State),
			gomysql.SetField(c.tasksDB.FieldByGoName("NextRunAt"), task.NextRunAt),
			gomysql.SetField(c.tasksDB.FieldByGoName("StartedAt"), task.StartedAt),
			gomysql.SetField(c.tasksDB.FieldByGoName("CompletedAt"), task.CompletedAt),
			gomysql.SetField(c.tasksDB.FieldByGoName("LastError"), task.LastError),
			gomysql.SetField(c.tasksDB.FieldByGoName("Active"), task.Active),
		); err != nil {
			return
		}

		task._retryPolicy.RetryCount = retryCount
		err = c.updateRetryPolicyWithRetry(task._retryPolicy)
		return
	}

	if err = c.retryTaskWithRetry(task, retryCount); err != nil {
		return
	}

	task._retryPolicy.RetryCount = retryCount
	return
}

func (c *Client) retryTaskWithRetry(task *task, retryCount int) (err error) {
	err = c.withSQLTxRetry(func(tx *sql.Tx) (txErr error) {
		if _, txErr = tx.Exec(
			"UPDATE task SET state = ?, next_run_at = ?, started_at = NULL, completed_at = NULL, last_error = NULL, active = ? WHERE id = ?;",
			string(task.State),
			timePtrSQLValue(task.NextRunAt),
			task.Active,
			task.ID,
		); txErr != nil {
			return
		}

		_, txErr = tx.Exec(
			"UPDATE taskRetryPolicy SET retry_count = ? WHERE task_id = ?;",
			retryCount,
			task.ID,
		)
		return
	})
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

func (c *Client) updateTaskFieldsWithRetry(id int, assignments ...gomysql.UpdateAssignment) (rows int64, err error) {
	var filter *gomysql.Filter = gomysql.NewFilter().
		KeyCmp(c.tasksDB.FieldByGoName("ID"), gomysql.OpEqual, id)

	for range c.lockRetryCount {
		if rows, err = c.tasksDB.UpdateWithFilter(filter, assignments...); err == nil {
			return
		} else if !isDatabaseLockedError(err) {
			return
		}

		time.Sleep(c.lockRetryDelay)
	}

	return
}

func (c *Client) execSQLWithRetry(query string, args ...any) (result sql.Result, err error) {
	for range c.lockRetryCount {
		if result, err = c.sqlDB.Exec(query, args...); err == nil {
			return
		}

		if !isDatabaseLockedError(err) {
			return
		}

		time.Sleep(c.lockRetryDelay)
	}

	return
}

func (c *Client) withSQLTxRetry(fn func(*sql.Tx) error) (err error) {
	var tx *sql.Tx

	for range c.lockRetryCount {
		if tx, err = c.sqlDB.Begin(); err != nil {
			if !isDatabaseLockedError(err) {
				return
			}

			time.Sleep(c.lockRetryDelay)
			continue
		}

		if err = fn(tx); err != nil {
			_ = tx.Rollback()
		} else if err = tx.Commit(); err == nil {
			return
		}

		if !isDatabaseLockedError(err) {
			return
		}

		_ = tx.Rollback()
		time.Sleep(c.lockRetryDelay)
	}

	return
}

func encodeTaskResultData(data []byte) (encoded []byte, err error) {
	var buf bytes.Buffer

	if err = gob.NewEncoder(&buf).Encode(data); err != nil {
		return
	}

	encoded = buf.Bytes()
	return
}

func formatTaskSQLTime(value time.Time) string {
	return value.UTC().Format(sqlTimeLayout)
}

func timePtrSQLValue(value *time.Time) any {
	if value == nil {
		return nil
	}

	return formatTaskSQLTime(*value)
}

func stringPtrSQLValue(value *string) any {
	if value == nil {
		return nil
	}

	return *value
}

func isDatabaseLockedError(err error) bool {
	if err == nil {
		return false
	}

	var message string = strings.ToLower(err.Error())
	return strings.Contains(message, "database is locked") ||
		strings.Contains(message, "database table is locked") ||
		strings.Contains(message, "sqlite_busy") ||
		strings.Contains(message, "sqlite_locked")
}
