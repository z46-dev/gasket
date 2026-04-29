package tests

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/z46-dev/gasket"
	_ "modernc.org/sqlite"
)

func TestInterruptedRunningTaskRetriesAndCompletes(t *testing.T) {
	var sharedDBFilePath string = filepath.Join(t.TempDir(), "recovery-retry.db")

	var (
		client *gasket.Client
		err    error
	)

	if client, err = setupWithClientOptions(
		t,
		sharedDBFilePath,
		gasket.PollInterval(5*time.Millisecond),
		gasket.TaskRecoveryTimeout(50*time.Millisecond),
	); err != nil {
		return
	}

	defer cleanup(t, client)

	var attempts atomic.Int32
	if err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		attempts.Add(1)
		result.Success = true
		result.Data = []byte("recovered")
		return
	}); err != nil {
		t.Fatalf("Error registering consumer: %v", err)
		return
	}

	var (
		taskInfo *gasket.TaskInfo = client.MustNewTask("email", []byte("recover me"), gasket.RetryPolicy(1, 20*time.Millisecond))
		cancel   context.CancelFunc
		runErr   <-chan error
		result   gasket.TaskConsumerResult
	)

	if err = markTaskRunningInDB(sharedDBFilePath, taskInfo.ID(), time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("Error marking task as running in DB: %v", err)
		return
	}

	_, cancel, runErr = runClient(client)
	defer cancel()

	result, err = waitForCompletion(t, taskInfo, 2*time.Second)
	assert.NoError(t, err, "Expected recovered task to complete successfully")
	assert.True(t, result.Success, "Expected recovered task to succeed after retry")
	assert.Equal(t, []byte("recovered"), result.Data, "Expected recovered task result data to match consumer output")
	assert.Equal(t, int32(1), attempts.Load(), "Expected recovered task consumer to run once after recovery")

	cancel()

	select {
	case err = <-runErr:
		assert.NoError(t, err, "Expected Run to stop without error")
	case <-time.After(time.Second):
		t.Fatal("Expected Run to stop within one second")
		return
	}

	if err = taskInfo.Refresh(); err != nil {
		t.Fatalf("Error refreshing task info: %v", err)
		return
	}

	assert.Equal(t, gasket.TaskStateCompleted, taskInfo.State(), "Expected recovered task state to be completed")
	assert.NotNil(t, taskInfo.RetryPolicyInfo(), "Expected retry policy info to be loaded")
	assert.Equal(t, 1, taskInfo.RetryPolicyInfo().RetryCount(), "Expected recovery to consume one retry")
	assert.NotZero(t, taskInfo.RetryPolicyInfo().LastTriedAt(), "Expected recovered retry attempt to record LastTriedAt")
}

func TestInterruptedRunningTaskFailsWithoutRetry(t *testing.T) {
	var sharedDBFilePath string = filepath.Join(t.TempDir(), "recovery-fail.db")

	var (
		client *gasket.Client
		err    error
	)

	if client, err = setupWithClientOptions(
		t,
		sharedDBFilePath,
		gasket.PollInterval(5*time.Millisecond),
		gasket.TaskRecoveryTimeout(50*time.Millisecond),
	); err != nil {
		return
	}

	defer cleanup(t, client)

	var attempts atomic.Int32
	if err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		attempts.Add(1)
		result.Success = true
		return
	}); err != nil {
		t.Fatalf("Error registering consumer: %v", err)
		return
	}

	var (
		taskInfo *gasket.TaskInfo = client.MustNewTask("email", []byte("recover me"))
		cancel   context.CancelFunc
		runErr   <-chan error
		result   gasket.TaskConsumerResult
	)

	if err = markTaskRunningInDB(sharedDBFilePath, taskInfo.ID(), time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("Error marking task as running in DB: %v", err)
		return
	}

	_, cancel, runErr = runClient(client)
	defer cancel()

	result, err = waitForCompletion(t, taskInfo, 2*time.Second)
	assert.NoError(t, err, "Expected interrupted task to finish with a failure result")
	assert.False(t, result.Success, "Expected interrupted task to fail without retry policy")
	assert.Error(t, result.Error, "Expected interrupted task failure to include an error")
	assert.ErrorContains(t, result.Error, "interrupted", "Expected interrupted task error to mention interruption")
	assert.Equal(t, int32(0), attempts.Load(), "Expected interrupted task not to execute without retry recovery")

	cancel()

	select {
	case err = <-runErr:
		assert.NoError(t, err, "Expected Run to stop without error")
	case <-time.After(time.Second):
		t.Fatal("Expected Run to stop within one second")
		return
	}

	if err = taskInfo.Refresh(); err != nil {
		t.Fatalf("Error refreshing task info: %v", err)
		return
	}

	assert.Equal(t, gasket.TaskStateFailed, taskInfo.State(), "Expected interrupted task state to be failed")
	assert.NotNil(t, taskInfo.LastError(), "Expected interrupted task LastError to be set")
	assert.Contains(t, *taskInfo.LastError(), "interrupted", "Expected interrupted task LastError to mention interruption")
}

func markTaskRunningInDB(filePath string, taskID int, startedAt time.Time) (err error) {
	var db *sql.DB
	if db, err = sql.Open("sqlite", filePath); err != nil {
		return
	}

	defer db.Close()

	_, err = db.Exec(
		"UPDATE task SET state = ?, active = ?, enqueued_at = ?, started_at = ?, completed_at = NULL, last_error = NULL WHERE id = ?;",
		string(gasket.TaskStateRunning),
		true,
		startedAt,
		startedAt,
		taskID,
	)

	return
}
