package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/z46-dev/gasket"
)

func TestTaskWaitForEnqueueAndCompletion(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = setup(t); err != nil {
		return
	}

	defer cleanup(t, client)

	if err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		result.Success = true
		result.Data = []byte("completed")
		return
	}); err != nil {
		t.Fatalf("Error registering consumer: %v", err)
		return
	}

	var (
		taskInfo *gasket.TaskInfo = client.MustNewTask("email", []byte("Send welcome email to somebody"))
		cancel   context.CancelFunc
		runErr   <-chan error
		result   gasket.TaskConsumerResult
	)

	_, cancel, runErr = runClient(client)
	defer cancel()

	err = waitForEnqueue(t, taskInfo, time.Second)
	assert.NoError(t, err, "Expected WaitForEnqueue to succeed")

	result, err = waitForCompletion(t, taskInfo, time.Second)
	assert.NoError(t, err, "Expected WaitForCompletion to succeed")
	assert.True(t, result.Success, "Expected completion result to be successful")
	assert.Equal(t, []byte("completed"), result.Data, "Expected completion result data to match consumer output")
	assert.NoError(t, result.Error, "Expected completion result error to be nil")

	cancel()

	select {
	case err = <-runErr:
		assert.NoError(t, err, "Expected Run to stop without error")
	case <-time.After(time.Second):
		t.Fatal("Expected Run to stop within one second")
		return
	}
}

func TestTaskCancel(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = setup(t); err != nil {
		return
	}

	defer cleanup(t, client)

	var executions atomic.Int32
	if err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		executions.Add(1)
		result.Success = true
		return
	}); err != nil {
		t.Fatalf("Error registering consumer: %v", err)
		return
	}

	var (
		taskInfo *gasket.TaskInfo = client.MustNewTask("email", []byte("cancel me"), gasket.ScheduleIn(time.Hour))
		cancel   context.CancelFunc
		runErr   <-chan error
	)

	err = taskInfo.Cancel()
	assert.NoError(t, err, "Expected task cancellation to succeed")

	err = waitForEnqueue(t, taskInfo, time.Second)
	assert.Error(t, err, "Expected WaitForEnqueue to fail after cancellation")
	assert.ErrorContains(t, err, "cancelled", "Expected enqueue wait failure to mention cancellation")

	_, err = waitForCompletion(t, taskInfo, time.Second)
	assert.Error(t, err, "Expected WaitForCompletion to fail after cancellation")
	assert.ErrorContains(t, err, "cancelled", "Expected completion wait failure to mention cancellation")

	_, cancel, runErr = runClient(client)
	defer cancel()

	time.Sleep(50 * time.Millisecond)
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

	assert.Equal(t, gasket.TaskStateCancelled, taskInfo.State(), "Expected cancelled task state to be cancelled")
	assert.Nil(t, taskInfo.NextRunAt(), "Expected cancelled task to have no next run time")
	assert.NotNil(t, taskInfo.CompletedAt(), "Expected cancelled task to have a completion time")
	assert.False(t, taskInfo.IsActive(), "Expected cancelled task to be inactive")
	assert.Equal(t, int32(0), executions.Load(), "Expected cancelled task not to be executed")

	err = taskInfo.Cancel()
	assert.Error(t, err, "Expected cancelling an already cancelled task to fail")
}
