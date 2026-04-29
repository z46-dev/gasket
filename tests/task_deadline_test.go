package tests

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/z46-dev/gasket"
)

func TestTaskRunByExecutesBeforeDeadline(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = setup(t); err != nil {
		return
	}

	defer cleanup(t, client)

	var executions atomic.Int32
	err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		executions.Add(1)
		result.Success = true
		result.Data = []byte("before deadline")
		return
	})

	if err != nil {
		t.Fatalf("Error registering consumer: %v", err)
		return
	}

	var taskInfo *gasket.TaskInfo = client.MustNewTask("email", []byte("run soon"), gasket.RunBy(2*time.Second))

	_, cancel, runErr := runClient(client)
	defer cancel()

	result, err := waitForCompletion(t, taskInfo, time.Second)
	assert.NoError(t, err, "Expected RunBy task to complete before its deadline")
	assert.True(t, result.Success, "Expected RunBy task to succeed before its deadline")
	assert.Equal(t, []byte("before deadline"), result.Data, "Expected RunBy task result data to match consumer output")
	assert.Equal(t, int32(1), executions.Load(), "Expected RunBy task to execute once")

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

	assert.Equal(t, gasket.TaskStateCompleted, taskInfo.State(), "Expected RunBy task state to be completed")
	assert.NotNil(t, taskInfo.StartedAt(), "Expected RunBy task to have started")
	assert.False(t, taskInfo.StartedAt().After(taskInfo.SchedulingInfo().ScheduledFor()), "Expected RunBy task to start before its deadline")
}

func TestTaskDeadlineFailure(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = setup(t); err != nil {
		return
	}

	defer cleanup(t, client)

	var executions atomic.Int32
	err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		executions.Add(1)
		result.Success = true
		return
	})

	if err != nil {
		t.Fatalf("Error registering consumer: %v", err)
		return
	}

	var taskInfo *gasket.TaskInfo = client.MustNewTask("email", []byte("too late"), gasket.RunBy(-50*time.Millisecond), gasket.RetryPolicy(2, 50*time.Millisecond))

	_, cancel, runErr := runClient(client)
	defer cancel()

	result, err := waitForCompletion(t, taskInfo, time.Second)
	assert.NoError(t, err, "Expected WaitForCompletion to return the deadline failure result")
	assert.False(t, result.Success, "Expected deadline-expired task to fail")
	assert.Error(t, result.Error, "Expected deadline-expired task to include an error")
	assert.ErrorContains(t, result.Error, "deadline", "Expected deadline-expired task error to mention the deadline")

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

	assert.Equal(t, int32(0), executions.Load(), "Expected deadline-expired task not to be executed")
	assert.Equal(t, gasket.TaskStateFailed, taskInfo.State(), "Expected deadline-expired task state to be failed")
	assert.NotNil(t, taskInfo.RetryPolicyInfo(), "Expected retry policy info to be loaded")
	assert.Equal(t, 0, taskInfo.RetryPolicyInfo().RetryCount(), "Expected deadline-expired task not to schedule retries")
	assert.Zero(t, taskInfo.RetryPolicyInfo().LastTriedAt(), "Expected deadline-expired task not to record an attempt time")
}
