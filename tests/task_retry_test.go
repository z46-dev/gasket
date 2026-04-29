package tests

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/z46-dev/gasket"
)

func TestTaskExecutionRetriesThenSucceeds(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = setup(t); err != nil {
		return
	}

	defer cleanup(t, client)

	var attempts atomic.Int32
	if err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		if attempts.Add(1) == 1 {
			result.Error = fmt.Errorf("temporary failure")
			return
		}

		result.Success = true
		result.Data = []byte("retried")
		return
	}); err != nil {
		t.Fatalf("Error registering consumer: %v", err)
		return
	}

	var (
		taskInfo *gasket.TaskInfo = client.MustNewTask("email", []byte("retry me"), gasket.RetryPolicy(1, 50*time.Millisecond))
		cancel   context.CancelFunc
		runErr   <-chan error
		result   gasket.TaskConsumerResult
	)

	_, cancel, runErr = runClient(client)
	defer cancel()

	result, err = waitForCompletion(t, taskInfo, 2*time.Second)
	assert.NoError(t, err, "Expected WaitForCompletion to succeed after retry")
	assert.True(t, result.Success, "Expected retried task to succeed")
	assert.Equal(t, []byte("retried"), result.Data, "Expected retried task result data to match consumer output")
	assert.Equal(t, int32(2), attempts.Load(), "Expected task to run twice")

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

	assert.Equal(t, gasket.TaskStateCompleted, taskInfo.State(), "Expected retried task state to be completed")
	assert.NotNil(t, taskInfo.RetryPolicyInfo(), "Expected retry policy info to be loaded")
	assert.Equal(t, 1, taskInfo.RetryPolicyInfo().RetryCount(), "Expected retry count to increment once")
	assert.NotZero(t, taskInfo.RetryPolicyInfo().LastTriedAt(), "Expected LastTriedAt to be set")
}

func TestTaskExecutionRetriesThenFails(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = setup(t); err != nil {
		return
	}

	defer cleanup(t, client)

	var attempts atomic.Int32
	if err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		attempts.Add(1)
		result.Error = fmt.Errorf("still failing")
		return
	}); err != nil {
		t.Fatalf("Error registering consumer: %v", err)
		return
	}

	var (
		taskInfo *gasket.TaskInfo = client.MustNewTask("email", []byte("fail me"), gasket.RetryPolicy(1, 50*time.Millisecond))
		cancel   context.CancelFunc
		runErr   <-chan error
		result   gasket.TaskConsumerResult
	)

	_, cancel, runErr = runClient(client)
	defer cancel()

	result, err = waitForCompletion(t, taskInfo, 2*time.Second)
	assert.NoError(t, err, "Expected WaitForCompletion to return the failed task result")
	assert.False(t, result.Success, "Expected retried task to fail")
	assert.Error(t, result.Error, "Expected failed retry result to include an error")
	assert.ErrorContains(t, result.Error, "still failing", "Expected failed retry result error to match consumer error")
	assert.Equal(t, int32(2), attempts.Load(), "Expected task to run twice before failing")

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

	assert.Equal(t, gasket.TaskStateFailed, taskInfo.State(), "Expected retried task state to be failed")
	assert.NotNil(t, taskInfo.RetryPolicyInfo(), "Expected retry policy info to be loaded")
	assert.Equal(t, 1, taskInfo.RetryPolicyInfo().RetryCount(), "Expected retry count to increment once before final failure")
	assert.NotZero(t, taskInfo.RetryPolicyInfo().LastTriedAt(), "Expected LastTriedAt to be set")
}
