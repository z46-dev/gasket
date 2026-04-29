package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/z46-dev/gasket"
)

func TestTaskCreation(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = setup(t); err != nil {
		return
	}

	defer cleanup(t, client)

	var taskInfo *gasket.TaskInfo = client.MustNewTask("email", []byte("Send welcome email to somebody"))

	assert.Equal(t, taskInfo.ID(), 1, "Expected new task to have ID 1")
	assert.Equal(t, taskInfo.TaskType(), "email", "Expected task type to be 'email'")
	assert.Equal(t, taskInfo.Payload(), []byte("Send welcome email to somebody"), "Expected payload to match input")
	assert.Equal(t, gasket.TaskStatePending, taskInfo.State(), "Expected new task state to be pending")
	assert.NotZero(t, taskInfo.CreatedAt(), "Expected CreatedAt to be set")
	assert.NotNil(t, taskInfo.NextRunAt(), "Expected NextRunAt to be set for new task")
	assert.WithinDuration(t, taskInfo.CreatedAt(), *taskInfo.NextRunAt(), 5*time.Second, "Expected NextRunAt to match CreatedAt for new task")
	assert.Zero(t, taskInfo.EnqueuedAt(), "Expected EnqueuedAt to be zero for new task")
	assert.Zero(t, taskInfo.StartedAt(), "Expected StartedAt to be zero for new task")
	assert.Zero(t, taskInfo.CompletedAt(), "Expected CompletedAt to be zero for new task")
	assert.Zero(t, taskInfo.LastError(), "Expected LastError to be zero for new task")
	assert.Nil(t, taskInfo.SchedulingInfo(), "Expected SchedulingInfo to be new since we want it enqueued immediately")
	assert.Nil(t, taskInfo.RetryPolicyInfo(), "Expected RetryPolicyInfo to be nil since we didn't set a retry policy")

	taskInfo = client.MustNewTask("", []byte{}, gasket.RunBy(24*time.Hour))
	assert.Equal(t, taskInfo.ID(), 2, "Expected second task to have ID 2")
	assert.Equal(t, gasket.TaskStatePending, taskInfo.State(), "Expected scheduled task state to be pending")
	assert.NotNil(t, taskInfo.SchedulingInfo(), "Expected SchedulingInfo to be set when using RunBy option")
	assert.NotNil(t, taskInfo.NextRunAt(), "Expected NextRunAt to be set when using RunBy option")
	assert.WithinDuration(t, taskInfo.CreatedAt(), *taskInfo.NextRunAt(), 5*time.Second, "Expected RunBy task to remain eligible immediately")
	assert.WithinDuration(t, time.Now().Add(24*time.Hour), taskInfo.SchedulingInfo().ScheduledFor(), 5*time.Second, "Expected ScheduledFor to be approximately 24 hours in the future")

	// This should panic (since we're using must) and I'm lazy
	assert.Panics(t, func() {
		client.MustNewTask("email", []byte("Send welcome email to somebody"), gasket.RunBy(24*time.Hour), gasket.ScheduleIn(48*time.Hour))
	}, "Expected creating a task with multiple scheduling options to panic")
}

func TestTaskExecution(t *testing.T) {
	var (
		client          *gasket.Client
		err             error
		receivedID      int
		receivedPayload []byte
	)

	if client, err = setup(t); err != nil {
		return
	}

	defer cleanup(t, client)

	var done chan struct{} = make(chan struct{})
	if err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		receivedID = id
		receivedPayload = append([]byte{}, payload...)
		result.Success = true

		close(done)
		return
	}); err != nil {
		t.Fatalf("Error registering consumer: %v", err)
		return
	}

	var taskInfo *gasket.TaskInfo = client.MustNewTask("email", []byte("Send welcome email to somebody"))

	var ctx context.Context
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var runErr chan error = make(chan error, 1)
	go func() {
		runErr <- client.Run(ctx)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Expected task to complete within one second")
		return
	}

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

	assert.Equal(t, taskInfo.ID(), receivedID, "Expected consumer to receive matching task ID")
	assert.Equal(t, taskInfo.Payload(), receivedPayload, "Expected consumer to receive matching payload")
	assert.Equal(t, gasket.TaskStateCompleted, taskInfo.State(), "Expected task state to be completed")
	assert.Nil(t, taskInfo.NextRunAt(), "Expected completed task to have no next run time")
	assert.NotNil(t, taskInfo.EnqueuedAt(), "Expected EnqueuedAt to be set after execution")
	assert.NotNil(t, taskInfo.StartedAt(), "Expected StartedAt to be set after execution")
	assert.NotNil(t, taskInfo.CompletedAt(), "Expected CompletedAt to be set after execution")
	assert.Nil(t, taskInfo.LastError(), "Expected LastError to be nil after successful execution")
	assert.False(t, taskInfo.IsActive(), "Expected completed task to be inactive")
}

func TestTaskScheduledExecution(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = setup(t); err != nil {
		return
	}

	defer cleanup(t, client)

	var done chan struct{} = make(chan struct{})
	if err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		result.Success = true
		close(done)
		return
	}); err != nil {
		t.Fatalf("Error registering consumer: %v", err)
		return
	}

	var taskInfo *gasket.TaskInfo = client.MustNewTask("email", []byte("scheduled"), gasket.ScheduleIn(100*time.Millisecond))

	var ctx context.Context
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var runErr chan error = make(chan error, 1)
	go func() {
		runErr <- client.Run(ctx)
	}()

	select {
	case <-done:
		t.Fatal("Expected scheduled task not to run immediately")
		return
	case <-time.After(30 * time.Millisecond):
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Expected scheduled task to complete within one second")
		return
	}

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

	assert.Equal(t, gasket.TaskStateCompleted, taskInfo.State(), "Expected scheduled task to be completed")
	assert.NotNil(t, taskInfo.StartedAt(), "Expected scheduled task to start")
	assert.True(t, taskInfo.StartedAt().After(taskInfo.CreatedAt()) || taskInfo.StartedAt().Equal(taskInfo.CreatedAt()), "Expected scheduled task to start after creation")
}

func TestTaskExecutionFailure(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = setup(t); err != nil {
		return
	}

	defer cleanup(t, client)

	var expectedError error = fmt.Errorf("consumer failed")
	var done chan struct{} = make(chan struct{})

	err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		result.Error = expectedError
		close(done)
		return
	})
	if err != nil {
		t.Fatalf("Error registering consumer: %v", err)
		return
	}

	var taskInfo *gasket.TaskInfo = client.MustNewTask("email", []byte("failure"))

	var ctx context.Context
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var runErr chan error = make(chan error, 1)
	go func() {
		runErr <- client.Run(ctx)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Expected failing task to complete within one second")
		return
	}

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

	assert.Equal(t, gasket.TaskStateFailed, taskInfo.State(), "Expected task state to be failed")
	assert.NotNil(t, taskInfo.CompletedAt(), "Expected CompletedAt to be set after failure")
	assert.NotNil(t, taskInfo.LastError(), "Expected LastError to be set after failure")
	assert.Equal(t, expectedError.Error(), *taskInfo.LastError(), "Expected LastError to match consumer error")
	assert.False(t, taskInfo.IsActive(), "Expected failed task to be inactive")
}

func TestTaskExecutionIsClaimedOnceAcrossClients(t *testing.T) {
	var sharedDBFilePath string = filepath.Join(t.TempDir(), "atomic-claim.db")

	var (
		client *gasket.Client
		err    error
	)

	if client, err = setup(t, sharedDBFilePath); err != nil {
		return
	}

	var clients []*gasket.Client = []*gasket.Client{client}
	defer func() {
		for _, client := range clients {
			if client == nil {
				continue
			}

			if closeErr := client.Close(); closeErr != nil {
				t.Errorf("Error closing client: %v", closeErr)
			}
		}
	}()

	for range 3 {
		if client, err = gasket.NewClient(sharedDBFilePath); err != nil {
			t.Fatalf("Error creating additional client: %v", err)
			return
		}

		clients = append(clients, client)
	}

	var (
		done        chan struct{} = make(chan struct{})
		doneOnce    sync.Once
		executions  atomic.Int32
		consumerFun gasket.TaskConsumerFunc = func(id int, payload []byte) (result gasket.TaskConsumerResult) {
			executions.Add(1)
			doneOnce.Do(func() {
				close(done)
			})

			time.Sleep(100 * time.Millisecond)
			result.Success = true
			return
		}
	)

	for _, client = range clients {
		if err = client.RegisterConsumer("email", consumerFun); err != nil {
			t.Fatalf("Error registering consumer: %v", err)
			return
		}
	}

	var taskInfo *gasket.TaskInfo = clients[0].MustNewTask("email", []byte("claim-once"))

	var ctx context.Context
	var cancel context.CancelFunc
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var runErr chan error = make(chan error, len(clients))
	for _, client = range clients {
		go func(client *gasket.Client) {
			runErr <- client.Run(ctx)
		}(client)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Expected one client to claim and execute the task within one second")
		return
	}

	time.Sleep(150 * time.Millisecond)
	cancel()

	for range clients {
		select {
		case err = <-runErr:
			assert.NoError(t, err, "Expected Run to stop without error")
		case <-time.After(time.Second):
			t.Fatal("Expected Run to stop within one second")
			return
		}
	}

	if err = taskInfo.Refresh(); err != nil {
		t.Fatalf("Error refreshing task info: %v", err)
		return
	}

	assert.Equal(t, int32(1), executions.Load(), "Expected task to be executed exactly once across clients")
	assert.Equal(t, gasket.TaskStateCompleted, taskInfo.State(), "Expected shared task to be completed")
	assert.NotNil(t, taskInfo.EnqueuedAt(), "Expected EnqueuedAt to be set after claim")
	assert.NotNil(t, taskInfo.StartedAt(), "Expected StartedAt to be set after claim")
	assert.NotNil(t, taskInfo.CompletedAt(), "Expected CompletedAt to be set after completion")
}
