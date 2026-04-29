package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/z46-dev/gasket"
)

func TestTaskRunInExecution(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = setup(t); err != nil {
		return
	}

	defer cleanup(t, client)

	var done chan struct{} = make(chan struct{})
	err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		result.Success = true
		result.Data = []byte("scheduled critically")
		close(done)
		return
	})
	if err != nil {
		t.Fatalf("Error registering consumer: %v", err)
		return
	}

	var taskInfo *gasket.TaskInfo = client.MustNewTask("email", []byte("run in a bit"), gasket.RunIn(100*time.Millisecond))

	assert.NotNil(t, taskInfo.NextRunAt(), "Expected RunIn task NextRunAt to be stored")
	assert.NotNil(t, taskInfo.SchedulingInfo(), "Expected RunIn task scheduling info to be stored")
	assert.WithinDuration(t, taskInfo.CreatedAt().Add(100*time.Millisecond), *taskInfo.NextRunAt(), 20*time.Millisecond, "Expected RunIn task NextRunAt to be set from the requested duration")
	assert.WithinDuration(t, *taskInfo.NextRunAt(), taskInfo.SchedulingInfo().ScheduledFor(), 5*time.Millisecond, "Expected RunIn task ScheduledFor to match NextRunAt")
	assert.True(t, taskInfo.SchedulingInfo().IsImperative(), "Expected RunIn task scheduling info to be imperative")

	_, cancel, runErr := runClient(client)
	defer cancel()

	select {
	case <-done:
		t.Fatal("Expected RunIn task not to run immediately")
		return
	case <-time.After(30 * time.Millisecond):
	}

	result, err := waitForCompletion(t, taskInfo, time.Second)
	assert.NoError(t, err, "Expected RunIn task to complete successfully")
	assert.True(t, result.Success, "Expected RunIn task to succeed")
	assert.Equal(t, []byte("scheduled critically"), result.Data, "Expected RunIn task result data to match consumer output")

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

	assert.Equal(t, gasket.TaskStateCompleted, taskInfo.State(), "Expected RunIn task to be completed")
	assert.NotNil(t, taskInfo.StartedAt(), "Expected RunIn task to have started")
	assert.False(t, taskInfo.StartedAt().Before(taskInfo.SchedulingInfo().ScheduledFor()), "Expected RunIn task not to start before its scheduled time")
}

func TestTaskRunInPrioritizedOverScheduleIn(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = setupWithClientOptions(t, DEFAULT_TEST_DB_FILE_PATH, gasket.PollInterval(100*time.Millisecond)); err != nil {
		return
	}

	defer cleanup(t, client)

	var (
		order []string
		mu    sync.Mutex
	)

	err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		mu.Lock()
		order = append(order, string(payload))
		mu.Unlock()
		result.Success = true
		return
	})
	if err != nil {
		t.Fatalf("Error registering consumer: %v", err)
		return
	}

	var (
		scheduledTask *gasket.TaskInfo = client.MustNewTask("email", []byte("scheduled"), gasket.ScheduleIn(10*time.Millisecond))
		runInTask     *gasket.TaskInfo = client.MustNewTask("email", []byte("run in"), gasket.RunIn(10*time.Millisecond))
	)

	_, cancel, runErr := runClient(client)
	defer cancel()

	_, err = waitForCompletion(t, scheduledTask, 2*time.Second)
	assert.NoError(t, err, "Expected scheduled task to complete successfully")

	_, err = waitForCompletion(t, runInTask, 2*time.Second)
	assert.NoError(t, err, "Expected RunIn task to complete successfully")

	cancel()

	select {
	case err = <-runErr:
		assert.NoError(t, err, "Expected Run to stop without error")
	case <-time.After(time.Second):
		t.Fatal("Expected Run to stop within one second")
		return
	}

	mu.Lock()
	defer mu.Unlock()

	assert.Len(t, order, 2, "Expected two task executions to be recorded")
	assert.Equal(t, "run in", order[0], "Expected RunIn task to execute before ScheduleIn when both are due")
	assert.Equal(t, "scheduled", order[1], "Expected ScheduleIn task to execute second when both are due")
}
