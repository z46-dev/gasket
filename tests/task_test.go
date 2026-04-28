package tests

import (
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

	var taskInfo *gasket.TaskInfo = gasket.MustNewTask("email", []byte("Send welcome email to somebody")).GetInfo()

	assert.Equal(t, taskInfo.ID, 0, "Expected new task to have ID 0")
	assert.Equal(t, taskInfo.TaskType, "email", "Expected task type to be 'email'")
	assert.Equal(t, taskInfo.Payload, []byte("Send welcome email to somebody"), "Expected payload to match input")
	assert.NotZero(t, taskInfo.CreatedAt, "Expected CreatedAt to be set")
	assert.Zero(t, taskInfo.EnqueuedAt, "Expected EnqueuedAt to be zero for new task")
	assert.Nil(t, taskInfo.SchedulingInfo, "Expected SchedulingInfo to be new since we want it enqueued immediately")
	assert.Nil(t, taskInfo.RetryPolicyInfo, "Expected RetryPolicyInfo to be nil since we didn't set a retry policy")

	taskInfo = gasket.MustNewTask("", []byte{}, gasket.RunBy(24*time.Hour)).GetInfo()
	assert.NotNil(t, taskInfo.SchedulingInfo, "Expected SchedulingInfo to be set when using RunBy option")
	assert.WithinDuration(t, time.Now().Add(24*time.Hour), taskInfo.SchedulingInfo.ScheduledFor, 5*time.Second, "Expected ScheduledFor to be approximately 24 hours in the future")

	// This should panic (since we're using must) and I'm lazy
	assert.Panics(t, func() {
		gasket.MustNewTask("email", []byte("Send welcome email to somebody"), gasket.RunBy(24*time.Hour), gasket.ScheduleIn(48*time.Hour))
	}, "Expected creating a task with multiple scheduling options to panic")
}
