package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/z46-dev/gasket"
)

func TestClientOptionValidation(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	client, err = gasket.NewClient(":memory:", gasket.PollInterval(0))
	assert.Error(t, err, "Expected zero poll interval to fail")
	assert.NotNil(t, client, "Expected client allocation to occur before option validation failure")

	client, err = gasket.NewClient(":memory:", gasket.DatabaseLockRetry(0, 1))
	assert.Error(t, err, "Expected zero database lock retry count to fail")
	assert.NotNil(t, client, "Expected client allocation to occur before option validation failure")

	client, err = gasket.NewClient(":memory:", gasket.DatabaseLockRetry(1, 0))
	assert.Error(t, err, "Expected zero database lock retry delay to fail")
	assert.NotNil(t, client, "Expected client allocation to occur before option validation failure")

	client, err = gasket.NewClient(":memory:", gasket.TaskRecoveryTimeout(-1))
	assert.Error(t, err, "Expected negative task recovery timeout to fail")
	assert.NotNil(t, client, "Expected client allocation to occur before option validation failure")
}
