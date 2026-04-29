package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/z46-dev/gasket"
)

func TestAPICreateDestroy(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = setup(t); err != nil {
		return
	}

	defer cleanup(t, client)
}

func TestAPICreateMultipleAndDestroy(t *testing.T) {
	var (
		client1 *gasket.Client
		client2 *gasket.Client
		err     error
	)

	if client1, err = setup(t, "client1.db"); err != nil {
		return
	}

	if client2, err = setup(t, "client2.db"); err != nil {
		return
	}

	defer cleanup(t, client1)
	defer cleanup(t, client2)
}

func TestAPIRegisterConsumer(t *testing.T) {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = setup(t); err != nil {
		return
	}

	defer cleanup(t, client)

	err = client.RegisterConsumer("", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		return
	})

	assert.Error(t, err, "Expected empty task type to fail")

	err = client.RegisterConsumer("email", nil)
	assert.Error(t, err, "Expected nil consumer to fail")

	err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		result.Success = true
		return
	})

	assert.NoError(t, err, "Expected consumer registration to succeed")

	err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		result.Success = true
		return
	})

	assert.Error(t, err, "Expected duplicate consumer registration to fail")
}
