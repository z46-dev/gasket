package tests

import (
	"testing"

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