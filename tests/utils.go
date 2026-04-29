package tests

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/z46-dev/gasket"
)

const DEFAULT_TEST_DB_FILE_PATH = ":memory:" //"test.db"

func setup(t *testing.T, pathOverride ...string) (client *gasket.Client, err error) {
	if len(pathOverride) > 0 {
		t.Logf("Using overridden test DB file path: %s", pathOverride[0])
	} else {
		pathOverride = append(pathOverride, DEFAULT_TEST_DB_FILE_PATH)
		t.Logf("Using default test DB file path: %s", pathOverride[0])
	}

	if err = rmTestDBFile(pathOverride[0]); err != nil {
		t.Fatalf("Error removing existing test DB file: %v", err)
		return
	}

	if client, err = gasket.NewClient(pathOverride[0]); err != nil {
		t.Fatalf("Error creating client: %v", err)
		return
	}

	t.Logf("Test setup complete, client created successfully")
	return
}

func setupWithClientOptions(t *testing.T, path string, opts ...gasket.ClientOption) (client *gasket.Client, err error) {
	t.Logf("Using overridden test DB file path: %s", path)

	if err = rmTestDBFile(path); err != nil {
		t.Fatalf("Error removing existing test DB file: %v", err)
		return
	}

	if client, err = gasket.NewClient(path, opts...); err != nil {
		t.Fatalf("Error creating client: %v", err)
		return
	}

	t.Logf("Test setup complete, client created successfully")
	return
}

func cleanup(t *testing.T, client *gasket.Client) (err error) {
	t.Logf("Starting test cleanup")

	if client != nil {
		if err = client.Close(); err != nil {
			t.Errorf("Error closing client: %v", err)
			return
		}
	}

	if err = rmTestDBFile(client.GetActiveDatabaseFilePath()); err != nil {
		t.Errorf("Error removing test DB file: %v", err)
		return
	}

	t.Logf("Test cleanup complete, client closed and DB file removed successfully")
	return
}

func rmTestDBFile(fileName string) (err error) {
	if fileName == ":memory:" {
		return
	}

	if err = os.Remove(fileName); err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
	}

	return
}

func runClient(client *gasket.Client) (ctx context.Context, cancel context.CancelFunc, runErr chan error) {
	ctx, cancel = context.WithCancel(context.Background())
	runErr = make(chan error, 1)

	go func() {
		runErr <- client.Run(ctx)
	}()

	return
}

func waitForEnqueue(t testing.TB, taskInfo *gasket.TaskInfo, timeout time.Duration) (err error) {
	t.Helper()

	type waitResult struct {
		err error
	}

	var resultChan chan waitResult = make(chan waitResult, 1)
	go func() {
		resultChan <- waitResult{
			err: taskInfo.WaitForEnqueue(),
		}
	}()

	select {
	case result := <-resultChan:
		err = result.err
	case <-time.After(timeout):
		t.Fatal("Timed out waiting for task enqueue")
	}

	return
}

func waitForCompletion(t testing.TB, taskInfo *gasket.TaskInfo, timeout time.Duration) (result gasket.TaskConsumerResult, err error) {
	t.Helper()

	type waitResult struct {
		result gasket.TaskConsumerResult
		err    error
	}

	var resultChan chan waitResult = make(chan waitResult, 1)
	go func() {
		result, err := taskInfo.WaitForCompletion()
		resultChan <- waitResult{
			result: result,
			err:    err,
		}
	}()

	select {
	case waitResult := <-resultChan:
		result = waitResult.result
		err = waitResult.err
	case <-time.After(timeout):
		t.Fatal("Timed out waiting for task completion")
	}

	return
}
