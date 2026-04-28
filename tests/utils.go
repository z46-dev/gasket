package tests

import (
	"os"
	"testing"

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
