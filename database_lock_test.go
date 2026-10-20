package gasket

import (
	"path/filepath"
	"testing"
	"time"
)

func TestTaskOperationsRetryDatabaseLock(t *testing.T) {
	for _, operation := range []string{"refresh", "create"} {
		t.Run(operation, func(t *testing.T) {
			client, err := NewClient(filepath.Join(t.TempDir(), "tasks.db"), DatabaseLockRetry(100, 5*time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			info, err := client.NewTask("email", []byte("hello"))
			if err != nil {
				t.Fatal(err)
			}

			// An exclusive transaction on the SQL connection blocks the separate ORM connection.
			if _, err = client.sqlDB.Exec("BEGIN EXCLUSIVE"); err != nil {
				t.Fatal(err)
			}
			defer client.sqlDB.Exec("ROLLBACK")
			done := make(chan error, 1)
			go func() {
				if operation == "refresh" {
					done <- info.Refresh()
					return
				}
				_, createErr := client.NewTask("email", []byte("retry"), ScheduleIn(time.Second), RetryPolicy(3, time.Second))
				done <- createErr
			}()
			select {
			case err := <-done:
				t.Fatalf("operation returned before lock was released: %v", err)
			case <-time.After(40 * time.Millisecond):
			}
			if _, err = client.sqlDB.Exec("COMMIT"); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("operation did not finish after lock was released")
			}
		})
	}
}

func TestTaskRefreshReturnsErrorAfterLockRetries(t *testing.T) {
	client, err := NewClient(filepath.Join(t.TempDir(), "tasks.db"), DatabaseLockRetry(2, time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	info, err := client.NewTask("email", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.sqlDB.Exec("BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	defer client.sqlDB.Exec("ROLLBACK")
	if err = info.Refresh(); !isDatabaseLockedError(err) {
		t.Fatalf("expected lock error after retries, got %v", err)
	}
}
