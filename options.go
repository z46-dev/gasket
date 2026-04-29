package gasket

import (
	"fmt"
	"time"
)

func PollInterval(interval time.Duration) ClientOption {
	return func(client *Client) (err error) {
		if interval <= 0 {
			err = fmt.Errorf(errPollInterval)
			return
		}

		client.runPollInterval = interval
		return
	}
}

func DatabaseLockRetry(count int, delay time.Duration) ClientOption {
	return func(client *Client) (err error) {
		if count <= 0 {
			err = fmt.Errorf(errDatabaseLockedRetryCount)
			return
		}

		if delay <= 0 {
			err = fmt.Errorf(errDatabaseLockedRetryDelay)
			return
		}

		client.lockRetryCount = count
		client.lockRetryDelay = delay
		return
	}
}

func TaskRecoveryTimeout(timeout time.Duration) ClientOption {
	return func(client *Client) (err error) {
		if timeout < 0 {
			err = fmt.Errorf(errTaskRecoveryTimeout)
			return
		}

		client.taskRecoveryTimeout = timeout
		return
	}
}
