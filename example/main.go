package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/z46-dev/gasket"
)

func main() {
	var (
		client *gasket.Client
		err    error
	)

	if client, err = gasket.NewClient(
		"tasks.db",
		gasket.PollInterval(25*time.Millisecond),
		gasket.DatabaseLockRetry(20, 5*time.Millisecond),
		gasket.TaskRecoveryTimeout(30*time.Second),
	); err != nil {
		panic(err)
	}

	defer client.Close()

	if err = client.RegisterConsumer("email", func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		fmt.Printf("running task %d: %s\n", id, string(payload))
		result.Success = true
		result.Data = []byte("sent")
		return
	}); err != nil {
		panic(err)
	}

	var (
		ctx    context.Context
		cancel context.CancelFunc
	)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	go func() {
		var runErr error
		if runErr = client.Run(ctx); runErr != nil {
			panic(runErr)
		}
	}()

	var wg sync.WaitGroup

	var waitCompletion = func(taskInfo *gasket.TaskInfo) {
		var (
			now    time.Time = time.Now()
			result gasket.TaskConsumerResult
			err    error
		)

		if result, err = taskInfo.WaitForCompletion(); err != nil {
			fmt.Printf("task %d wait error: %v\n", taskInfo.ID(), err)
		} else {
			fmt.Printf("task %d completed in %v: success=%v data=%s err=%v\n", taskInfo.ID(), time.Since(now), result.Success, string(result.Data), result.Error)
		}
	}

	wg.Go(func() {
		var taskInfo *gasket.TaskInfo = client.MustNewTask(
			"email",
			[]byte("Send normal delayed email"),
			gasket.ScheduleIn(5*time.Second),
			gasket.RetryPolicy(3, 5*time.Second),
		)

		fmt.Printf("created scheduled(+5s) task %d\n", taskInfo.ID())
		waitCompletion(taskInfo)
	})

	wg.Go(func() {
		var taskInfo *gasket.TaskInfo = client.MustNewTask(
			"email",
			[]byte("Send welcome email ASAP"),
			gasket.RunBy(20*time.Second),
			gasket.RetryPolicy(3, 5*time.Second),
		)

		fmt.Printf("created RunBy(+20s) task %d\n", taskInfo.ID())
		waitCompletion(taskInfo)
	})

	wg.Go(func() {
		var taskInfo *gasket.TaskInfo = client.MustNewTask(
			"email",
			[]byte("Send critical delayed email"),
			gasket.RunIn(5*time.Second),
			gasket.RetryPolicy(3, 5*time.Second),
		)

		fmt.Printf("created RunIn(+5s) task %d\n", taskInfo.ID())
		waitCompletion(taskInfo)
	})

	wg.Wait()
}
