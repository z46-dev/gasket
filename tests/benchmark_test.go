package tests

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"html"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/z46-dev/gasket"
)

var benchmarkGraphDir = flag.String("gasket.bench.graph-dir", "", "write benchmark CSV and SVG graphs to this directory")

type benchmarkStorage string

const (
	benchmarkStorageMemory benchmarkStorage = "memory"
	benchmarkStorageDisk   benchmarkStorage = "disk"
)

type benchmarkWorkload struct {
	name          string
	payloadBytes  int
	sleepEvery    int
	sleepDuration time.Duration
	runInEvery    int
	retryEvery    int
}

type benchmarkScenario struct {
	storage  benchmarkStorage
	clients  int
	workload benchmarkWorkload
}

type benchmarkGraphRow struct {
	name         string
	storage      benchmarkStorage
	workload     string
	clients      int
	tasks        int
	elapsed      time.Duration
	tasksPerSec  float64
	nsPerTask    float64
	payloadBytes int
}

var (
	benchmarkGraphMu   sync.Mutex
	benchmarkGraphRows []benchmarkGraphRow
)

func BenchmarkGasketWorkloads(b *testing.B) {
	var workloads []benchmarkWorkload = []benchmarkWorkload{
		{
			name:         "instant-small",
			payloadBytes: 128,
		},
		{
			name:         "instant-large",
			payloadBytes: 64 * 1024,
		},
		{
			name:          "mixed-short",
			payloadBytes:  1024,
			sleepEvery:    3,
			sleepDuration: time.Millisecond,
			runInEvery:    5,
			retryEvery:    7,
		},
		{
			name:          "mixed-long",
			payloadBytes:  1024,
			sleepEvery:    2,
			sleepDuration: 5 * time.Millisecond,
			runInEvery:    4,
			retryEvery:    6,
		},
	}

	var scenarios []benchmarkScenario
	for _, workload := range workloads {
		scenarios = append(scenarios, benchmarkScenario{
			storage:  benchmarkStorageMemory,
			clients:  1,
			workload: workload,
		})

		for _, clients := range []int{1, 2, 4} {
			scenarios = append(scenarios, benchmarkScenario{
				storage:  benchmarkStorageDisk,
				clients:  clients,
				workload: workload,
			})
		}
	}

	for _, scenario := range scenarios {
		scenario := scenario
		b.Run(fmt.Sprintf("%s/clients=%d/%s", scenario.storage, scenario.clients, scenario.workload.name), func(b *testing.B) {
			runGasketBenchmarkScenario(b, scenario)
		})
	}
}

func runGasketBenchmarkScenario(b *testing.B, scenario benchmarkScenario) {
	if b.N == 0 {
		return
	}

	var (
		dbPath  string
		clients []*gasket.Client
		err     error
	)

	switch scenario.storage {
	case benchmarkStorageMemory:
		dbPath = fmt.Sprintf("file:gasket-bench-%s-%d?mode=memory&cache=shared", strings.ReplaceAll(b.Name(), "/", "-"), time.Now().UnixNano())
	case benchmarkStorageDisk:
		dbPath = filepath.Join(b.TempDir(), "bench.db")
	default:
		b.Fatalf("unknown benchmark storage mode %q", scenario.storage)
	}

	for clientIndex := 0; clientIndex < scenario.clients; clientIndex++ {
		var client *gasket.Client
		if client, err = gasket.NewClient(
			dbPath,
			gasket.PollInterval(time.Millisecond),
			gasket.DatabaseLockRetry(100, time.Millisecond),
		); err != nil {
			closeBenchmarkClients(clients)
			b.Fatalf("create benchmark client: %v", err)
		}

		clients = append(clients, client)
	}

	defer closeBenchmarkClients(clients)

	var (
		attempts        sync.Map
		consumerCalls   atomic.Int64
		terminalTasks   atomic.Int64
		expectedCalls   int
		expectedPayload = makeBenchmarkPayload(scenario.workload.payloadBytes)
	)

	for _, client := range clients {
		registerBenchmarkConsumers(b, client, scenario.workload, &attempts, &consumerCalls, &terminalTasks)
	}

	b.ReportAllocs()
	b.ResetTimer()
	startedAt := time.Now()

	var tasks []*gasket.TaskInfo
	for taskIndex := 0; taskIndex < b.N; taskIndex++ {
		taskType := benchmarkTaskType(scenario.workload, taskIndex)
		opts := benchmarkTaskOptions(scenario.workload, taskIndex)
		taskInfo, err := clients[0].NewTask(taskType, expectedPayload, opts...)
		if err != nil {
			b.Fatalf("create benchmark task %d: %v", taskIndex, err)
		}

		tasks = append(tasks, taskInfo)
		expectedCalls += benchmarkExpectedConsumerCalls(scenario.workload, taskIndex)
	}

	var (
		ctx     context.Context
		cancel  context.CancelFunc
		runErrs []chan error
	)

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	for _, client := range clients {
		runErr := make(chan error, 1)
		runErrs = append(runErrs, runErr)
		go func(client *gasket.Client, runErr chan error) {
			runErr <- client.Run(ctx)
		}(client, runErr)
	}

	waitForBenchmarkCompletions(b, tasks, 30*time.Second)

	elapsed := time.Since(startedAt)
	b.StopTimer()

	cancel()
	for _, runErr := range runErrs {
		select {
		case err = <-runErr:
			if err != nil {
				b.Fatalf("benchmark client run failed: %v", err)
			}
		case <-time.After(5 * time.Second):
			b.Fatal("timed out waiting for benchmark client to stop")
		}
	}

	if got := int(consumerCalls.Load()); got != expectedCalls {
		b.Fatalf("expected %d consumer calls, got %d", expectedCalls, got)
	}

	if got := int(terminalTasks.Load()); got != b.N {
		b.Fatalf("expected %d terminal tasks, got %d", b.N, got)
	}

	tasksPerSecond := float64(b.N) / elapsed.Seconds()
	nsPerTask := float64(elapsed.Nanoseconds()) / float64(b.N)

	b.ReportMetric(tasksPerSecond, "tasks/s")
	b.ReportMetric(nsPerTask, "ns/task")
	b.ReportMetric(float64(scenario.clients), "clients")
	b.ReportMetric(float64(scenario.workload.payloadBytes), "payload_bytes")

	recordBenchmarkGraphRow(b, benchmarkGraphRow{
		name:         b.Name(),
		storage:      scenario.storage,
		workload:     scenario.workload.name,
		clients:      scenario.clients,
		tasks:        b.N,
		elapsed:      elapsed,
		tasksPerSec:  tasksPerSecond,
		nsPerTask:    nsPerTask,
		payloadBytes: scenario.workload.payloadBytes,
	})
}

func registerBenchmarkConsumers(
	b *testing.B,
	client *gasket.Client,
	workload benchmarkWorkload,
	attempts *sync.Map,
	consumerCalls *atomic.Int64,
	terminalTasks *atomic.Int64,
) {
	b.Helper()

	for _, taskType := range []string{"bench-instant", "bench-sleep", "bench-retry"} {
		taskType := taskType
		err := client.RegisterConsumer(taskType, func(id int, payload []byte) (result gasket.TaskConsumerResult) {
			consumerCalls.Add(1)

			if taskType == "bench-sleep" {
				time.Sleep(workload.sleepDuration)
			}

			if taskType == "bench-retry" {
				var failedAlready bool
				if _, loaded := attempts.LoadOrStore(id, struct{}{}); loaded {
					failedAlready = true
				}

				if !failedAlready {
					result.Success = false
					result.Error = fmt.Errorf("benchmark retry")
					return
				}
			}

			result.Success = true
			result.Data = []byte("ok")
			terminalTasks.Add(1)
			return
		})
		if err != nil {
			b.Fatalf("register benchmark consumer %q: %v", taskType, err)
		}
	}
}

func benchmarkTaskType(workload benchmarkWorkload, taskIndex int) string {
	if workload.retryEvery > 0 && (taskIndex+1)%workload.retryEvery == 0 {
		return "bench-retry"
	}

	if workload.sleepEvery > 0 && (taskIndex+1)%workload.sleepEvery == 0 {
		return "bench-sleep"
	}

	return "bench-instant"
}

func benchmarkTaskOptions(workload benchmarkWorkload, taskIndex int) []gasket.TaskOption {
	var opts []gasket.TaskOption

	if workload.runInEvery > 0 && (taskIndex+1)%workload.runInEvery == 0 {
		opts = append(opts, gasket.RunIn(time.Millisecond))
	}

	if workload.retryEvery > 0 && (taskIndex+1)%workload.retryEvery == 0 {
		opts = append(opts, gasket.RetryPolicy(1, time.Millisecond))
	}

	return opts
}

func benchmarkExpectedConsumerCalls(workload benchmarkWorkload, taskIndex int) int {
	if workload.retryEvery > 0 && (taskIndex+1)%workload.retryEvery == 0 {
		return 2
	}

	return 1
}

func waitForBenchmarkCompletions(b *testing.B, tasks []*gasket.TaskInfo, timeout time.Duration) {
	b.Helper()

	deadline := time.Now().Add(timeout)
	for _, taskInfo := range tasks {
		for {
			if time.Now().After(deadline) {
				b.Fatalf("timed out waiting for benchmark completions")
			}

			err := taskInfo.Refresh()
			if err != nil {
				if strings.Contains(err.Error(), "database is locked") {
					time.Sleep(time.Millisecond)
					continue
				}

				b.Fatalf("refresh benchmark task %d: %v", taskInfo.ID(), err)
			}

			switch taskInfo.State() {
			case gasket.TaskStateCompleted:
				goto nextTask
			case gasket.TaskStateFailed:
				b.Fatalf("task %d failed: %v", taskInfo.ID(), taskInfo.LastError())
			case gasket.TaskStateCancelled:
				b.Fatalf("task %d was cancelled", taskInfo.ID())
			}

			time.Sleep(time.Millisecond)
		}

	nextTask:
	}
}

func makeBenchmarkPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	return payload
}

func closeBenchmarkClients(clients []*gasket.Client) {
	for _, client := range clients {
		if client != nil {
			_ = client.Close()
		}
	}
}

func recordBenchmarkGraphRow(b *testing.B, row benchmarkGraphRow) {
	b.Helper()

	if benchmarkGraphDir == nil || *benchmarkGraphDir == "" {
		return
	}

	benchmarkGraphMu.Lock()
	defer benchmarkGraphMu.Unlock()

	benchmarkGraphRows = append(benchmarkGraphRows, row)
	if err := writeBenchmarkGraphFiles(*benchmarkGraphDir, benchmarkGraphRows); err != nil {
		b.Fatalf("write benchmark graph files: %v", err)
	}
}

func writeBenchmarkGraphFiles(dir string, rows []benchmarkGraphRow) (err error) {
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return
	}

	if err = writeBenchmarkCSV(filepath.Join(dir, "gasket-benchmark.csv"), rows); err != nil {
		return
	}

	err = writeBenchmarkSVG(filepath.Join(dir, "gasket-benchmark.svg"), rows)
	return
}

func writeBenchmarkCSV(path string, rows []benchmarkGraphRow) (err error) {
	var file *os.File
	if file, err = os.Create(path); err != nil {
		return
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err = writer.Write([]string{"name", "storage", "workload", "clients", "tasks", "elapsed_ms", "tasks_per_sec", "ns_per_task", "payload_bytes"}); err != nil {
		return
	}

	for _, row := range rows {
		if err = writer.Write([]string{
			row.name,
			string(row.storage),
			row.workload,
			strconv.Itoa(row.clients),
			strconv.Itoa(row.tasks),
			strconv.FormatFloat(float64(row.elapsed)/float64(time.Millisecond), 'f', 3, 64),
			strconv.FormatFloat(row.tasksPerSec, 'f', 2, 64),
			strconv.FormatFloat(row.nsPerTask, 'f', 0, 64),
			strconv.Itoa(row.payloadBytes),
		}); err != nil {
			return
		}
	}

	err = writer.Error()
	return
}

func writeBenchmarkSVG(path string, rows []benchmarkGraphRow) (err error) {
	var file *os.File
	if file, err = os.Create(path); err != nil {
		return
	}

	defer file.Close()

	const (
		width       = 1200
		leftMargin  = 300
		rightMargin = 120
		rowHeight   = 28
		topMargin   = 54
		bottomPad   = 36
	)

	height := topMargin + bottomPad + rowHeight*len(rows)
	maxTasksPerSecond := 0.0
	for _, row := range rows {
		maxTasksPerSecond = math.Max(maxTasksPerSecond, row.tasksPerSec)
	}

	if maxTasksPerSecond <= 0 {
		maxTasksPerSecond = 1
	}

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" width="%d" height="%d" viewBox="0 0 %d %d">`, width, height, width, height))
	builder.WriteString(`<rect width="100%" height="100%" fill="#ffffff"/>`)
	builder.WriteString(`<text x="24" y="34" font-family="Arial, sans-serif" font-size="22" font-weight="700" fill="#1f2933">Gasket benchmark throughput</text>`)

	barMaxWidth := width - leftMargin - rightMargin
	for index, row := range rows {
		y := topMargin + index*rowHeight
		barWidth := int((row.tasksPerSec / maxTasksPerSecond) * float64(barMaxWidth))
		if barWidth < 1 {
			barWidth = 1
		}

		label := fmt.Sprintf("%s / %s / %d clients", row.workload, row.storage, row.clients)
		value := fmt.Sprintf("%.1f tasks/s", row.tasksPerSec)
		color := "#2f80ed"
		if row.storage == benchmarkStorageDisk {
			color = "#27ae60"
		}

		builder.WriteString(fmt.Sprintf(`<text x="24" y="%d" font-family="Arial, sans-serif" font-size="13" fill="#334e68">%s</text>`, y+17, html.EscapeString(label)))
		builder.WriteString(fmt.Sprintf(`<rect x="%d" y="%d" width="%d" height="18" rx="3" fill="%s"/>`, leftMargin, y+4, barWidth, color))
		builder.WriteString(fmt.Sprintf(`<text x="%d" y="%d" font-family="Arial, sans-serif" font-size="13" fill="#102a43">%s</text>`, leftMargin+barWidth+8, y+18, html.EscapeString(value)))
	}

	builder.WriteString(`</svg>`)
	_, err = file.WriteString(builder.String())
	return
}
