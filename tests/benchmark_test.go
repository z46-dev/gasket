package tests

import (
	"context"
	cryptorand "crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"image/color"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/z46-dev/gasket"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

const (
	benchmarkTaskTypeSuccess string = "bench-success"
	benchmarkTaskTypeFailure string = "bench-failure"
	benchmarkTaskTypeRetry   string = "bench-retry"
)

const (
	benchmarkPayloadPoolSize int = 64
	benchmarkMinRecordTasks  int = 2
)

const (
	benchmarkDatabaseLockRetryCount int           = 10000
	benchmarkDatabaseLockRetryDelay time.Duration = time.Millisecond
)

var (
	benchmarkOutputMu sync.Mutex
	benchmarkRecords  []benchmarkRecord
)

type benchmarkWorkload string

const (
	benchmarkWorkloadImmediate benchmarkWorkload = "immediate-small"
	benchmarkWorkloadScheduled benchmarkWorkload = "scheduled-due-small"
	benchmarkWorkloadMixed     benchmarkWorkload = "mixed-results-small"
	benchmarkWorkloadPayload   benchmarkWorkload = "large-payload"
	benchmarkWorkloadRetry     benchmarkWorkload = "retry-once-small"
)

type benchmarkCounters struct {
	successes        atomic.Int64
	failures         atomic.Int64
	attempts         atomic.Int64
	consumerFailures atomic.Int64
	payloadBytes     atomic.Int64
	payloadChecksum  atomic.Uint64
	startedUnixNano  atomic.Int64
	retryMu          sync.Mutex
	retrySeen        map[int]bool
	latencyMu        sync.Mutex
	latenciesMS      []float64
}

type benchmarkSample struct {
	ElapsedMS        int64   `json:"elapsed_ms"`
	OpsPerSecond     float64 `json:"ops_per_second"`
	Successes        int64   `json:"successes"`
	Failures         int64   `json:"failures"`
	Attempts         int64   `json:"attempts"`
	ConsumerFailures int64   `json:"consumer_failures"`
}

type benchmarkRecord struct {
	Name             string            `json:"name"`
	Workload         benchmarkWorkload `json:"workload"`
	Clients          int               `json:"clients"`
	Tasks            int               `json:"tasks"`
	DurationMS       int64             `json:"duration_ms"`
	OpsPerSecond     float64           `json:"ops_per_second"`
	AttemptsPerTask  float64           `json:"attempts_per_task"`
	PayloadMBPerSec  float64           `json:"payload_mb_per_second"`
	CompletionMeanMS float64           `json:"completion_mean_ms"`
	CompletionP50MS  float64           `json:"completion_p50_ms"`
	CompletionP95MS  float64           `json:"completion_p95_ms"`
	CompletionMaxMS  float64           `json:"completion_max_ms"`
	Successes        int64             `json:"successes"`
	Failures         int64             `json:"failures"`
	Attempts         int64             `json:"attempts"`
	ConsumerFailures int64             `json:"consumer_failures"`
	PayloadBytes     int64             `json:"payload_bytes"`
	PayloadChecksum  uint64            `json:"payload_checksum"`
	Samples          []benchmarkSample `json:"samples"`
}

type benchmarkTaskCounts struct {
	terminal  int64
	completed int64
	failed    int64
}

type benchmarkPayloadPool struct {
	payloads    [][]byte
	averageSize int64
}

type benchmarkErrorPoints struct {
	plotter.XYs
	plotter.YErrors
}

func (counters *benchmarkCounters) terminalCount() int64 {
	return counters.successes.Load() + counters.failures.Load()
}

func (counters *benchmarkCounters) recordCompletionLatency() {
	var (
		started int64 = counters.startedUnixNano.Load()
		latency float64
	)

	if started == 0 {
		return
	}

	latency = float64(time.Since(time.Unix(0, started)).Microseconds()) / 1000
	counters.latencyMu.Lock()
	counters.latenciesMS = append(counters.latenciesMS, latency)
	counters.latencyMu.Unlock()
}

func (counters *benchmarkCounters) completionLatencies() (latencies []float64) {
	counters.latencyMu.Lock()
	defer counters.latencyMu.Unlock()

	latencies = append([]float64{}, counters.latenciesMS...)
	return
}

func BenchmarkTaskProcessing(b *testing.B) {
	var (
		workloads    []benchmarkWorkload = []benchmarkWorkload{benchmarkWorkloadImmediate, benchmarkWorkloadScheduled, benchmarkWorkloadMixed, benchmarkWorkloadPayload, benchmarkWorkloadRetry}
		clientCounts []int               = []int{1, 2, 4}
		workload     benchmarkWorkload
	)

	for _, workload = range workloads {
		runBenchmarkWorkload(b, workload, clientCounts)
	}
}

func runBenchmarkWorkload(b *testing.B, workload benchmarkWorkload, clientCounts []int) {
	b.Run(string(workload), func(b *testing.B) {
		var clientCount int

		for _, clientCount = range clientCounts {
			runBenchmarkClientCount(b, workload, clientCount)
		}
	})
}

func runBenchmarkClientCount(b *testing.B, workload benchmarkWorkload, clientCount int) {
	var name string = fmt.Sprintf("clients=%d", clientCount)

	b.Run(name, func(b *testing.B) {
		runBenchmarkTaskProcessing(b, workload, clientCount)
	})
}

func runBenchmarkTaskProcessing(b *testing.B, workload benchmarkWorkload, clientCount int) {
	var (
		dbPath    string = filepath.Join(b.TempDir(), "benchmark.db")
		counters  *benchmarkCounters
		clients   []*gasket.Client
		db        *sql.DB
		err       error
		cancel    context.CancelFunc
		runErr    chan error
		samples   []benchmarkSample
		stop      chan struct{}
		sampleOut chan []benchmarkSample
		started   time.Time
		elapsed   time.Duration
		counts    benchmarkTaskCounts
		record    benchmarkRecord
		rng       *rand.Rand
		latencies []float64
	)

	b.StopTimer()

	counters = &benchmarkCounters{
		retrySeen: map[int]bool{},
	}

	if clients, err = newBenchmarkClients(dbPath, clientCount, counters); err != nil {
		b.Fatalf("Error creating benchmark clients: %v", err)
		return
	}

	defer closeBenchmarkClients(b, clients)

	if db, err = sql.Open("sqlite", dbPath); err != nil {
		b.Fatalf("Error opening benchmark database: %v", err)
		return
	}

	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	rng = newBenchmarkRand()
	seedBenchmarkTasks(b, clients[0], workload, rng)

	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()

	started = time.Now()
	counters.startedUnixNano.Store(started.UnixNano())
	stop = make(chan struct{})
	sampleOut = make(chan []benchmarkSample, 1)
	go sampleBenchmarkCounters(counters, started, benchmarkSampleInterval(), stop, sampleOut)

	_, cancel, runErr = runBenchmarkClients(clients)
	defer cancel()

	counts = waitForBenchmarkCompletion(b, db, b.N, counters, runErr)
	elapsed = time.Since(started)

	close(stop)
	samples = <-sampleOut

	b.StopTimer()
	cancel()
	waitForBenchmarkClients(b, runErr, len(clients))

	if counts.completed != counters.successes.Load() {
		b.Fatalf("Expected completed task count %d to match consumer successes %d", counts.completed, counters.successes.Load())
		return
	}

	if counts.failed != counters.failures.Load() {
		b.Fatalf("Expected failed task count %d to match consumer failures %d", counts.failed, counters.failures.Load())
		return
	}

	latencies = counters.completionLatencies()
	record = benchmarkRecord{
		Name:             b.Name(),
		Workload:         workload,
		Clients:          clientCount,
		Tasks:            b.N,
		DurationMS:       elapsed.Milliseconds(),
		OpsPerSecond:     float64(b.N) / elapsed.Seconds(),
		AttemptsPerTask:  float64(counters.attempts.Load()) / float64(b.N),
		PayloadMBPerSec:  float64(counters.payloadBytes.Load()) / (1024 * 1024) / elapsed.Seconds(),
		Successes:        counters.successes.Load(),
		Failures:         counters.failures.Load(),
		Attempts:         counters.attempts.Load(),
		ConsumerFailures: counters.consumerFailures.Load(),
		PayloadBytes:     counters.payloadBytes.Load(),
		PayloadChecksum:  counters.payloadChecksum.Load(),
		Samples:          samples,
	}
	record.CompletionMeanMS, record.CompletionP50MS, record.CompletionP95MS, record.CompletionMaxMS = benchmarkLatencyStats(latencies)

	b.ReportMetric(record.OpsPerSecond, "ops/sec")
	b.ReportMetric(record.AttemptsPerTask, "attempts/task")
	b.ReportMetric(record.PayloadMBPerSec, "payload_MBps")
	b.ReportMetric(record.CompletionP95MS, "p95_ms")
	b.ReportMetric(float64(record.Successes), "successes")
	b.ReportMetric(float64(record.Failures), "failures")
	b.ReportMetric(float64(record.Attempts), "attempts")
	b.ReportMetric(float64(record.ConsumerFailures), "consumer_failures")
	b.ReportMetric(float64(record.PayloadBytes)/(1024*1024), "payload_mb")

	writeBenchmarkRecord(b, record)
}

func newBenchmarkClients(dbPath string, count int, counters *benchmarkCounters) (clients []*gasket.Client, err error) {
	var (
		i      int
		client *gasket.Client
	)

	clients = make([]*gasket.Client, 0, count)
	for i = 0; i < count; i++ {
		if client, err = gasket.NewClient(dbPath, gasket.PollInterval(time.Millisecond), gasket.DatabaseLockRetry(benchmarkDatabaseLockRetryCount, benchmarkDatabaseLockRetryDelay)); err != nil {
			return
		}

		if err = registerBenchmarkConsumers(client, counters); err != nil {
			return
		}

		clients = append(clients, client)
	}

	return
}

func registerBenchmarkConsumers(client *gasket.Client, counters *benchmarkCounters) (err error) {
	if err = client.RegisterConsumer(benchmarkTaskTypeSuccess, func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		counters.attempts.Add(1)
		consumeBenchmarkPayload(counters, payload)
		counters.successes.Add(1)
		counters.recordCompletionLatency()
		result.Success = true
		return
	}); err != nil {
		return
	}

	if err = client.RegisterConsumer(benchmarkTaskTypeFailure, func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		counters.attempts.Add(1)
		consumeBenchmarkPayload(counters, payload)
		counters.consumerFailures.Add(1)
		counters.failures.Add(1)
		counters.recordCompletionLatency()
		result.Error = fmt.Errorf("benchmark failure")
		return
	}); err != nil {
		return
	}

	err = client.RegisterConsumer(benchmarkTaskTypeRetry, func(id int, payload []byte) (result gasket.TaskConsumerResult) {
		var failed bool

		counters.attempts.Add(1)
		consumeBenchmarkPayload(counters, payload)

		counters.retryMu.Lock()
		if !counters.retrySeen[id] {
			counters.retrySeen[id] = true
			failed = true
		}
		counters.retryMu.Unlock()

		if failed {
			counters.consumerFailures.Add(1)
			result.Error = fmt.Errorf("benchmark retry")
			return
		}

		counters.successes.Add(1)
		counters.recordCompletionLatency()
		result.Success = true
		return
	})

	return
}

func seedBenchmarkTasks(b *testing.B, client *gasket.Client, workload benchmarkWorkload, rng *rand.Rand) {
	var (
		i       int
		task    string
		payload []byte
		opts    []gasket.TaskOption
		err     error
		pool    benchmarkPayloadPool
	)

	pool = newBenchmarkPayloadPool(b, workload, rng)
	if pool.averageSize > 0 {
		b.SetBytes(pool.averageSize)
	}

	for i = 0; i < b.N; i++ {
		task = benchmarkTaskType(workload, rng)
		payload = pool.payloads[rng.Intn(len(pool.payloads))]
		opts = benchmarkTaskOptions(workload, task, rng)

		if _, err = client.NewTask(task, payload, opts...); err != nil {
			b.Fatalf("Error creating benchmark task: %v", err)
			return
		}
	}
}

func benchmarkTaskType(workload benchmarkWorkload, rng *rand.Rand) (taskType string) {
	var roll int

	taskType = benchmarkTaskTypeSuccess

	switch workload {
	case benchmarkWorkloadMixed:
		roll = rng.Intn(100)
		if roll < 15 {
			taskType = benchmarkTaskTypeFailure
		} else if roll < 30 {
			taskType = benchmarkTaskTypeRetry
		}
	case benchmarkWorkloadRetry:
		taskType = benchmarkTaskTypeRetry
	}

	return
}

func benchmarkTaskOptions(workload benchmarkWorkload, taskType string, rng *rand.Rand) (opts []gasket.TaskOption) {
	var scheduledFor time.Duration

	switch workload {
	case benchmarkWorkloadScheduled:
		scheduledFor = -time.Duration(rng.Intn(10)+1) * time.Millisecond
		opts = append(opts, gasket.ScheduleIn(scheduledFor))
	}

	if taskType == benchmarkTaskTypeRetry {
		opts = append(opts, gasket.RetryPolicy(1, time.Nanosecond))
	}

	return
}

func newBenchmarkPayloadPool(b *testing.B, workload benchmarkWorkload, rng *rand.Rand) (pool benchmarkPayloadPool) {
	var (
		i       int
		size    int
		total   int64
		err     error
		payload []byte
	)

	pool.payloads = make([][]byte, 0, benchmarkPayloadPoolSize)
	for i = 0; i < benchmarkPayloadPoolSize; i++ {
		size = benchmarkPayloadSize(workload, rng)
		payload = make([]byte, size)
		if _, err = cryptorand.Read(payload); err != nil {
			b.Fatalf("Error generating random benchmark payload: %v", err)
			return
		}

		total += int64(size)
		pool.payloads = append(pool.payloads, payload)
	}

	pool.averageSize = total / int64(len(pool.payloads))
	return
}

func benchmarkPayloadSize(workload benchmarkWorkload, rng *rand.Rand) (size int) {
	var (
		minSize int = 256
		maxSize int = 4 * 1024
	)

	switch workload {
	case benchmarkWorkloadScheduled:
		minSize = 128
		maxSize = 2 * 1024
	case benchmarkWorkloadMixed:
		minSize = 512
		maxSize = 8 * 1024
	case benchmarkWorkloadPayload:
		minSize = 96 * 1024
		maxSize = 256 * 1024
	case benchmarkWorkloadRetry:
		minSize = 256
		maxSize = 4 * 1024
	}

	size = minSize + rng.Intn(maxSize-minSize+1)
	return
}

func consumeBenchmarkPayload(counters *benchmarkCounters, payload []byte) {
	counters.payloadBytes.Add(int64(len(payload)))
	counters.payloadChecksum.Add(uint64(crc32.ChecksumIEEE(payload)))
}

func newBenchmarkRand() (rng *rand.Rand) {
	var (
		seedBytes [8]byte
		seed      int64
		err       error
	)

	if _, err = cryptorand.Read(seedBytes[:]); err != nil {
		seed = time.Now().UnixNano()
	} else {
		seed = int64(binary.LittleEndian.Uint64(seedBytes[:]))
	}

	rng = rand.New(rand.NewSource(seed))
	return
}

func runBenchmarkClients(clients []*gasket.Client) (ctx context.Context, cancel context.CancelFunc, runErr chan error) {
	var client *gasket.Client

	ctx, cancel = context.WithCancel(context.Background())
	runErr = make(chan error, len(clients))

	for _, client = range clients {
		go func(client *gasket.Client) {
			runErr <- client.Run(ctx)
		}(client)
	}

	return
}

func waitForBenchmarkCompletion(b *testing.B, db *sql.DB, total int, counters *benchmarkCounters, runErr <-chan error) (counts benchmarkTaskCounts) {
	var (
		deadline time.Time = time.Now().Add(5 * time.Minute)
		err      error
	)

	for counters.terminalCount() < int64(total) {
		select {
		case err = <-runErr:
			b.Fatalf("Benchmark client stopped before completion: %v", err)
			return
		default:
		}

		if time.Now().After(deadline) {
			b.Fatalf("Timed out waiting for benchmark consumers: got %d of %d terminal tasks", counters.terminalCount(), total)
			return
		}

		time.Sleep(time.Millisecond)
	}

	for {
		select {
		case err = <-runErr:
			b.Fatalf("Benchmark client stopped before completion: %v", err)
			return
		default:
		}

		if counts, err = loadBenchmarkTaskCounts(db); err == nil && counts.terminal == int64(total) {
			return
		}

		if time.Now().After(deadline) {
			if err != nil {
				b.Fatalf("Timed out waiting for benchmark completion after last count error: %v", err)
				return
			}

			b.Fatalf("Timed out waiting for benchmark completion: got %d of %d terminal tasks", counts.terminal, total)
			return
		}

		time.Sleep(time.Millisecond)
	}
}

func loadBenchmarkTaskCounts(db *sql.DB) (counts benchmarkTaskCounts, err error) {
	var row *sql.Row

	row = db.QueryRow(
		`SELECT
			COUNT(CASE WHEN state IN (?, ?) THEN 1 END),
			COUNT(CASE WHEN state = ? THEN 1 END),
			COUNT(CASE WHEN state = ? THEN 1 END)
		FROM task;`,
		string(gasket.TaskStateCompleted),
		string(gasket.TaskStateFailed),
		string(gasket.TaskStateCompleted),
		string(gasket.TaskStateFailed),
	)

	err = row.Scan(&counts.terminal, &counts.completed, &counts.failed)
	return
}

func sampleBenchmarkCounters(counters *benchmarkCounters, started time.Time, interval time.Duration, stop <-chan struct{}, sampleOut chan<- []benchmarkSample) {
	var (
		ticker  *time.Ticker
		samples []benchmarkSample = make([]benchmarkSample, 0)
	)

	ticker = time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			samples = append(samples, newBenchmarkSample(counters, started))
		case <-stop:
			samples = append(samples, newBenchmarkSample(counters, started))
			sampleOut <- samples
			return
		}
	}
}

func newBenchmarkSample(counters *benchmarkCounters, started time.Time) (sample benchmarkSample) {
	var (
		elapsed   time.Duration = time.Since(started)
		completed int64
	)

	completed = counters.successes.Load() + counters.failures.Load()
	sample.ElapsedMS = elapsed.Milliseconds()
	if elapsed > 0 {
		sample.OpsPerSecond = float64(completed) / elapsed.Seconds()
	}

	sample.Successes = counters.successes.Load()
	sample.Failures = counters.failures.Load()
	sample.Attempts = counters.attempts.Load()
	sample.ConsumerFailures = counters.consumerFailures.Load()
	return
}

func benchmarkSampleInterval() (interval time.Duration) {
	var (
		raw string = os.Getenv("GASKET_BENCH_SAMPLE_INTERVAL")
		err error
	)

	interval = 50 * time.Millisecond
	if raw == "" {
		return
	}

	if interval, err = time.ParseDuration(raw); err != nil || interval <= 0 {
		interval = 50 * time.Millisecond
	}

	return
}

func writeBenchmarkRecord(b *testing.B, record benchmarkRecord) {
	var (
		path    string = os.Getenv("GASKET_BENCH_JSON")
		plotDir string = os.Getenv("GASKET_BENCH_PLOT_DIR")
		dir     string
		file    *os.File
		encoder *json.Encoder
		err     error
		records []benchmarkRecord
	)

	if path == "" && plotDir == "" {
		path = "./benchmark_results.json"
		plotDir = "./benchmark_plots"
	}

	if record.Tasks < benchmarkMinRecordTasks {
		return
	}

	benchmarkOutputMu.Lock()
	benchmarkRecords = append(benchmarkRecords, record)
	records = append([]benchmarkRecord{}, benchmarkRecords...)
	defer benchmarkOutputMu.Unlock()

	if path != "" {
		dir = filepath.Dir(path)
		if dir != "." {
			if err = os.MkdirAll(dir, 0755); err != nil {
				b.Fatalf("Error creating benchmark output directory: %v", err)
				return
			}
		}

		if file, err = os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err != nil {
			b.Fatalf("Error opening benchmark output: %v", err)
			return
		}

		defer file.Close()

		encoder = json.NewEncoder(file)
		if err = encoder.Encode(record); err != nil {
			b.Fatalf("Error writing benchmark output: %v", err)
			return
		}
	}

	if plotDir != "" {
		writeBenchmarkPlots(b, plotDir, records)
	}
}

func writeBenchmarkPlots(b *testing.B, dir string, records []benchmarkRecord) {
	var err error

	records = benchmarkPlotRecords(records)

	if err = os.MkdirAll(dir, 0755); err != nil {
		b.Fatalf("Error creating benchmark plot directory: %v", err)
		return
	}

	if err = writeBenchmarkOpsPlot(filepath.Join(dir, "ops-by-client.png"), records); err != nil {
		b.Fatalf("Error writing benchmark ops plot: %v", err)
		return
	}

	if err = writeBenchmarkLatencyPlot(filepath.Join(dir, "p95-latency-by-client.png"), records); err != nil {
		b.Fatalf("Error writing benchmark latency plot: %v", err)
		return
	}

	if err = writeBenchmarkPayloadThroughputPlot(filepath.Join(dir, "payload-throughput-by-client.png"), records); err != nil {
		b.Fatalf("Error writing benchmark payload throughput plot: %v", err)
		return
	}

	if err = writeBenchmarkOutcomePlot(filepath.Join(dir, "outcomes.png"), records); err != nil {
		b.Fatalf("Error writing benchmark outcome plot: %v", err)
		return
	}

	if err = writeBenchmarkTimeSeriesPlots(dir, records); err != nil {
		b.Fatalf("Error writing benchmark time series plot: %v", err)
		return
	}
}

func writeBenchmarkOpsPlot(path string, records []benchmarkRecord) (err error) {
	return writeBenchmarkMetricByClientPlot(path, records, "Task throughput by client count", "operations per second", func(record benchmarkRecord) float64 {
		return record.OpsPerSecond
	})
}

func writeBenchmarkLatencyPlot(path string, records []benchmarkRecord) (err error) {
	return writeBenchmarkMetricByClientPlot(path, records, "P95 completion latency by client count", "milliseconds", func(record benchmarkRecord) float64 {
		return record.CompletionP95MS
	})
}

func writeBenchmarkPayloadThroughputPlot(path string, records []benchmarkRecord) (err error) {
	return writeBenchmarkMetricByClientPlot(path, records, "Payload throughput by client count", "MiB per second", func(record benchmarkRecord) float64 {
		return record.PayloadMBPerSec
	})
}

func writeBenchmarkMetricByClientPlot(path string, records []benchmarkRecord, title string, yLabel string, value func(benchmarkRecord) float64) (err error) {
	var (
		p         *plot.Plot = plot.New()
		workload  benchmarkWorkload
		workloads []benchmarkWorkload
		index     int
		points    benchmarkErrorPoints
		line      *plotter.Line
		scatter   *plotter.Scatter
		errors    *plotter.YErrorBars
		color     color.Color
	)

	p.Title.Text = title
	p.X.Label.Text = "clients"
	p.Y.Label.Text = yLabel
	p.Add(plotter.NewGrid())
	p.Legend.Top = true
	p.X.Tick.Marker = plot.ConstantTicks(benchmarkClientTicks(records))

	workloads = sortedBenchmarkWorkloads(records)
	for index, workload = range workloads {
		points = benchmarkMetricPoints(records, workload, value)
		if len(points.XYs) == 0 {
			continue
		}

		color = plotutil.Color(index)
		if line, err = plotter.NewLine(points); err != nil {
			return
		}

		line.Color = color
		line.Width = vg.Points(1.5)
		line.Dashes = plotutil.Dashes(index)

		if scatter, err = plotter.NewScatter(points); err != nil {
			return
		}

		scatter.Color = color
		scatter.Shape = plotutil.Shape(index)
		scatter.Radius = vg.Points(3)

		if errors, err = plotter.NewYErrorBars(points); err != nil {
			return
		}

		errors.LineStyle.Color = color
		errors.LineStyle.Width = vg.Points(1)
		errors.CapWidth = vg.Points(6)

		p.Add(line, scatter, errors)
		p.Legend.Add(benchmarkWorkloadLabel(workload), line, scatter)
	}

	err = p.Save(10*vg.Inch, 5*vg.Inch, path)
	return
}

func writeBenchmarkOutcomePlot(path string, records []benchmarkRecord) (err error) {
	var (
		p       *plot.Plot = plot.New()
		series  []string   = []string{"success %", "failed %", "consumer failure %"}
		index   int
		name    string
		points  benchmarkErrorPoints
		line    *plotter.Line
		scatter *plotter.Scatter
		errors  *plotter.YErrorBars
		color   color.Color
	)

	p.Title.Text = "Benchmark outcomes"
	p.X.Label.Text = "workload"
	p.Y.Label.Text = "percentage"
	p.Y.Min = 0
	p.Y.Max = 100
	p.Add(plotter.NewGrid())
	p.Legend.Top = true
	p.X.Tick.Marker = plot.ConstantTicks(benchmarkWorkloadTicks(records))

	for index, name = range series {
		points = benchmarkOutcomePoints(records, name)
		if len(points.XYs) == 0 {
			continue
		}

		color = plotutil.Color(index)
		if line, err = plotter.NewLine(points); err != nil {
			return
		}

		line.Color = color
		line.Width = vg.Points(1.5)
		line.Dashes = plotutil.Dashes(index)

		if scatter, err = plotter.NewScatter(points); err != nil {
			return
		}

		scatter.Color = color
		scatter.Shape = plotutil.Shape(index)
		scatter.Radius = vg.Points(3)

		if errors, err = plotter.NewYErrorBars(points); err != nil {
			return
		}

		errors.LineStyle.Color = color
		errors.LineStyle.Width = vg.Points(1)
		errors.CapWidth = vg.Points(6)

		p.Add(line, scatter, errors)
		p.Legend.Add(name, line, scatter)
	}

	err = p.Save(10*vg.Inch, 5*vg.Inch, path)
	return
}

func writeBenchmarkTimeSeriesPlots(dir string, records []benchmarkRecord) (err error) {
	var (
		workloads []benchmarkWorkload
		workload  benchmarkWorkload
	)

	workloads = sortedBenchmarkWorkloads(records)
	for _, workload = range workloads {
		if err = writeBenchmarkTimeSeriesPlot(filepath.Join(dir, fmt.Sprintf("ops-over-time-%s.png", benchmarkWorkloadFileName(workload))), records, workload); err != nil {
			return
		}
	}

	return
}

func writeBenchmarkTimeSeriesPlot(path string, records []benchmarkRecord, workload benchmarkWorkload) (err error) {
	var (
		p       *plot.Plot = plot.New()
		latest  []benchmarkRecord
		record  benchmarkRecord
		index   int
		points  plotter.XYs
		line    *plotter.Line
		scatter *plotter.Scatter
		color   color.Color
	)

	p.Title.Text = fmt.Sprintf("%s operations per second over time", benchmarkWorkloadLabel(workload))
	p.X.Label.Text = "seconds"
	p.Y.Label.Text = "operations per second"
	p.Add(plotter.NewGrid())
	p.Legend.Top = true

	latest = latestBenchmarkRecords(records, workload)
	for index, record = range latest {
		points = benchmarkSamplePoints(record)
		if len(points) == 0 {
			continue
		}

		color = plotutil.Color(index)
		if line, err = plotter.NewLine(points); err != nil {
			return
		}

		if scatter, err = plotter.NewScatter(points); err != nil {
			return
		}

		line.Color = color
		line.Width = vg.Points(1.25)
		line.Dashes = plotutil.Dashes(index)
		scatter.Color = color
		scatter.Shape = plotutil.Shape(index)
		scatter.Radius = vg.Points(2.5)
		p.Add(line, scatter)
		p.Legend.Add(fmt.Sprintf("%d clients", record.Clients), line, scatter)
	}

	err = p.Save(11*vg.Inch, 5*vg.Inch, path)
	return
}

func benchmarkOpsPoints(records []benchmarkRecord, workload benchmarkWorkload) (points benchmarkErrorPoints) {
	return benchmarkMetricPoints(records, workload, func(record benchmarkRecord) float64 {
		return record.OpsPerSecond
	})
}

func benchmarkMetricPoints(records []benchmarkRecord, workload benchmarkWorkload, value func(benchmarkRecord) float64) (points benchmarkErrorPoints) {
	var (
		clients []int
		client  int
		values  []float64
		mean    float64
		stddev  float64
	)

	clients = sortedBenchmarkClients(records)
	for _, client = range clients {
		values = benchmarkMetricValues(records, workload, client, value)
		if len(values) == 0 {
			continue
		}

		mean, stddev = meanAndStddev(values)
		points.XYs = append(points.XYs, plotter.XY{X: float64(client), Y: mean})
		points.YErrors = append(points.YErrors, struct{ Low, High float64 }{Low: stddev, High: stddev})
	}

	return
}

func benchmarkOutcomePoints(records []benchmarkRecord, series string) (points benchmarkErrorPoints) {
	var (
		workloads []benchmarkWorkload
		workload  benchmarkWorkload
		index     int
		values    []float64
		mean      float64
		stddev    float64
	)

	workloads = sortedBenchmarkWorkloads(records)
	for index, workload = range workloads {
		values = benchmarkOutcomeValues(records, workload, series)
		if len(values) == 0 {
			continue
		}

		mean, stddev = meanAndStddev(values)
		points.XYs = append(points.XYs, plotter.XY{X: float64(index + 1), Y: mean})
		points.YErrors = append(points.YErrors, struct{ Low, High float64 }{Low: stddev, High: stddev})
	}

	return
}

func benchmarkOpsValues(records []benchmarkRecord, workload benchmarkWorkload, clients int) (values []float64) {
	return benchmarkMetricValues(records, workload, clients, func(record benchmarkRecord) float64 {
		return record.OpsPerSecond
	})
}

func benchmarkMetricValues(records []benchmarkRecord, workload benchmarkWorkload, clients int, value func(benchmarkRecord) float64) (values []float64) {
	var record benchmarkRecord

	for _, record = range records {
		if record.Workload == workload && record.Clients == clients {
			values = append(values, value(record))
		}
	}

	return
}

func benchmarkOutcomeValues(records []benchmarkRecord, workload benchmarkWorkload, series string) (values []float64) {
	var (
		record benchmarkRecord
		total  float64
		value  float64
	)

	for _, record = range records {
		if record.Workload != workload {
			continue
		}

		total = float64(record.Successes + record.Failures)
		if total == 0 {
			continue
		}

		switch series {
		case "success %":
			value = 100 * float64(record.Successes) / total
		case "failed %":
			value = 100 * float64(record.Failures) / total
		case "consumer failure %":
			if record.Attempts == 0 {
				continue
			}
			value = 100 * float64(record.ConsumerFailures) / float64(record.Attempts)
		}

		values = append(values, value)
	}

	return
}

func benchmarkSamplePoints(record benchmarkRecord) (points plotter.XYs) {
	var (
		sample benchmarkSample
	)

	for _, sample = range record.Samples {
		points = append(points, plotter.XY{
			X: float64(sample.ElapsedMS) / 1000,
			Y: sample.OpsPerSecond,
		})
	}

	return
}

func benchmarkPlotRecords(records []benchmarkRecord) (filtered []benchmarkRecord) {
	var (
		maxTasksByName map[string]int = map[string]int{}
		record         benchmarkRecord
		maxTasks       int
	)

	for _, record = range records {
		if record.Tasks > maxTasksByName[record.Name] {
			maxTasksByName[record.Name] = record.Tasks
		}
	}

	for _, record = range records {
		maxTasks = maxTasksByName[record.Name]
		if record.Tasks == maxTasks {
			filtered = append(filtered, record)
		}
	}

	return
}

func sortedBenchmarkWorkloads(records []benchmarkRecord) (workloads []benchmarkWorkload) {
	var (
		seen   map[benchmarkWorkload]bool = map[benchmarkWorkload]bool{}
		record benchmarkRecord
	)

	for _, record = range records {
		if seen[record.Workload] {
			continue
		}

		seen[record.Workload] = true
		workloads = append(workloads, record.Workload)
	}

	sort.Slice(workloads, func(i int, j int) bool {
		return benchmarkWorkloadRank(workloads[i]) < benchmarkWorkloadRank(workloads[j])
	})
	return
}

func benchmarkWorkloadRank(workload benchmarkWorkload) int {
	switch workload {
	case benchmarkWorkloadImmediate:
		return 0
	case benchmarkWorkloadScheduled:
		return 1
	case benchmarkWorkloadMixed:
		return 2
	case benchmarkWorkloadRetry:
		return 3
	case benchmarkWorkloadPayload:
		return 4
	default:
		return 100
	}
}

func sortedBenchmarkClients(records []benchmarkRecord) (clients []int) {
	var (
		seen   map[int]bool = map[int]bool{}
		record benchmarkRecord
	)

	for _, record = range records {
		if seen[record.Clients] {
			continue
		}

		seen[record.Clients] = true
		clients = append(clients, record.Clients)
	}

	sort.Ints(clients)
	return
}

func benchmarkClientTicks(records []benchmarkRecord) (ticks []plot.Tick) {
	var (
		clients []int
		client  int
	)

	clients = sortedBenchmarkClients(records)
	for _, client = range clients {
		ticks = append(ticks, plot.Tick{
			Value: float64(client),
			Label: fmt.Sprintf("%d", client),
		})
	}

	return
}

func benchmarkWorkloadTicks(records []benchmarkRecord) (ticks []plot.Tick) {
	var (
		workloads []benchmarkWorkload
		workload  benchmarkWorkload
		index     int
	)

	workloads = sortedBenchmarkWorkloads(records)
	for index, workload = range workloads {
		ticks = append(ticks, plot.Tick{
			Value: float64(index + 1),
			Label: benchmarkWorkloadLabel(workload),
		})
	}

	return
}

func latestBenchmarkRecords(records []benchmarkRecord, workload benchmarkWorkload) (latest []benchmarkRecord) {
	var (
		byName map[string]benchmarkRecord = map[string]benchmarkRecord{}
		names  []string
		name   string
		record benchmarkRecord
		ok     bool
	)

	for _, record = range records {
		if record.Workload != workload {
			continue
		}

		if _, ok = byName[record.Name]; !ok {
			names = append(names, record.Name)
		}

		byName[record.Name] = record
	}

	sort.Strings(names)
	for _, name = range names {
		latest = append(latest, byName[name])
	}

	return
}

func benchmarkWorkloadFileName(workload benchmarkWorkload) (name string) {
	switch workload {
	case benchmarkWorkloadImmediate:
		name = "immediate-small"
	case benchmarkWorkloadScheduled:
		name = "scheduled-due-small"
	case benchmarkWorkloadMixed:
		name = "mixed-results-small"
	case benchmarkWorkloadPayload:
		name = "large-payload"
	case benchmarkWorkloadRetry:
		name = "retry-once-small"
	default:
		name = "workload"
	}

	return
}

func benchmarkWorkloadLabel(workload benchmarkWorkload) (label string) {
	switch workload {
	case benchmarkWorkloadImmediate:
		label = "immediate small"
	case benchmarkWorkloadScheduled:
		label = "scheduled due small"
	case benchmarkWorkloadMixed:
		label = "mixed results small"
	case benchmarkWorkloadPayload:
		label = "large payload"
	case benchmarkWorkloadRetry:
		label = "retry once small"
	default:
		label = string(workload)
	}

	return
}

func meanAndStddev(values []float64) (mean float64, stddev float64) {
	var (
		value float64
		sum   float64
	)

	for _, value = range values {
		sum += value
	}

	mean = sum / float64(len(values))
	if len(values) < 2 {
		return
	}

	sum = 0
	for _, value = range values {
		sum += (value - mean) * (value - mean)
	}

	stddev = math.Sqrt(sum / float64(len(values)-1))
	return
}

func benchmarkLatencyStats(values []float64) (mean float64, p50 float64, p95 float64, max float64) {
	var (
		value  float64
		sorted []float64
	)

	if len(values) == 0 {
		return
	}

	for _, value = range values {
		mean += value
	}
	mean /= float64(len(values))

	sorted = append([]float64{}, values...)
	sort.Float64s(sorted)

	p50 = benchmarkPercentile(sorted, 0.50)
	p95 = benchmarkPercentile(sorted, 0.95)
	max = sorted[len(sorted)-1]
	return
}

func benchmarkPercentile(sorted []float64, percentile float64) float64 {
	var index int

	if len(sorted) == 0 {
		return 0
	}

	index = int(math.Ceil(percentile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	} else if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}

func closeBenchmarkClients(b *testing.B, clients []*gasket.Client) {
	var (
		client *gasket.Client
		err    error
	)

	for _, client = range clients {
		if client == nil {
			continue
		}

		if err = client.Close(); err != nil {
			b.Errorf("Error closing benchmark client: %v", err)
		}
	}
}

func waitForBenchmarkClients(b *testing.B, runErr <-chan error, count int) {
	var (
		i   int
		err error
	)

	for i = 0; i < count; i++ {
		select {
		case err = <-runErr:
			if err != nil {
				b.Fatalf("Benchmark client stopped with error: %v", err)
				return
			}
		case <-time.After(time.Second):
			b.Fatal("Timed out waiting for benchmark client shutdown")
			return
		}
	}
}
