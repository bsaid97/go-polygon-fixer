package utils

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerPool manages a pool of goroutines for parallel processing
type WorkerPool struct {
	NumWorkers int
	JobQueue   chan interface{}
	Results    chan interface{}
	wg         sync.WaitGroup
	started    bool
	mu         sync.Mutex
}

// NewWorkerPool creates a new worker pool with specified number of workers
func NewWorkerPool(numWorkers int, jobBufferSize int, resultBufferSize int) *WorkerPool {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	
	return &WorkerPool{
		NumWorkers: numWorkers,
		JobQueue:   make(chan interface{}, jobBufferSize),
		Results:    make(chan interface{}, resultBufferSize),
		started:    false,
	}
}

// StartWorkers starts the worker goroutines with the given work function
func (wp *WorkerPool) StartWorkers(workFunc func(interface{}) interface{}) {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	
	if wp.started {
		return
	}
	
	wp.started = true
	wp.wg.Add(wp.NumWorkers)
	
	for i := 0; i < wp.NumWorkers; i++ {
		go wp.worker(i, workFunc)
	}
}

// worker processes jobs from the job queue
func (wp *WorkerPool) worker(id int, workFunc func(interface{}) interface{}) {
	defer wp.wg.Done()
	
	for job := range wp.JobQueue {
		result := workFunc(job)
		// Always send the result, even if it's nil
		wp.Results <- result
	}
}

// SubmitJob adds a job to the job queue
func (wp *WorkerPool) SubmitJob(job interface{}) {
	wp.JobQueue <- job
}

// ProgressTracker tracks progress of concurrent operations
type ProgressTracker struct {
	Total     int64
	Processed int64
	StartTime time.Time
	Name      string
}

// NewProgressTracker creates a new progress tracker
func NewProgressTracker(total int64, name string) *ProgressTracker {
	return &ProgressTracker{
		Total:     total,
		Processed: 0,
		StartTime: time.Now(),
		Name:      name,
	}
}

// Increment increments the processed count atomically
func (pt *ProgressTracker) Increment() {
	processed := atomic.AddInt64(&pt.Processed, 1)
	
	// Print progress every 100 items or at completion
	if processed%100 == 0 || processed == pt.Total {
		elapsed := time.Since(pt.StartTime)
		rate := float64(processed) / elapsed.Seconds()
		percentage := float64(processed) / float64(pt.Total) * 100
		
		fmt.Printf("%s: %d/%d (%.1f%%) - %.1f items/sec\n", 
			pt.Name, processed, pt.Total, percentage, rate)
	}
}

// GetProgress returns the current progress
func (pt *ProgressTracker) GetProgress() (int64, int64, float64) {
	processed := atomic.LoadInt64(&pt.Processed)
	percentage := float64(processed) / float64(pt.Total) * 100
	return processed, pt.Total, percentage
}

// ParallelProcessor provides utilities for parallel processing
type ParallelProcessor struct {
	NumWorkers int
}

// NewParallelProcessor creates a new parallel processor
func NewParallelProcessor(numWorkers int) *ParallelProcessor {
	if numWorkers <= 0 {
		numWorkers = runtime.NumCPU()
	}
	
	return &ParallelProcessor{
		NumWorkers: numWorkers,
	}
}

// ProcessBatch processes a batch of items in parallel
func (pp *ParallelProcessor) ProcessBatch(items []interface{}, 
	workFunc func(interface{}) interface{}, 
	progressName string) ([]interface{}, error) {
	
	if len(items) == 0 {
		return []interface{}{}, nil
	}
	
	// Create progress tracker
	tracker := NewProgressTracker(int64(len(items)), progressName)
	
	// Create worker pool
	wp := NewWorkerPool(pp.NumWorkers, len(items), len(items))
	
	// Start workers with progress tracking
	wp.StartWorkers(func(job interface{}) interface{} {
		result := workFunc(job)
		tracker.Increment()
		return result
	})
	
	// Submit all jobs
	for _, item := range items {
		wp.SubmitJob(item)
	}
	
	// Close job queue to signal no more jobs
	close(wp.JobQueue)
	
	// Collect all results - we expect exactly len(items) results
	results := make([]interface{}, 0, len(items))
	for i := 0; i < len(items); i++ {
		result := <-wp.Results
		if result != nil {
			results = append(results, result)
		}
	}
	
	// Wait for all workers to finish
	wp.wg.Wait()
	
	// Close results channel
	close(wp.Results)
	
	fmt.Printf("%s: Completed processing %d items\n", progressName, len(results))
	return results, nil
}