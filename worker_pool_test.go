package work

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	workredis "github.com/GettEngineering/work/redis"
)

type tstCtx struct {
	bytes.Buffer
}

func (c *tstCtx) record(s string) {
	_, _ = c.WriteString(s)
}

var tstCtxType = reflect.TypeOf(tstCtx{})

func TestWorkerPoolHandlerValidations(t *testing.T) {
	cases := []struct {
		fn   interface{}
		good bool
	}{
		{func(_ *Job) error { return nil }, true},
		{func(_ *tstCtx, _ *Job) error { return nil }, true},
		{func(_ *tstCtx, _ *Job) {}, false},
		{func(_ *tstCtx, _ *Job) string { return "" }, false},
		{func(_ *tstCtx, _ *Job) (error, string) { return nil, "" }, false}, //nolint:stylecheck // it's a test case
		{func(_ *tstCtx) error { return nil }, false},
		{func(_ tstCtx, _ *Job) error { return nil }, false},
		{func() error { return nil }, false},
		{func(_ *tstCtx, _ *Job, _ string) error { return nil }, false},
	}

	for i, testCase := range cases {
		r := isValidHandlerType(tstCtxType, reflect.ValueOf(testCase.fn))
		if testCase.good != r {
			t.Errorf("idx %d: should return %v but returned %v", i, testCase.good, r)
		}
	}
}

func TestWorkerPoolMiddlewareValidations(t *testing.T) {
	cases := []struct {
		fn   interface{}
		good bool
	}{
		{func(_ *Job, _ NextMiddlewareFunc) error { return nil }, true},
		{func(_ *tstCtx, _ *Job, _ NextMiddlewareFunc) error { return nil }, true},
		{func(_ *tstCtx, _ *Job) error { return nil }, false},
		{func(_ *tstCtx, _ *Job, _ NextMiddlewareFunc) {}, false},
		{func(_ *tstCtx, _ *Job, _ NextMiddlewareFunc) string { return "" }, false},
		{func(_ *tstCtx, _ *Job, _ NextMiddlewareFunc) (error, string) { return nil, "" }, false}, //nolint:stylecheck // it's a test case
		{func(_ *tstCtx, _ NextMiddlewareFunc) error { return nil }, false},
		{func(_ tstCtx, _ *Job, _ NextMiddlewareFunc) error { return nil }, false},
		{func() error { return nil }, false},
		{func(_ *tstCtx, _ *Job, _ string) error { return nil }, false},
		{func(_ *tstCtx, _ *Job, _ NextMiddlewareFunc, _ string) error { return nil }, false},
	}

	for i, testCase := range cases {
		r := isValidMiddlewareType(tstCtxType, reflect.ValueOf(testCase.fn))
		if testCase.good != r {
			t.Errorf("idx %d: should return %v but returned %v", i, testCase.good, r)
		}
	}
}

func TestWorkerPoolStartStop(_ *testing.T) {
	// TODO: something should be tested here
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	wp := NewWorkerPool(TestContext{}, 10, ns, redisAdapter)
	wp.Start()
	wp.Start()
	wp.Stop()
	wp.Stop()
	wp.Start()
	wp.Stop()
}

func TestWorkerPoolValidations(t *testing.T) {
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	wp := NewWorkerPool(TestContext{}, 10, ns, redisAdapter)

	func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				assert.Regexp(t, "Your middleware function can have one of these signatures", fmt.Sprintf("%v", panicErr))
			} else {
				t.Errorf("expected a panic when using bad middleware")
			}
		}()

		wp.Middleware(TestWorkerPoolValidations)
	}()

	func() {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				assert.Regexp(t, "Your handler function can have one of these signatures", fmt.Sprintf("%v", panicErr))
			} else {
				t.Errorf("expected a panic when using a bad handler")
			}
		}()

		wp.Job("wat", TestWorkerPoolValidations)
	}()
}

func TestWorkersPoolRunSingleThreaded(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	job1 := "job1"
	numJobs, concurrency, sleepTime := 5, 5, 2
	wp := setupTestWorkerPool(ctx, redisAdapter, ns, job1, concurrency, JobOptions{Priority: 1, MaxConcurrency: 1})
	wp.Start()
	// enqueue some jobs
	enqueuer := NewEnqueuer(ns, redisAdapter)
	for i := 0; i < numJobs; i++ {
		_, err := enqueuer.Enqueue(job1, Q{"sleep": sleepTime})
		assert.Nil(t, err)
	}

	// make sure we've enough jobs queued up to make an interesting test
	jobsQueued := listSize(ctx, redisAdapter, redisKeyJobs(ns, job1))
	assert.True(t, jobsQueued >= 3, "should be at least 3 jobs queued up, but only found %v", jobsQueued)

	// now make sure the during the duration of job execution there is never > 1 job in flight
	start := time.Now()
	totalRuntime := time.Duration(sleepTime*numJobs) * time.Millisecond
	time.Sleep(10 * time.Millisecond)

	for time.Since(start) < totalRuntime {
		// jobs in progress, lock count for the job and lock info for the pool should never exceed 1
		jobsInProgress := listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, wp.workerPoolID, job1))
		assert.True(t, jobsInProgress <= 1, "jobsInProgress should never exceed 1: actual=%d", jobsInProgress)

		jobLockCount := getInt64(ctx, redisAdapter, redisKeyJobsLock(ns, job1))
		assert.True(t, jobLockCount <= 1, "global lock count for job should never exceed 1, got: %v", jobLockCount)
		wpLockCount := hgetInt64(ctx, redisAdapter, redisKeyJobsLockInfo(ns, job1), wp.workerPoolID)
		assert.True(t, wpLockCount <= 1, "lock count for the worker pool should never exceed 1: actual=%v", wpLockCount)
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
	wp.Drain()
	wp.Stop()

	// At this point it should all be empty.
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, wp.workerPoolID, job1)))
	assert.EqualValues(t, 0, getInt64(ctx, redisAdapter, redisKeyJobsLock(ns, job1)))
	assert.EqualValues(t, 0, hgetInt64(ctx, redisAdapter, redisKeyJobsLockInfo(ns, job1), wp.workerPoolID))
}

func TestWorkerPoolPauseSingleThreadedJobs(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns, job1 := "work", "job1"
	numJobs, concurrency, sleepTime := 5, 5, 2
	wp := setupTestWorkerPool(ctx, redisAdapter, ns, job1, concurrency, JobOptions{Priority: 1, MaxConcurrency: 1})
	wp.Start()
	// enqueue some jobs
	enqueuer := NewEnqueuer(ns, redisAdapter)
	for i := 0; i < numJobs; i++ {
		_, err := enqueuer.Enqueue(job1, Q{"sleep": sleepTime})
		assert.Nil(t, err)
	}
	// provide time for jobs to process
	time.Sleep(10 * time.Millisecond)

	// pause work, provide time for outstanding jobs to finish and queue up another job
	err := pauseJobs(ctx, ns, job1, redisAdapter)
	assert.NoError(t, err)
	time.Sleep(2 * time.Millisecond)
	_, err = enqueuer.Enqueue(job1, Q{"sleep": sleepTime})
	assert.Nil(t, err)

	// check that we still have some jobs to process
	assert.True(t, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)) >= 1)

	// now make sure no jobs get started until we unpause
	start := time.Now()
	totalRuntime := time.Duration(sleepTime*numJobs) * time.Millisecond

	for time.Since(start) < totalRuntime {
		assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, wp.workerPoolID, job1)))
		// lock count for the job and lock info for the pool should both be at 1 while job is running
		assert.EqualValues(t, 0, getInt64(ctx, redisAdapter, redisKeyJobsLock(ns, job1)))
		assert.EqualValues(t, 0, hgetInt64(ctx, redisAdapter, redisKeyJobsLockInfo(ns, job1), wp.workerPoolID))
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}

	// unpause work and get past the backoff time
	err = unpauseJobs(ctx, ns, job1, redisAdapter)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	wp.Drain()
	wp.Stop()

	// At this point it should all be empty.
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, wp.workerPoolID, job1)))
	assert.EqualValues(t, 0, getInt64(ctx, redisAdapter, redisKeyJobsLock(ns, job1)))
	assert.EqualValues(t, 0, hgetInt64(ctx, redisAdapter, redisKeyJobsLockInfo(ns, job1), wp.workerPoolID))
}

//
// Test helpers.
//

func (t *TestContext) SleepyJob(job *Job) error {
	sleepTime := time.Duration(job.ArgInt64("sleep"))
	time.Sleep(sleepTime * time.Millisecond)
	return nil
}

func setupTestWorkerPool(
	ctx context.Context,
	redisAdapter workredis.Redis,
	namespace, jobName string,
	concurrency int,
	jobOpts JobOptions,
) *WorkerPool {
	deleteQueue(ctx, redisAdapter, namespace, jobName)
	deleteRetryAndDead(ctx, redisAdapter, namespace)
	deletePausedAndLockedKeys(ctx, namespace, jobName, redisAdapter)

	wp := NewWorkerPool(TestContext{}, uint(concurrency), namespace, redisAdapter)
	wp.JobWithOptions(jobName, jobOpts, (*TestContext).SleepyJob)
	// reset the backoff times to help with testing
	sleepBackoffsInMilliseconds = []int64{10, 10, 10, 10, 10}
	return wp
}
