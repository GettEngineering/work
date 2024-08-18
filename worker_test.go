package work

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	goredisv8 "github.com/go-redis/redis/v8"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"

	workredis "github.com/GettEngineering/work/redis"
	goredisv8adapter "github.com/GettEngineering/work/redis/adapters/goredisv8"
	redigoadapter "github.com/GettEngineering/work/redis/adapters/redigo"
)

func TestWorkerBasics(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	job1 := "job1"
	job2 := "job2"
	job3 := "job3"

	cleanKeyspace(ctx, ns, redisAdapter)

	var arg1 float64
	var arg2 float64
	var arg3 float64

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			var ok bool

			arg1, ok = job.Args["a"].(float64)
			assert.True(t, ok)

			return nil
		},
	}
	jobTypes[job2] = &jobType{
		Name:       job2,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			var ok bool

			arg2, ok = job.Args["a"].(float64)
			assert.True(t, ok)

			return nil
		},
	}
	jobTypes[job3] = &jobType{
		Name:       job3,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(job *Job) error {
			var ok bool

			arg3, ok = job.Args["a"].(float64)
			assert.True(t, ok)

			return nil
		},
	}

	enqueuer := NewEnqueuer(ns, redisAdapter)
	_, err := enqueuer.Enqueue(job1, Q{"a": 1})
	assert.Nil(t, err)
	_, err = enqueuer.Enqueue(job2, Q{"a": 2})
	assert.Nil(t, err)
	_, err = enqueuer.Enqueue(job3, Q{"a": 3})
	assert.Nil(t, err)

	w := newWorker(ns, "1", redisAdapter, tstCtxType, jobTypes, nil)
	w.start()
	w.drain()
	w.stop()

	// make sure the jobs ran (side effect of setting these variables to the job arguments)
	assert.EqualValues(t, 1.0, arg1)
	assert.EqualValues(t, 2.0, arg2)
	assert.EqualValues(t, 3.0, arg3)

	// nothing in retries or dead
	assert.EqualValues(t, 0, zsetSize(ctx, redisAdapter, redisKeyRetry(ns)))
	assert.EqualValues(t, 0, zsetSize(ctx, redisAdapter, redisKeyDead(ns)))

	// Nothing in the queues or in-progress queues
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job2)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job3)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, "1", job1)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, "1", job2)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, "1", job3)))

	// nothing in the worker status
	h := readHash(ctx, redisAdapter, redisKeyWorkerObservation(ns, w.workerID))
	assert.EqualValues(t, 0, len(h))
}

func TestWorkerInProgress(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	job1 := "job1"
	deleteQueue(ctx, redisAdapter, ns, job1)
	deleteRetryAndDead(ctx, redisAdapter, ns)
	deletePausedAndLockedKeys(ctx, ns, job1, redisAdapter)

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(_ *Job) error {
			time.Sleep(30 * time.Millisecond)
			return nil
		},
	}

	enqueuer := NewEnqueuer(ns, redisAdapter)
	_, err := enqueuer.Enqueue(job1, Q{"a": 1})
	assert.Nil(t, err)

	w := newWorker(ns, "1", redisAdapter, tstCtxType, jobTypes, nil)
	w.start()

	// instead of w.forceIter(), we'll wait for 10 milliseconds to let the job start
	// The job will then sleep for 30ms. In that time, we should be able to see something in the in-progress queue.
	time.Sleep(10 * time.Millisecond)
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 1, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, "1", job1)))
	assert.EqualValues(t, 1, getInt64(ctx, redisAdapter, redisKeyJobsLock(ns, job1)))
	assert.EqualValues(t, 1, hgetInt64(ctx, redisAdapter, redisKeyJobsLockInfo(ns, job1), w.poolID))

	// nothing in the worker status
	w.observer.drain()
	h := readHash(ctx, redisAdapter, redisKeyWorkerObservation(ns, w.workerID))
	assert.Equal(t, job1, h["job_name"])
	assert.Equal(t, `{"a":1}`, h["args"])
	// NOTE: we could check for job_id and started_at, but it's a PITA and it's tested in observer_test.

	w.drain()
	w.stop()

	// At this point, it should all be empty.
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, "1", job1)))

	// nothing in the worker status
	h = readHash(ctx, redisAdapter, redisKeyWorkerObservation(ns, w.workerID))
	assert.EqualValues(t, 0, len(h))
}

func TestWorkerRetry(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	job1 := "job1"
	deleteQueue(ctx, redisAdapter, ns, job1)
	deleteRetryAndDead(ctx, redisAdapter, ns)
	deletePausedAndLockedKeys(ctx, ns, job1, redisAdapter)

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1, MaxFails: 3},
		IsGeneric:  true,
		GenericHandler: func(_ *Job) error {
			return fmt.Errorf("sorry kid")
		},
	}

	enqueuer := NewEnqueuer(ns, redisAdapter)
	_, err := enqueuer.Enqueue(job1, Q{"a": 1})
	assert.Nil(t, err)
	w := newWorker(ns, "1", redisAdapter, tstCtxType, jobTypes, nil)
	w.start()
	w.drain()
	w.stop()

	// Ensure the right stuff is in our queues:
	assert.EqualValues(t, 1, zsetSize(ctx, redisAdapter, redisKeyRetry(ns)))
	assert.EqualValues(t, 0, zsetSize(ctx, redisAdapter, redisKeyDead(ns)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, "1", job1)))
	assert.EqualValues(t, 0, getInt64(ctx, redisAdapter, redisKeyJobsLock(ns, job1)))
	assert.EqualValues(t, 0, hgetInt64(ctx, redisAdapter, redisKeyJobsLockInfo(ns, job1), w.poolID))

	// Get the job on the retry queue
	ts, job := jobOnZset(ctx, redisAdapter, redisKeyRetry(ns))

	assert.True(t, ts > nowEpochSeconds())      // enqueued in the future
	assert.True(t, ts < (nowEpochSeconds()+80)) // but less than a minute from now (first failure)

	assert.Equal(t, job1, job.Name) // basics are preserved
	assert.EqualValues(t, 1, job.Fails)
	assert.Equal(t, "sorry kid", job.LastErr)
	assert.True(t, (nowEpochSeconds()-job.FailedAt) <= 2)
}

// Check if a custom backoff function functions functionally.
func TestWorkerRetryWithCustomBackoff(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	job1 := "job1"
	deleteQueue(ctx, redisAdapter, ns, job1)
	deleteRetryAndDead(ctx, redisAdapter, ns)
	calledCustom := 0

	custombo := func(_ *Job) int64 {
		calledCustom++
		return 5 // Always 5 seconds
	}

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1, MaxFails: 3, Backoff: custombo},
		IsGeneric:  true,
		GenericHandler: func(_ *Job) error {
			return fmt.Errorf("sorry kid")
		},
	}

	enqueuer := NewEnqueuer(ns, redisAdapter)
	_, err := enqueuer.Enqueue(job1, Q{"a": 1})
	assert.Nil(t, err)
	w := newWorker(ns, "1", redisAdapter, tstCtxType, jobTypes, nil)
	w.start()
	w.drain()
	w.stop()

	// Ensure the right stuff is in our queues:
	assert.EqualValues(t, 1, zsetSize(ctx, redisAdapter, redisKeyRetry(ns)))
	assert.EqualValues(t, 0, zsetSize(ctx, redisAdapter, redisKeyDead(ns)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, "1", job1)))

	// Get the job on the retry queue
	ts, job := jobOnZset(ctx, redisAdapter, redisKeyRetry(ns))

	assert.True(t, ts > nowEpochSeconds())      // enqueued in the future
	assert.True(t, ts < (nowEpochSeconds()+10)) // but less than ten secs in

	assert.Equal(t, job1, job.Name) // basics are preserved
	assert.EqualValues(t, 1, job.Fails)
	assert.Equal(t, "sorry kid", job.LastErr)
	assert.True(t, (nowEpochSeconds()-job.FailedAt) <= 2)
	assert.Equal(t, 1, calledCustom)
}

func TestWorkerDead(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	job1 := "job1"
	job2 := "job2"
	deleteQueue(ctx, redisAdapter, ns, job1)
	deleteQueue(ctx, redisAdapter, ns, job2)
	deleteRetryAndDead(ctx, redisAdapter, ns)
	deletePausedAndLockedKeys(ctx, ns, job1, redisAdapter)

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1, MaxFails: 0},
		IsGeneric:  true,
		GenericHandler: func(_ *Job) error {
			return fmt.Errorf("sorry kid1")
		},
	}
	jobTypes[job2] = &jobType{
		Name:       job2,
		JobOptions: JobOptions{Priority: 1, MaxFails: 0, SkipDead: true},
		IsGeneric:  true,
		GenericHandler: func(_ *Job) error {
			return fmt.Errorf("sorry kid2")
		},
	}

	enqueuer := NewEnqueuer(ns, redisAdapter)
	_, err := enqueuer.Enqueue(job1, nil)
	assert.Nil(t, err)
	_, err = enqueuer.Enqueue(job2, nil)
	assert.Nil(t, err)
	w := newWorker(ns, "1", redisAdapter, tstCtxType, jobTypes, nil)
	w.start()
	w.drain()
	w.stop()

	// Ensure the right stuff is in our queues:
	assert.EqualValues(t, 0, zsetSize(ctx, redisAdapter, redisKeyRetry(ns)))
	assert.EqualValues(t, 1, zsetSize(ctx, redisAdapter, redisKeyDead(ns)))

	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, "1", job1)))
	assert.EqualValues(t, 0, getInt64(ctx, redisAdapter, redisKeyJobsLock(ns, job1)))
	assert.EqualValues(t, 0, hgetInt64(ctx, redisAdapter, redisKeyJobsLockInfo(ns, job1), w.poolID))

	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job2)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, "1", job2)))
	assert.EqualValues(t, 0, getInt64(ctx, redisAdapter, redisKeyJobsLock(ns, job2)))
	assert.EqualValues(t, 0, hgetInt64(ctx, redisAdapter, redisKeyJobsLockInfo(ns, job2), w.poolID))

	// Get the job on the dead queue
	ts, job := jobOnZset(ctx, redisAdapter, redisKeyDead(ns))

	assert.True(t, ts <= nowEpochSeconds())

	assert.Equal(t, job1, job.Name) // basics are preserved
	assert.EqualValues(t, 1, job.Fails)
	assert.Equal(t, "sorry kid1", job.LastErr)
	assert.True(t, (nowEpochSeconds()-job.FailedAt) <= 2)
}

func TestWorkersPaused(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	job1 := "job1"
	deleteQueue(ctx, redisAdapter, ns, job1)
	deleteRetryAndDead(ctx, redisAdapter, ns)
	deletePausedAndLockedKeys(ctx, ns, job1, redisAdapter)

	jobTypes := make(map[string]*jobType)
	jobTypes[job1] = &jobType{
		Name:       job1,
		JobOptions: JobOptions{Priority: 1},
		IsGeneric:  true,
		GenericHandler: func(_ *Job) error {
			time.Sleep(30 * time.Millisecond)
			return nil
		},
	}

	enqueuer := NewEnqueuer(ns, redisAdapter)
	_, err := enqueuer.Enqueue(job1, Q{"a": 1})
	assert.Nil(t, err)

	w := newWorker(ns, "1", redisAdapter, tstCtxType, jobTypes, nil)
	// pause the jobs prior to starting
	err = pauseJobs(ctx, ns, job1, redisAdapter)
	assert.Nil(t, err)
	// reset the backoff times to help with testing
	sleepBackoffsInMilliseconds = []int64{10, 10, 10, 10, 10}
	w.start()

	// make sure the jobs stay in the still in the run queue and not moved to in progress
	for i := 0; i < 2; i++ {
		time.Sleep(10 * time.Millisecond)
		assert.EqualValues(t, 1, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)))
		assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, "1", job1)))
	}

	// now unpause the jobs and check that they start
	err = unpauseJobs(ctx, ns, job1, redisAdapter)
	assert.Nil(t, err)
	// sleep through 2 backoffs to make sure we allow enough time to start running
	time.Sleep(20 * time.Millisecond)
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 1, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, "1", job1)))

	w.observer.drain()
	h := readHash(ctx, redisAdapter, redisKeyWorkerObservation(ns, w.workerID))
	assert.Equal(t, job1, h["job_name"])
	assert.Equal(t, `{"a":1}`, h["args"])
	w.drain()
	w.stop()

	// At this point, it should all be empty.
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, "1", job1)))

	// nothing in the worker status
	h = readHash(ctx, redisAdapter, redisKeyWorkerObservation(ns, w.workerID))
	assert.EqualValues(t, 0, len(h))
}

// Test that in the case of an unavailable Redis server,
// the worker loop exits in the case of a WorkerPool.Stop.
func TestStop(_ *testing.T) {
	// TODO: something should be tested here
	redisAdapter := newTestBrokenRedis()
	wp := NewWorkerPool(TestContext{}, 10, "work", redisAdapter)
	wp.Start()
	wp.Stop()
}

func BenchmarkJobProcessing(b *testing.B) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	cleanKeyspace(ctx, ns, redisAdapter)
	enqueuer := NewEnqueuer(ns, redisAdapter)

	for i := 0; i < b.N; i++ {
		_, err := enqueuer.Enqueue("wat", nil)
		if err != nil {
			panic(err)
		}
	}

	wp := NewWorkerPool(TestContext{}, 10, ns, redisAdapter)
	wp.Job("wat", func(_ *TestContext, _ *Job) error {
		return nil
	})

	b.ResetTimer()

	wp.Start()
	wp.Drain()
	wp.Stop()
}

func newTestRedis(addr string) workredis.Redis {
	switch os.Getenv("TEST_REDIS_ADAPTER") {
	case "goredisv8":
		rdb := goredisv8.NewClient(&goredisv8.Options{
			Addr:     addr,
			Password: "",
			DB:       0,
		})
		return goredisv8adapter.NewAdapter(rdb)
	default:
		pool := &redis.Pool{
			MaxActive:   10,
			MaxIdle:     10,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", addr)
				if err != nil {
					return nil, err
				}
				return c, nil
			},
			Wait: true,
		}
		return redigoadapter.NewAdapter(pool)
	}
}

func newTestBrokenRedis() workredis.Redis {
	const addr = "notworking-redis:6379"

	switch os.Getenv("TEST_REDIS_ADAPTER") {
	case "goredisv8":
		rdb := goredisv8.NewClient(&goredisv8.Options{
			Addr:     addr,
			Password: "",
			DB:       0,
		})
		return goredisv8adapter.NewAdapter(rdb)
	default:
		pool := &redis.Pool{
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", addr, redis.DialConnectTimeout(1*time.Second))
				if err != nil {
					return nil, err
				}
				return c, nil
			},
		}
		return redigoadapter.NewAdapter(pool)
	}
}

func deleteQueue(ctx context.Context, redisAdapter workredis.Redis, namespace, jobName string) {
	err := redisAdapter.Del(ctx, redisKeyJobs(namespace, jobName), redisKeyJobsInProgress(namespace, "1", jobName))
	if err != nil {
		panic("could not delete queue: " + err.Error())
	}
}

func deleteRetryAndDead(ctx context.Context, redisAdapter workredis.Redis, namespace string) {
	err := redisAdapter.Del(ctx, redisKeyRetry(namespace), redisKeyDead(namespace))
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}
}

func zsetSize(ctx context.Context, redisAdapter workredis.Redis, key string) int64 {
	v, err := redisAdapter.ZCard(ctx, key)
	if err != nil {
		panic("could not get ZSET size: " + err.Error())
	}
	return v
}

func listSize(ctx context.Context, redisAdapter workredis.Redis, key string) int64 {
	v, err := redisAdapter.LLen(ctx, key).Result()
	if err != nil {
		panic("could not get list length: " + err.Error())
	}
	return v
}

func getInt64(ctx context.Context, redisAdapter workredis.Redis, key string) int64 {
	v, err := redisAdapter.Get(ctx, key).Int64()
	if err != nil {
		panic("could not GET int64: " + err.Error())
	}
	return v
}

func exists(ctx context.Context, redisAdapter workredis.Redis, key string) bool {
	v, err := redisAdapter.Exists(ctx, key)
	if err != nil {
		panic("could not EXISTS: " + err.Error())
	}
	return v
}

func hgetInt64(ctx context.Context, redisAdapter workredis.Redis, redisKey, hashKey string) int64 {
	v, err := redisAdapter.HGet(ctx, redisKey, hashKey).Int64()
	if err != nil {
		panic("could not HGET int64: " + err.Error())
	}
	return v
}

func jobOnZset(ctx context.Context, redisAdapter workredis.Redis, key string) (int64, *Job) {
	r, err := redisAdapter.ZRangeWithScores(ctx, key, 0, 0)
	if err != nil {
		panic("ZRANGE error: " + err.Error())
	}

	if r.Next() {
		vv, score := r.Val()

		var member []byte

		switch vv := vv.(type) {
		case []byte:
			member = vv
		case string:
			member = []byte(vv)
		default:
			panic("expect member []byte or string")
		}

		job, err := newJob(member, nil, nil)
		if err != nil {
			panic("couldn't get job: " + err.Error())
		}

		return int64(score), job
	}

	panic("couldn't get job")
}

func jobOnQueue(ctx context.Context, redisAdapter workredis.Redis, key string) *Job {
	rawJSON, err := redisAdapter.RPop(ctx, key).Bytes()
	if err != nil {
		panic("could RPOP from job queue: " + err.Error())
	}

	job, err := newJob(rawJSON, nil, nil)
	if err != nil {
		panic("couldn't get job: " + err.Error())
	}

	return job
}

func knownJobs(ctx context.Context, redisAdapter workredis.Redis, key string) []string {
	jobNames, err := redisAdapter.SMembers(ctx, key)
	if err != nil {
		panic(err)
	}
	return jobNames
}

func cleanKeyspace(ctx context.Context, namespace string, redisAdapter workredis.Redis) {
	keys, err := redisAdapter.Keys(ctx, namespace+"*")
	if err != nil {
		panic("could not get keys: " + err.Error())
	}
	for _, k := range keys {
		if err := redisAdapter.Del(ctx, k); err != nil {
			panic("could not del: " + err.Error())
		}
	}
}

func pauseJobs(ctx context.Context, namespace, jobName string, redisAdapter workredis.Redis) error {
	if err := redisAdapter.Set(ctx, redisKeyJobsPaused(namespace, jobName), "1"); err != nil {
		return err
	}
	return nil
}

func unpauseJobs(ctx context.Context, namespace, jobName string, redisAdapter workredis.Redis) error {
	if err := redisAdapter.Del(ctx, redisKeyJobsPaused(namespace, jobName)); err != nil {
		return err
	}
	return nil
}

func deletePausedAndLockedKeys(ctx context.Context, namespace, jobName string, redisAdapter workredis.Redis) {
	if err := redisAdapter.Del(ctx, redisKeyJobsPaused(namespace, jobName)); err != nil {
		panic("could not delete paused key: " + err.Error())
	}
	if err := redisAdapter.Del(ctx, redisKeyJobsLock(namespace, jobName)); err != nil {
		panic("could not delete lock key: " + err.Error())
	}
	if err := redisAdapter.Del(ctx, redisKeyJobsLockInfo(namespace, jobName)); err != nil {
		panic("could not delete lock info key: " + err.Error())
	}
}

type emptyCtx struct{}

// Starts up a pool with two workers emptying it as fast as they can
// The pool is Stop()ped while jobs are still going on.  Tests that the
// pool processing is really stopped and that it's not first completely
// drained before returning.
// https://github.com/gocraft/work/issues/24
func TestWorkerPoolStop(t *testing.T) {
	ns := "will_it_end"
	redisAdapter := newTestRedis(":6379")
	numIters := 30

	var started, stopped int32

	wp := NewWorkerPool(emptyCtx{}, 2, ns, redisAdapter)

	wp.Job("sample_job", func(_ *emptyCtx, _ *Job) error {
		atomic.AddInt32(&started, 1)
		time.Sleep(1 * time.Second)
		atomic.AddInt32(&stopped, 1)
		return nil
	})

	enqueuer := NewEnqueuer(ns, redisAdapter)

	for i := 0; i <= numIters; i++ {
		_, err := enqueuer.Enqueue("sample_job", Q{})
		if err != nil {
			t.Fatalf("Error enqueuing job: %s", err)
		}
	}

	// Start the pool and quit before it has had a chance to complete
	// all the jobs.
	wp.Start()
	time.Sleep(5 * time.Second)
	wp.Stop()

	if started != stopped {
		t.Errorf("Expected that jobs were finished and not killed while processing (started=%d, stopped=%d)", started, stopped)
	}

	if started >= int32(numIters) {
		t.Errorf("Expected that jobs queue was not completely emptied.")
	}
}
