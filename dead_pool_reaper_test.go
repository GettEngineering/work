package work

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/GettEngineering/work/redis"
)

func TestDeadPoolReaper(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	cleanKeyspace(ctx, ns, redisAdapter)

	workerPoolsKey := redisKeyWorkerPools(ns)

	// Create redis data.
	err := redisAdapter.WithPipeline(ctx, func(r redis.Redis) error {
		rerr := r.SAdd(ctx, workerPoolsKey, "1")
		assert.NoError(t, rerr)
		rerr = r.SAdd(ctx, workerPoolsKey, "2")
		assert.NoError(t, rerr)
		rerr = r.SAdd(ctx, workerPoolsKey, "3")
		assert.NoError(t, rerr)

		rerr = r.HSet(ctx, redisKeyHeartbeat(ns, "1"),
			"heartbeat_at", time.Now().Unix(),
			"job_names", "type1,type2",
		)
		assert.NoError(t, rerr)

		rerr = r.HSet(ctx, redisKeyHeartbeat(ns, "2"),
			"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
			"job_names", "type1,type2",
		)
		assert.NoError(t, rerr)

		rerr = r.HSet(ctx, redisKeyHeartbeat(ns, "3"),
			"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
			"job_names", "type1,type2",
		)
		assert.NoError(t, rerr)

		return nil
	})
	assert.NoError(t, err)

	// Test getting dead pool
	reaper := newDeadPoolReaper(ns, redisAdapter, []string{})
	deadPools, err := reaper.findDeadPools(ctx)
	assert.NoError(t, err)
	assert.Equal(t, map[string][]string{"2": {"type1", "type2"}, "3": {"type1", "type2"}}, deadPools)

	// Test requeueing jobs
	err = redisAdapter.LPush(ctx, redisKeyJobsInProgress(ns, "2", "type1"), "foo")
	assert.NoError(t, err)
	err = redisAdapter.Incr(ctx, redisKeyJobsLock(ns, "type1"))
	assert.NoError(t, err)
	err = redisAdapter.HIncrBy(ctx, redisKeyJobsLockInfo(ns, "type1"), "2", 1) // worker pool 2 has lock
	assert.NoError(t, err)

	// Ensure 0 jobs in jobs queue
	jobsCount, err := redisAdapter.LLen(ctx, redisKeyJobs(ns, "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)

	// Ensure 1 job in inprogress queue
	jobsCount, err = redisAdapter.LLen(ctx, redisKeyJobsInProgress(ns, "2", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Reap
	err = reaper.reap(ctx)
	assert.NoError(t, err)

	// Ensure 1 jobs in jobs queue
	jobsCount, err = redisAdapter.LLen(ctx, redisKeyJobs(ns, "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Ensure 0 job in inprogress queue
	jobsCount, err = redisAdapter.LLen(ctx, redisKeyJobsInProgress(ns, "2", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)

	// Locks should get cleaned up
	assert.EqualValues(t, 0, getInt64(ctx, redisAdapter, redisKeyJobsLock(ns, "type1")))
	_, err = redisAdapter.HGet(ctx, redisKeyJobsLockInfo(ns, "type1"), "2").Int64()
	assert.ErrorIs(t, err, redis.Nil)
}

func TestDeadPoolReaperNoHeartbeat(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	cleanKeyspace(ctx, ns, redisAdapter)

	workerPoolsKey := redisKeyWorkerPools(ns)

	// Create redis data.
	err := redisAdapter.WithPipeline(ctx, func(r redis.Redis) error {
		rerr := r.SAdd(ctx, workerPoolsKey, "1")
		assert.NoError(t, rerr)
		rerr = r.SAdd(ctx, workerPoolsKey, "2")
		assert.NoError(t, rerr)
		rerr = r.SAdd(ctx, workerPoolsKey, "3")
		assert.NoError(t, rerr)
		// stale lock info
		rerr = r.Set(ctx, redisKeyJobsLock(ns, "type1"), 3)
		assert.NoError(t, rerr)
		rerr = r.HSet(ctx, redisKeyJobsLockInfo(ns, "type1"), "1", 1)
		assert.NoError(t, rerr)
		rerr = r.HSet(ctx, redisKeyJobsLockInfo(ns, "type1"), "2", 1)
		assert.NoError(t, rerr)
		rerr = r.HSet(ctx, redisKeyJobsLockInfo(ns, "type1"), "3", 1)
		assert.NoError(t, rerr)

		return nil
	})
	assert.NoError(t, err)

	// make sure test data was created
	numPools, err := redisAdapter.SCard(ctx, workerPoolsKey)
	assert.NoError(t, err)
	assert.EqualValues(t, int64(3), numPools)

	// Test getting dead pool ids
	reaper := newDeadPoolReaper(ns, redisAdapter, []string{"type1"})
	deadPools, err := reaper.findDeadPools(ctx)
	assert.NoError(t, err)
	assert.Equal(t, map[string][]string{"1": {}, "2": {}, "3": {}}, deadPools)

	// Test requeueing jobs
	err = redisAdapter.LPush(ctx, redisKeyJobsInProgress(ns, "2", "type1"), "foo")
	assert.NoError(t, err)

	// Ensure 0 jobs in jobs queue
	jobsCount, err := redisAdapter.LLen(ctx, redisKeyJobs(ns, "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)

	// Ensure 1 job in inprogress queue
	jobsCount, err = redisAdapter.LLen(ctx, redisKeyJobsInProgress(ns, "2", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Ensure dead worker pools still in the set
	jobsCount, err = redisAdapter.SCard(ctx, redisKeyWorkerPools(ns))
	assert.NoError(t, err)
	assert.Equal(t, int64(3), jobsCount)

	// Reap
	err = reaper.reap(ctx)
	assert.NoError(t, err)

	// Ensure jobs queue was not altered
	jobsCount, err = redisAdapter.LLen(ctx, redisKeyJobs(ns, "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)

	// Ensure inprogress queue was not altered
	jobsCount, err = redisAdapter.LLen(ctx, redisKeyJobsInProgress(ns, "2", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Ensure dead worker pools were removed from the set
	jobsCount, err = redisAdapter.SCard(ctx, redisKeyWorkerPools(ns))
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)

	// Stale lock info was cleaned up using reap.curJobTypes
	assert.EqualValues(t, 0, getInt64(ctx, redisAdapter, redisKeyJobsLock(ns, "type1")))
	for _, poolID := range []string{"1", "2", "3"} {
		_, err = redisAdapter.HGet(ctx, redisKeyJobsLockInfo(ns, "type1"), poolID).Int64()
		assert.ErrorIs(t, err, redis.Nil)
	}
}

func TestDeadPoolReaperNoJobTypes(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	cleanKeyspace(ctx, ns, redisAdapter)

	workerPoolsKey := redisKeyWorkerPools(ns)

	// Create redis data
	err := redisAdapter.WithPipeline(ctx, func(r redis.Redis) error {
		rerr := r.SAdd(ctx, workerPoolsKey, "1")
		assert.NoError(t, rerr)
		rerr = r.SAdd(ctx, workerPoolsKey, "2")
		assert.NoError(t, rerr)

		rerr = r.HSet(ctx, redisKeyHeartbeat(ns, "1"),
			"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
		)
		assert.NoError(t, rerr)

		rerr = r.HSet(ctx, redisKeyHeartbeat(ns, "2"),
			"heartbeat_at", time.Now().Add(-1*time.Hour).Unix(),
			"job_names", "type1,type2",
		)
		assert.NoError(t, rerr)

		return nil
	})
	assert.NoError(t, err)

	// Test getting dead pool
	reaper := newDeadPoolReaper(ns, redisAdapter, []string{})
	deadPools, err := reaper.findDeadPools(ctx)
	assert.NoError(t, err)
	assert.Equal(t, map[string][]string{"2": {"type1", "type2"}}, deadPools)

	// Test requeueing jobs
	err = redisAdapter.LPush(ctx, redisKeyJobsInProgress(ns, "1", "type1"), "foo")
	assert.NoError(t, err)
	err = redisAdapter.LPush(ctx, redisKeyJobsInProgress(ns, "2", "type1"), "foo")
	assert.NoError(t, err)

	// Ensure 0 jobs in jobs queue
	jobsCount, err := redisAdapter.LLen(ctx, redisKeyJobs(ns, "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)

	// Ensure 1 job in inprogress queue for each job
	jobsCount, err = redisAdapter.LLen(ctx, redisKeyJobsInProgress(ns, "1", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)
	jobsCount, err = redisAdapter.LLen(ctx, redisKeyJobsInProgress(ns, "2", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Reap. Ensure job 2 is requeued but not job 1
	err = reaper.reap(ctx)
	assert.NoError(t, err)

	// Ensure 1 jobs in jobs queue
	jobsCount, err = redisAdapter.LLen(ctx, redisKeyJobs(ns, "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Ensure 1 job in inprogress queue for 1
	jobsCount, err = redisAdapter.LLen(ctx, redisKeyJobsInProgress(ns, "1", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), jobsCount)

	// Ensure 0 jobs in inprogress queue for 2
	jobsCount, err = redisAdapter.LLen(ctx, redisKeyJobsInProgress(ns, "2", "type1")).Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), jobsCount)
}

func TestDeadPoolReaperWithWorkerPools(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	job1 := "job1"
	stalePoolID := "aaa"
	cleanKeyspace(ctx, ns, redisAdapter)

	// test vars
	expectedDeadTime := 5 * time.Millisecond

	// create a stale job with a heartbeat
	err := redisAdapter.SAdd(ctx, redisKeyWorkerPools(ns), stalePoolID)
	assert.NoError(t, err)
	err = redisAdapter.LPush(ctx, redisKeyJobsInProgress(ns, stalePoolID, job1), `{"sleep": 10}`)
	assert.NoError(t, err)
	jobTypes := map[string]*jobType{"job1": nil}
	staleHeart := newWorkerPoolHeartbeater(ns, redisAdapter, stalePoolID, jobTypes, 1, []string{"id1"})
	staleHeart.start()

	// should have 1 stale job and empty job queue
	assert.EqualValues(t, 1, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, stalePoolID, job1)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)))

	// setup a worker pool and start the reaper, which should restart the stale job above
	wp := setupTestWorkerPool(ctx, redisAdapter, ns, job1, 1, JobOptions{Priority: 1})
	wp.deadPoolReaper = newDeadPoolReaper(wp.namespace, redisAdapter, []string{"job1"})
	wp.deadPoolReaper.deadTime = expectedDeadTime
	wp.deadPoolReaper.start()

	// sleep long enough for staleJob to be considered dead
	time.Sleep(expectedDeadTime * 2)

	// now we should have 1 job in queue and no more stale jobs
	assert.EqualValues(t, 1, listSize(ctx, redisAdapter, redisKeyJobs(ns, job1)))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobsInProgress(ns, wp.workerPoolID, job1)))
	staleHeart.stop()
	wp.deadPoolReaper.stop()
}

func TestDeadPoolReaperCleanStaleLocks(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	cleanKeyspace(ctx, ns, redisAdapter)

	job1, job2 := "type1", "type2"
	jobNames := []string{job1, job2}
	workerPoolID1, workerPoolID2 := "1", "2"
	lock1 := redisKeyJobsLock(ns, job1)
	lock2 := redisKeyJobsLock(ns, job2)
	lockInfo1 := redisKeyJobsLockInfo(ns, job1)
	lockInfo2 := redisKeyJobsLockInfo(ns, job2)

	// Create redis data.
	err := redisAdapter.WithPipeline(ctx, func(r redis.Redis) error {
		rerr := r.Set(ctx, lock1, 3)
		assert.NoError(t, rerr)
		rerr = r.Set(ctx, lock2, 1)
		assert.NoError(t, rerr)
		rerr = r.HSet(ctx, lockInfo1, workerPoolID1, 1) // workerPoolID1 holds 1 lock on job1
		assert.NoError(t, rerr)
		rerr = r.HSet(ctx, lockInfo1, workerPoolID2, 2) // workerPoolID2 holds 2 locks on job1
		assert.NoError(t, rerr)
		rerr = r.HSet(ctx, lockInfo2, workerPoolID2, 2) // test that we go below 0 on job2 lock
		assert.NoError(t, rerr)

		return nil
	})
	assert.NoError(t, err)

	reaper := newDeadPoolReaper(ns, redisAdapter, jobNames)
	// clean lock info for workerPoolID1
	reaper.cleanStaleLockInfo(ctx, workerPoolID1, jobNames)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, getInt64(ctx, redisAdapter, lock1))      // job1 lock should be decr by 1
	assert.EqualValues(t, 1, getInt64(ctx, redisAdapter, lock2))      // job2 lock is unchanged
	_, err = redisAdapter.HGet(ctx, lockInfo1, workerPoolID1).Int64() // workerPoolID1 removed from job1's lock info
	assert.ErrorIs(t, err, redis.Nil)

	// now clean lock info for workerPoolID2
	err = reaper.cleanStaleLockInfo(ctx, workerPoolID2, jobNames)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, getInt64(ctx, redisAdapter, lock1))
	// lock should be able to go below 0 as
	// - reaper may requeue jobs from an alive worker
	// - when jobs finished the lock is increased back
	// - so it is fixed itself
	assert.EqualValues(t, -1, getInt64(ctx, redisAdapter, lock2))
	// worker pool ID 2 removed from both lock info hashes
	_, err = redisAdapter.HGet(ctx, lockInfo1, workerPoolID2).Int64()
	assert.ErrorIs(t, err, redis.Nil)
	_, err = redisAdapter.HGet(ctx, lockInfo2, workerPoolID2).Int64()
	assert.ErrorIs(t, err, redis.Nil)
}
