package work

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/GettEngineering/work/redis"
)

const (
	deadTime          = 10 * time.Second // 2 x heartbeat
	reapPeriod        = 10 * time.Minute
	reapJitterSecs    = 30
	requeueKeysPerJob = 4
)

type deadPoolReaper struct {
	namespace    string
	redisAdapter redis.Redis
	deadTime     time.Duration
	reapPeriod   time.Duration
	curJobTypes  []string

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

func newDeadPoolReaper(namespace string, redisAdapter redis.Redis, curJobTypes []string) *deadPoolReaper {
	return &deadPoolReaper{
		namespace:        namespace,
		redisAdapter:     redisAdapter,
		deadTime:         deadTime,
		reapPeriod:       reapPeriod,
		curJobTypes:      curJobTypes,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
	}
}

func (r *deadPoolReaper) start() {
	go r.loop()
}

func (r *deadPoolReaper) stop() {
	r.stopChan <- struct{}{}
	<-r.doneStoppingChan
}

func (r *deadPoolReaper) loop() {
	ctx := context.TODO()

	// Reap immediately after we provide some time for initialization
	timer := time.NewTimer(r.deadTime)
	defer timer.Stop()

	for {
		select {
		case <-r.stopChan:
			r.doneStoppingChan <- struct{}{}
			return
		case <-timer.C:
			// Schedule next occurrence periodically with jitter
			timer.Reset(r.reapPeriod + time.Duration(rand.Intn(reapJitterSecs))*time.Second)

			// Reap
			if err := r.reap(ctx); err != nil {
				logError("dead_pool_reaper.reap", err)
			}
		}
	}
}

func (r *deadPoolReaper) reap(ctx context.Context) error {
	// Get dead pools
	deadPoolIDs, err := r.findDeadPools(ctx)
	if err != nil {
		return fmt.Errorf("find dead pools: %w", err)
	}

	workerPoolsKey := redisKeyWorkerPools(r.namespace)

	// Cleanup all dead pools
	for deadPoolID, jobTypes := range deadPoolIDs {
		lockJobTypes := jobTypes
		// if we found jobs from the heartbeat, requeue them and remove the heartbeat
		if len(jobTypes) > 0 {
			r.requeueInProgressJobs(ctx, deadPoolID, jobTypes)
			if err := r.redisAdapter.Del(ctx, redisKeyHeartbeat(r.namespace, deadPoolID)); err != nil {
				return fmt.Errorf("delete heartbeat: %w", err)
			}
		} else {
			// try to clean up locks for the current set of jobs if heartbeat was not found
			lockJobTypes = r.curJobTypes
		}
		// Cleanup any stale lock info
		if err = r.cleanStaleLockInfo(ctx, deadPoolID, lockJobTypes); err != nil {
			return fmt.Errorf("clean stale lock info: %w", err)
		}
		// Remove dead pool from worker pools set
		if err := r.redisAdapter.SRem(ctx, workerPoolsKey, deadPoolID); err != nil {
			return fmt.Errorf("remove dead pool from worker pools: %w", err)
		}
	}

	return nil
}

func (r *deadPoolReaper) cleanStaleLockInfo(ctx context.Context, poolID string, jobTypes []string) error {
	numKeys := len(jobTypes) * 2
	redisReapLocksScript := r.redisAdapter.NewScript(redisLuaReapStaleLocks, numKeys)

	keys := make([]string, 0, numKeys)

	for _, jobType := range jobTypes {
		keys = append(keys, redisKeyJobsLock(r.namespace, jobType), redisKeyJobsLockInfo(r.namespace, jobType))
	}

	if err := redisReapLocksScript.Run(ctx, keys, poolID).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return err
	}

	return nil
}

func (r *deadPoolReaper) requeueInProgressJobs(ctx context.Context, poolID string, jobTypes []string) error {
	numKeys := len(jobTypes) * requeueKeysPerJob
	redisRequeueScript := r.redisAdapter.NewScript(redisLuaReenqueueJob, numKeys)
	keys := make([]string, 0, numKeys)

	for _, jobType := range jobTypes {
		// pops from in progress, push into job queue and decrement the queue lock
		keys = append(
			keys,
			redisKeyJobsInProgress(r.namespace, poolID, jobType),
			redisKeyJobs(r.namespace, jobType),
			redisKeyJobsLock(r.namespace, jobType),
			redisKeyJobsLockInfo(r.namespace, jobType),
		) // KEYS[1-4 * N]
	}

	// Keep moving jobs until all queues are empty.
	for {
		values, err := redisRequeueScript.Run(ctx, keys, poolID).Slice()
		if errors.Is(err, redis.Nil) {
			return nil
		} else if err != nil {
			return err
		}

		if len(values) != 3 {
			return fmt.Errorf("need 3 elements back")
		}
	}
}

// findDeadPools returns a map of dead worker pool IDs to the job types that were running in them.
func (r *deadPoolReaper) findDeadPools(ctx context.Context) (map[string][]string, error) {
	workerPoolsKey := redisKeyWorkerPools(r.namespace)

	workerPoolIDs, err := r.redisAdapter.SMembers(ctx, workerPoolsKey)
	if err != nil {
		return nil, fmt.Errorf("get worker pools: %w", err)
	}

	deadPools := map[string][]string{}
	for _, workerPoolID := range workerPoolIDs {
		heartbeatKey := redisKeyHeartbeat(r.namespace, workerPoolID)
		heartbeatAt, err := r.redisAdapter.HGet(ctx, heartbeatKey, "heartbeat_at").Int64()
		if errors.Is(err, redis.Nil) {
			// heartbeat expired, save dead pool and use cur set of jobs from reaper
			deadPools[workerPoolID] = []string{}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get pool's \"heartbeat_at\": %w", err)
		}

		// Check that last heartbeat was long enough ago to consider the pool dead
		if time.Unix(heartbeatAt, 0).Add(r.deadTime).After(time.Now()) {
			continue
		}

		jobTypesList, err := r.redisAdapter.HGet(ctx, heartbeatKey, "job_names").Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get pool's \"job_names\": %w", err)
		}

		deadPools[workerPoolID] = strings.Split(jobTypesList, ",")
	}

	return deadPools, nil
}
