package work

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/GettEngineering/work/redis"
)

// ErrNotDeleted is returned by functions that delete jobs to indicate that although the redis commands were successful,
// no object was actually deleted by those commmands.
var ErrNotDeleted = fmt.Errorf("nothing deleted")

// ErrNotRetried is returned by functions that retry jobs to indicate that although the redis commands were successful,
// no object was actually retried by those commmands.
var ErrNotRetried = fmt.Errorf("nothing retried")

// Client implements all of the functionality of the web UI. It can be used to inspect the status of a running cluster and retry dead jobs.
type Client struct {
	namespace    string
	redisAdapter redis.Redis
}

// NewClient creates a new Client with the specified redis namespace and connection pool.
func NewClient(namespace string, redisAdapter redis.Redis) *Client {
	return &Client{
		namespace:    namespace,
		redisAdapter: redisAdapter,
	}
}

// WorkerPoolHeartbeat represents the heartbeat from a worker pool.
// WorkerPool's write a heartbeat every 5 seconds so we know they're alive and includes config information.
type WorkerPoolHeartbeat struct {
	WorkerPoolID string   `json:"worker_pool_id"`
	StartedAt    int64    `json:"started_at"`
	HeartbeatAt  int64    `json:"heartbeat_at"`
	JobNames     []string `json:"job_names"`
	Concurrency  uint     `json:"concurrency"`
	Host         string   `json:"host"`
	Pid          int      `json:"pid"`
	WorkerIDs    []string `json:"worker_ids"`
}

// WorkerPoolHeartbeats queries Redis and returns all WorkerPoolHeartbeat's it finds (even for those
// worker pools which don't have a current heartbeat).
func (c *Client) WorkerPoolHeartbeats() ([]*WorkerPoolHeartbeat, error) {
	ctx := context.TODO()

	workerPoolsKey := redisKeyWorkerPools(c.namespace)

	workerPoolIDs, err := c.redisAdapter.SMembers(ctx, workerPoolsKey)
	if err != nil {
		return nil, fmt.Errorf("SMEMBERS of %s: %w", workerPoolsKey, err)
	}
	sort.Strings(workerPoolIDs)

	replies := make([]redis.StringStringMapReply, 0, len(workerPoolIDs))

	err = c.redisAdapter.WithPipeline(ctx, func(r redis.Redis) error {
		for _, wpid := range workerPoolIDs {
			key := redisKeyHeartbeat(c.namespace, wpid)
			replies = append(replies, r.HGetAll(ctx, key))
		}

		return nil
	})
	if err != nil {
		logError("worker_pool_statuses.get_heartbeats", err)
		return nil, fmt.Errorf("query heartbeats: %w", err)
	}

	heartbeats := make([]*WorkerPoolHeartbeat, 0, len(workerPoolIDs))

	for i, wpid := range workerPoolIDs {
		heartbeat := &WorkerPoolHeartbeat{
			WorkerPoolID: wpid,
		}

		res, err := replies[i].Result()
		if err != nil {
			logError("worker_pool_statuses.result", err)
			return nil, fmt.Errorf("get worker pool's heartbeat: %w", err)
		}

		for key, value := range res {
			var err error

			switch key {
			case "heartbeat_at":
				heartbeat.HeartbeatAt, err = strconv.ParseInt(value, 10, 64)
			case "started_at":
				heartbeat.StartedAt, err = strconv.ParseInt(value, 10, 64)
			case "job_names":
				heartbeat.JobNames = strings.Split(value, ",")
				sort.Strings(heartbeat.JobNames)
			case "concurrency":
				var vv uint64
				vv, err = strconv.ParseUint(value, 10, 0)
				heartbeat.Concurrency = uint(vv)
			case "host":
				heartbeat.Host = value
			case "pid":
				var vv int64
				vv, err = strconv.ParseInt(value, 10, 0)
				heartbeat.Pid = int(vv)
			case "worker_ids":
				heartbeat.WorkerIDs = strings.Split(value, ",")
				sort.Strings(heartbeat.WorkerIDs)
			}
			if err != nil {
				logError("worker_pool_statuses.parse", err)
				return nil, fmt.Errorf("parse heartbeat: %w", err)
			}
		}

		heartbeats = append(heartbeats, heartbeat)
	}

	return heartbeats, nil
}

// WorkerObservation represents the latest observation taken from a worker.
// The observation indicates whether the worker is busy processing a job, and if so, information about that job.
type WorkerObservation struct {
	WorkerID string `json:"worker_id"`
	IsBusy   bool   `json:"is_busy"`

	// If IsBusy:
	JobName   string `json:"job_name"`
	JobID     string `json:"job_id"`
	StartedAt int64  `json:"started_at"`
	ArgsJSON  string `json:"args_json"`
	Checkin   string `json:"checkin"`
	CheckinAt int64  `json:"checkin_at"`
}

// WorkerObservations returns all of the WorkerObservation's it finds for all worker pools' workers.
func (c *Client) WorkerObservations() ([]*WorkerObservation, error) {
	ctx := context.TODO()

	hbs, err := c.WorkerPoolHeartbeats()
	if err != nil {
		logError("worker_observations.worker_pool_heartbeats", err)
		return nil, err
	}

	var workerIDs []string
	for _, hb := range hbs {
		workerIDs = append(workerIDs, hb.WorkerIDs...)
	}

	replies := make([]redis.StringStringMapReply, 0, len(workerIDs))

	err = c.redisAdapter.WithPipeline(ctx, func(r redis.Redis) error {
		for _, wid := range workerIDs {
			key := redisKeyWorkerObservation(c.namespace, wid)
			replies = append(replies, r.HGetAll(ctx, key))
		}

		return nil
	})
	if err != nil {
		logError("worker_observations.get_observations", err)
		return nil, fmt.Errorf("query worker observations: %w", err)
	}

	observations := make([]*WorkerObservation, 0, len(workerIDs))

	for i, wid := range workerIDs {
		ob := &WorkerObservation{
			WorkerID: wid,
		}

		res, err := replies[i].Result()
		if err != nil {
			logError("worker_observations.result", err)
			return nil, fmt.Errorf("get worker observation: %w", err)
		}

		for key, value := range res {
			ob.IsBusy = true

			var err error

			switch key {
			case "job_name":
				ob.JobName = value
			case "job_id":
				ob.JobID = value
			case "started_at":
				ob.StartedAt, err = strconv.ParseInt(value, 10, 64)
			case "args":
				ob.ArgsJSON = value
			case "checkin":
				ob.Checkin = value
			case "checkin_at":
				ob.CheckinAt, err = strconv.ParseInt(value, 10, 64)
			}
			if err != nil {
				logError("worker_observations.parse", err)
				return nil, fmt.Errorf("parse worker observation: %w", err)
			}
		}

		observations = append(observations, ob)
	}

	return observations, nil
}

// Queue represents a queue that holds jobs with the same name.
// It indicates their name, count, and latency (in seconds).
// Latency is a measurement of how long ago the next job to be processed was enqueued.
type Queue struct {
	JobName string `json:"job_name"`
	Count   int64  `json:"count"`
	Latency int64  `json:"latency"`
}

// Queues returns the Queue's it finds.
func (c *Client) Queues() ([]*Queue, error) {
	ctx := context.TODO()

	// Get all known job names.
	key := redisKeyKnownJobs(c.namespace)

	jobNames, err := c.redisAdapter.SMembers(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("SMEMBERS of %s: %w", key, err)
	}
	sort.Strings(jobNames)

	// Get the length of each job name's list.
	llenReplies := make([]redis.IntReply, 0, len(jobNames))

	err = c.redisAdapter.WithPipeline(ctx, func(r redis.Redis) error {
		for _, jobName := range jobNames {
			llenReplies = append(
				llenReplies,
				r.LLen(ctx, redisKeyJobs(c.namespace, jobName)),
			)
		}

		return nil
	})
	if err != nil {
		logError("client.queues.jobs_lengths", err)
		return nil, fmt.Errorf("query job lengths: %w", err)
	}

	queues := make([]*Queue, 0, len(jobNames))

	for i, jobName := range jobNames {
		count, err := llenReplies[i].Result()
		if err != nil {
			logError("client.queues.length_result", err)
			return nil, fmt.Errorf("get job length: %w", err)
		}

		queue := &Queue{
			JobName: jobName,
			Count:   count,
		}

		queues = append(queues, queue)
	}

	// Get the latency of the last job in each queue.
	lindexReplies := make([]redis.Reply, 0, len(queues))

	err = c.redisAdapter.WithPipeline(ctx, func(r redis.Redis) error {
		for _, s := range queues {
			if s.Count > 0 {
				lindexReplies = append(
					lindexReplies,
					r.LIndex(ctx, redisKeyJobs(c.namespace, s.JobName), -1),
				)
			}
		}

		return nil
	})
	if err != nil {
		logError("client.queues.last_jobs", err)
		return nil, fmt.Errorf("query last jobs: %w", err)
	}

	now := nowEpochSeconds()
	j := 0

	for _, s := range queues {
		if s.Count <= 0 {
			continue
		}

		b, err := lindexReplies[j].Bytes()
		if err != nil {
			logError("client.queues.last_job_result", err)
			return nil, fmt.Errorf("get last job: %w", err)
		}

		j++

		job, err := newJob(b, nil, nil)
		if err != nil {
			logError("client.queues.new_job", err)
		}
		s.Latency = now - job.EnqueuedAt
	}

	return queues, nil
}

// RetryJob represents a job in the retry queue.
type RetryJob struct {
	RetryAt int64 `json:"retry_at"`
	*Job
}

// ScheduledJob represents a job in the scheduled queue.
type ScheduledJob struct {
	RunAt int64 `json:"run_at"`
	*Job
}

// DeadJob represents a job in the dead queue.
type DeadJob struct {
	DiedAt int64 `json:"died_at"`
	*Job
}

// ScheduledJobs returns a list of ScheduledJob's. The page param is 1-based; each page is 20 items.
// The total number of items (not pages) in the list of scheduled jobs is also returned.
func (c *Client) ScheduledJobs(page uint) ([]*ScheduledJob, int64, error) {
	ctx := context.TODO()
	key := redisKeyScheduled(c.namespace)

	jobsWithScores, count, err := c.getZsetPage(ctx, key, page)
	if err != nil {
		logError("client.scheduled_jobs.get_zset_page", err)
		return nil, 0, err
	}

	jobs := make([]*ScheduledJob, 0, len(jobsWithScores))

	for _, jws := range jobsWithScores {
		jobs = append(jobs, &ScheduledJob{RunAt: jws.Score, Job: jws.job})
	}

	return jobs, count, nil
}

// RetryJobs returns a list of RetryJob's. The page param is 1-based; each page is 20 items.
// The total number of items (not pages) in the list of retry jobs is also returned.
func (c *Client) RetryJobs(page uint) ([]*RetryJob, int64, error) {
	ctx := context.TODO()
	key := redisKeyRetry(c.namespace)

	jobsWithScores, count, err := c.getZsetPage(ctx, key, page)
	if err != nil {
		logError("client.retry_jobs.get_zset_page", err)
		return nil, 0, err
	}

	jobs := make([]*RetryJob, 0, len(jobsWithScores))

	for _, jws := range jobsWithScores {
		jobs = append(jobs, &RetryJob{RetryAt: jws.Score, Job: jws.job})
	}

	return jobs, count, nil
}

// DeadJobs returns a list of DeadJob's. The page param is 1-based; each page is 20 items.
// The total number of items (not pages) in the list of dead jobs is also returned.
func (c *Client) DeadJobs(page uint) ([]*DeadJob, int64, error) {
	ctx := context.TODO()
	key := redisKeyDead(c.namespace)

	jobsWithScores, count, err := c.getZsetPage(ctx, key, page)
	if err != nil {
		logError("client.dead_jobs.get_zset_page", err)
		return nil, 0, err
	}

	jobs := make([]*DeadJob, 0, len(jobsWithScores))

	for _, jws := range jobsWithScores {
		jobs = append(jobs, &DeadJob{DiedAt: jws.Score, Job: jws.job})
	}

	return jobs, count, nil
}

// DeleteDeadJob deletes a dead job from Redis.
func (c *Client) DeleteDeadJob(diedAt int64, jobID string) error {
	ctx := context.TODO()

	ok, _, err := c.deleteZsetJob(ctx, redisKeyDead(c.namespace), diedAt, jobID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotDeleted
	}
	return nil
}

// RetryDeadJob retries a dead job. The job will be re-queued on the normal work queue for eventual processing by a worker.
func (c *Client) RetryDeadJob(diedAt int64, jobID string) error {
	ctx := context.TODO()

	// Get queues for job names
	queues, err := c.Queues()
	if err != nil {
		logError("client.retry_all_dead_jobs.queues", err)
		return err
	}

	// Extract job names
	jobNames := make([]string, 0, len(queues))
	for _, q := range queues {
		jobNames = append(jobNames, q.JobName)
	}

	script := c.redisAdapter.NewScript(redisLuaRequeueSingleDeadCmd, len(jobNames)+1)

	keys := make([]string, 0, len(jobNames)+1)
	keys = append(keys, redisKeyDead(c.namespace)) // KEY[1]
	for _, jobName := range jobNames {
		keys = append(keys, redisKeyJobs(c.namespace, jobName)) // KEY[2, 3, ...]
	}

	args := []any{
		redisKeyJobsPrefix(c.namespace), // ARGV[1]
		nowEpochSeconds(),
		diedAt,
		jobID,
	}

	cnt, err := script.Run(ctx, keys, args...).Int64()
	if err != nil {
		logError("client.retry_dead_job.do", err)
		return fmt.Errorf("run script: %w", err)
	}

	if cnt == 0 {
		return ErrNotRetried
	}

	return nil
}

// RetryAllDeadJobs requeues all dead jobs. In other words, it puts them all back on the normal work queue for workers
// to pull from and process.
func (c *Client) RetryAllDeadJobs() error {
	ctx := context.TODO()

	// Get queues for job names
	queues, err := c.Queues()
	if err != nil {
		logError("client.retry_all_dead_jobs.queues", err)
		return err
	}

	// Extract job names
	jobNames := make([]string, 0, len(queues))
	for _, q := range queues {
		jobNames = append(jobNames, q.JobName)
	}

	script := c.redisAdapter.NewScript(redisLuaRequeueAllDeadCmd, len(jobNames)+1)

	keys := make([]string, 0, len(jobNames)+1)
	keys = append(keys, redisKeyDead(c.namespace)) // KEY[1]
	for _, jobName := range jobNames {
		keys = append(keys, redisKeyJobs(c.namespace, jobName)) // KEY[2, 3, ...]
	}

	args := []any{
		redisKeyJobsPrefix(c.namespace), // ARGV[1]
		nowEpochSeconds(),
		1000,
	}

	// Cap iterations for safety (which could reprocess 1k*1k jobs).
	// This is conceptually an infinite loop but let's be careful.
	for i := 0; i < 1000; i++ {
		res, err := script.Run(ctx, keys, args...).Int64()
		if err != nil {
			logError("client.retry_all_dead_jobs.do", err)
			return fmt.Errorf("run script: %w", err)
		}

		if res == 0 {
			break
		}
	}

	return nil
}

// DeleteAllDeadJobs deletes all dead jobs.
func (c *Client) DeleteAllDeadJobs() error {
	ctx := context.TODO()

	key := redisKeyDead(c.namespace)
	if err := c.redisAdapter.Del(ctx, key); err != nil {
		logError("client.delete_all_dead_jobs", err)
		return fmt.Errorf("DEL of %s: %w", key, err)
	}

	return nil
}

// DeleteScheduledJob deletes a job in the scheduled queue.
func (c *Client) DeleteScheduledJob(scheduledFor int64, jobID string) error {
	ctx := context.TODO()

	ok, jobBytes, err := c.deleteZsetJob(ctx, redisKeyScheduled(c.namespace), scheduledFor, jobID)
	if err != nil {
		return err
	}

	// If we get a job back, parse it and see if it's a unique job. If it is, we need to delete the unique key.
	if len(jobBytes) > 0 {
		job, err := newJob(jobBytes, nil, nil)
		if err != nil {
			logError("client.delete_scheduled_job.new_job", err)
			return err
		}

		if job.Unique {
			uniqueKey, err := redisKeyUniqueJob(c.namespace, job.Name, job.Args)
			if err != nil {
				logError("client.delete_scheduled_job.redis_key_unique_job", err)
				return err
			}

			err = c.redisAdapter.Del(ctx, uniqueKey)
			if err != nil {
				logError("worker.delete_unique_job.del", err)
				return fmt.Errorf("delete unique job key: %w", err)
			}
		}
	}

	if !ok {
		return ErrNotDeleted
	}
	return nil
}

// DeleteRetryJob deletes a job in the retry queue.
func (c *Client) DeleteRetryJob(retryAt int64, jobID string) error {
	ctx := context.TODO()

	ok, _, err := c.deleteZsetJob(ctx, redisKeyRetry(c.namespace), retryAt, jobID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrNotDeleted
	}
	return nil
}

// deleteZsetJob deletes the job in the specified zset (dead, retry, or scheduled queue).
// zsetKey is like "work:dead" or "work:scheduled".
// The function deletes all jobs with the given jobID with the specified zscore (there should only be one,
// but in theory there could be bad data).
func (c *Client) deleteZsetJob(
	ctx context.Context,
	zsetKey string,
	zscore int64,
	jobID string,
) (deleted bool, lastJob []byte, err error) {
	script := c.redisAdapter.NewScript(redisLuaDeleteSingleCmd, 1)
	keys := []string{zsetKey} // KEY[1]
	args := []any{
		zscore, // ARGV[1]
		jobID,  // ARGV[2]
	}

	values, err := script.Run(ctx, keys, args...).Slice()
	if err != nil {
		logError("client.delete_zset_job.run", err)
		return false, nil, fmt.Errorf("run script to delete jobs: %w", err)
	}

	if len(values) != 2 {
		return false, nil, fmt.Errorf("need 2 elements back from redis command")
	}

	cnt, ok := values[0].(int64)
	if !ok {
		return false, nil, fmt.Errorf("expected int64, got %T", values[0])
	}

	var jobBytes []byte
	switch v := values[1].(type) {
	case string:
		jobBytes = []byte(v)
	case []byte:
		jobBytes = v
	default:
		return false, nil, fmt.Errorf("expected string or []byte, got %T", values[1])
	}

	return cnt > 0, jobBytes, nil
}

type jobScore struct {
	JobBytes []byte
	Score    int64
	job      *Job
}

func (c *Client) getZsetPage(ctx context.Context, key string, page uint) ([]jobScore, int64, error) {
	if page == 0 {
		page = 1
	}

	ranges, err := c.redisAdapter.ZRangeByScoreWithScores(ctx, key, "-inf", "+inf", int64((page-1)*20), 20)
	if err != nil {
		logError("client.get_zset_page.ranges", err)
		return nil, 0, fmt.Errorf("ZRANGE of %s: %w", key, err)
	}

	jobsWithScores := make([]jobScore, ranges.Len())

	for i := 0; ranges.Next(); i++ {
		member, score := ranges.Val()

		v, ok := member.(string)
		if !ok {
			err = fmt.Errorf("expected string, got %T", member)
			logError("client.get_zset_page.read_range", err)
			return nil, 0, err
		}

		rawJSON := []byte(v)

		job, err := newJob(rawJSON, nil, nil)
		if err != nil {
			logError("client.get_zset_page.new_job", err)
			return nil, 0, err
		}

		jobsWithScores[i].JobBytes = rawJSON
		jobsWithScores[i].Score = int64(score)
		jobsWithScores[i].job = job
	}

	count, err := c.redisAdapter.ZCard(ctx, key)
	if err != nil {
		logError("client.get_zset_page.int64", err)
		return nil, 0, fmt.Errorf("get total count of jobs: %w", err)
	}

	return jobsWithScores, count, nil
}
