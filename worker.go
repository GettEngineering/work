package work

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"time"

	"github.com/GettEngineering/work/redis"
)

const fetchKeysPerJobType = 6

type worker struct {
	workerID      string
	poolID        string
	namespace     string
	redisAdapter  redis.Redis
	jobTypes      map[string]*jobType
	sleepBackoffs []int64
	middleware    []*middlewareHandler
	contextType   reflect.Type

	redisFetchScript redis.Script
	sampler          prioritySampler
	*observer

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

func newWorker(
	namespace string,
	poolID string,
	redisAdapter redis.Redis,
	contextType reflect.Type,
	jobTypes map[string]*jobType,
	sleepBackoffs []int64,
) *worker {
	workerID := makeIdentifier()
	ob := newObserver(namespace, redisAdapter, workerID)

	if len(sleepBackoffs) == 0 {
		sleepBackoffs = sleepBackoffsInMilliseconds
	}

	w := &worker{
		workerID:      workerID,
		poolID:        poolID,
		namespace:     namespace,
		redisAdapter:  redisAdapter,
		contextType:   contextType,
		sleepBackoffs: sleepBackoffs,

		observer: ob,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}

	w.updateMiddlewareAndJobTypes(nil, jobTypes)

	return w
}

// note: can't be called while the thing is started
func (w *worker) updateMiddlewareAndJobTypes(middleware []*middlewareHandler, jobTypes map[string]*jobType) {
	w.middleware = middleware
	sampler := prioritySampler{}
	for _, jt := range jobTypes {
		sampler.add(jt.Priority,
			redisKeyJobs(w.namespace, jt.Name),
			redisKeyJobsInProgress(w.namespace, w.poolID, jt.Name),
			redisKeyJobsPaused(w.namespace, jt.Name),
			redisKeyJobsLock(w.namespace, jt.Name),
			redisKeyJobsLockInfo(w.namespace, jt.Name),
			redisKeyJobsConcurrency(w.namespace, jt.Name))
	}
	w.sampler = sampler
	w.jobTypes = jobTypes
	w.redisFetchScript = w.redisAdapter.NewScript(redisLuaFetchJob, len(jobTypes)*fetchKeysPerJobType)
}

func (w *worker) start() {
	go w.loop()
	go w.observer.start()
}

func (w *worker) stop() {
	w.stopChan <- struct{}{}
	<-w.doneStoppingChan
	w.observer.drain()
	w.observer.stop()
}

func (w *worker) drain() {
	w.drainChan <- struct{}{}
	<-w.doneDrainingChan
	w.observer.drain()
}

var sleepBackoffsInMilliseconds = []int64{0, 10, 100, 1000, 5000}

func (w *worker) loop() {
	var (
		drained           bool
		consequtiveNoJobs int64
	)

	ctx := context.TODO()

	// Begin immediately. We'll change the duration on each tick with a timer.Reset()
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-w.stopChan:
			w.doneStoppingChan <- struct{}{}
			return
		case <-w.drainChan:
			drained = true
			timer.Reset(0)
		case <-timer.C:
			job, err := w.fetchJob(ctx)

			switch {
			case err != nil:
				logError("worker.fetch", err)
				timer.Reset(10 * time.Millisecond)
			case job != nil:
				w.processJob(ctx, job)
				consequtiveNoJobs = 0
				timer.Reset(0)
			default:
				if drained {
					w.doneDrainingChan <- struct{}{}
					drained = false
				}
				consequtiveNoJobs++
				idx := consequtiveNoJobs
				if idx >= int64(len(w.sleepBackoffs)) {
					idx = int64(len(w.sleepBackoffs)) - 1
				}
				timer.Reset(time.Duration(w.sleepBackoffs[idx]) * time.Millisecond)
			}
		}
	}
}

// fetchJob returns a job, or nil if there are no jobs.
// It looks for any of the registered jobs. As soon it finds one, it
// extracts the job from the queue <namespace>:jobs:<jobName> and puts it to the queue
// <namespace>:jobs:<jobName>:<poolID>:inprogress. For more details see redisLuaFetchJob lua script.
// The found job is returned as a Job struct.
func (w *worker) fetchJob(ctx context.Context) (*Job, error) {
	// resort queues
	// NOTE: we could optimize this to only resort every second, or something.
	w.sampler.sample()

	numKeys := len(w.sampler.samples) * fetchKeysPerJobType
	keys := make([]string, 0, numKeys)

	for _, s := range w.sampler.samples {
		keys = append(
			keys,
			s.redisJobs,
			s.redisJobsInProg,
			s.redisJobsPaused,
			s.redisJobsLock,
			s.redisJobsLockInfo,
			s.redisJobsMaxConcurrency,
		) // KEYS[1-6 * N]
	}

	values, err := w.redisFetchScript.Run(
		ctx,
		keys,
		w.poolID, // ARGV[1]
	).Slice()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("run fetch script: %w", err)
	}

	if len(values) != 3 {
		return nil, fmt.Errorf("need 3 elements back")
	}

	rawJSON, ok := convertToBytes(values[0])
	if !ok {
		return nil, fmt.Errorf("expected msg is []byte or string, got %T", values[0])
	}

	dequeuedFrom, ok := convertToBytes(values[1])
	if !ok {
		return nil, fmt.Errorf("expected queue is []byte or string, got %T", values[1])
	}

	inProgQueue, ok := convertToBytes(values[2])
	if !ok {
		return nil, fmt.Errorf("expected inqueue is []byte or string, got %T", values[2])
	}

	job, err := newJob(rawJSON, dequeuedFrom, inProgQueue)
	if err != nil {
		return nil, fmt.Errorf("new job: %w", err)
	}

	return job, nil
}

func (w *worker) processJob(ctx context.Context, job *Job) {
	if job.Unique {
		updatedJob := w.getUniqueJob(ctx, job)
		// This is to support the old way of doing it, where we used the job off the queue and just deleted the unique key
		// Going forward the job on the queue will always be just a placeholder, and we will be replacing it with the
		// updated job extracted here
		if updatedJob != nil {
			job = updatedJob
		}
	}

	var runErr error

	jt := w.jobTypes[job.Name]
	if jt == nil {
		runErr = fmt.Errorf("stray job: no handler")
		logError("process_job.stray", runErr)
	} else {
		w.observeStarted(job.Name, job.ID, job.Args)
		job.observer = w.observer // for Checkin
		_, runErr = runJob(job, w.contextType, w.middleware, jt)
		w.observeDone(job.Name, job.ID, runErr)
	}

	fate := terminateOnly
	op := opTerminate
	if runErr != nil {
		job.failed(runErr)
		fate, op = w.jobFate(jt, job)
	}
	w.removeJobFromInProgress(ctx, job, fate)

	// Remove unique job after it has finished or has been put in dead queue.
	if job.Unique && op != opRetry {
		w.deleteUniqueJob(ctx, job)
	}
}

func (w *worker) getUniqueJob(ctx context.Context, job *Job) *Job {
	var (
		uniqueKey string
		err       error
	)

	if job.UniqueKey != "" {
		uniqueKey = job.UniqueKey
	} else { // For jobs put in queue prior to this change. In the future this can be deleted as there will always be a UniqueKey
		uniqueKey, err = redisKeyUniqueJob(w.namespace, job.Name, job.Args)
		if err != nil {
			logError("worker.get_unique_job.key", err)
			return nil
		}
	}

	rawJSON, err := w.redisAdapter.Get(ctx, uniqueKey).Bytes()
	if err != nil {
		logError("worker.get_unique_job.get", err)
		return nil
	}

	// Previous versions did not support updated arguments and just set key to 1, so in these cases we should do nothing.
	// In the future this can be deleted, as we will always be getting arguments from here.
	// If job.Fails != 0, that means the job comes from retry queue and we already have all args.
	if string(rawJSON) == "1" || job.Fails != 0 {
		return nil
	}

	// The job pulled off the queue was just a placeholder with no args, so replace it
	jobWithArgs, err := newJob(rawJSON, job.dequeuedFrom, job.inProgQueue)
	if err != nil {
		logError("worker.get_unique_job.updated_job", err)
		return nil
	}

	//nolint:lll // we need long lines here for readability
	// This is a hack to fix the following problem.
	// If a job is scheduled with a unique key (EnqueueUniqueInByKey), it's added in 2 places in redis:
	// scheduled queue and under unique key.
	// A requeuer loop calls a lua script, which extracts the job from the scheduled queue and
	// puts it to the jobs queue. Also the script adds a new field to the json body of the job using cjson library.
	// It encodes json with a different field order than the golang encoding/json.
	// Later on, a worker loop moves the job from the jobs queue to the inprocess queue.
	// The worker after processing the job, deletes the job from the inprocess queue and the unique key,
	// but
	// for deletion it uses rawJson from unique job received from unique key, which doesn't match the json body of the job
	// in the inprocess queue. Without this hack we'd get memory leak in redis, because the job would never be deleted
	// from the inprocess queue.
	//
	// EnqueueUniqueInByKey -> scheduled queue -> (json body is modified) -> jobs queue -> inprocess queue -> (handle job) -> delete from inprocess queue
	//                    \                                                                                                   using rawJson from unique key
	//                      -> unique key
	// NOTE: this field is used only to delete the job from the inprocess queue.
	// job.rawJSON is the original json body of the job coming from jobs queue.
	jobWithArgs.rawJSON = job.rawJSON

	return jobWithArgs
}

func (w *worker) deleteUniqueJob(ctx context.Context, job *Job) {
	var (
		uniqueKey string
		err       error
	)

	if job.UniqueKey != "" {
		uniqueKey = job.UniqueKey
	} else { // For jobs put in queue prior to this change. In the future this can be deleted as there will always be a UniqueKey
		uniqueKey, err = redisKeyUniqueJob(w.namespace, job.Name, job.Args)
		if err != nil {
			logError("worker.delete_unique_job.key", err)
			return
		}
	}

	err = w.redisAdapter.Del(ctx, uniqueKey)
	if err != nil {
		logError("worker.delete_unique_job.del", err)
	}
}

func (w *worker) removeJobFromInProgress(ctx context.Context, job *Job, fate terminateOp) {
	err := w.redisAdapter.WithMulti(ctx, func(r redis.Redis) error {
		key := string(job.inProgQueue)
		if err := r.LRem(ctx, key, job.rawJSON); err != nil {
			return fmt.Errorf("LREM from %s: %w", key, err)
		}

		key = redisKeyJobsLock(w.namespace, job.Name)
		if err := r.Decr(ctx, key); err != nil {
			return fmt.Errorf("DECR of %s: %w", key, err)
		}

		key = redisKeyJobsLockInfo(w.namespace, job.Name)
		if err := r.HIncrBy(ctx, key, w.poolID, -1); err != nil {
			return fmt.Errorf("HINCRBY of %s: %w", key, err)
		}

		if err := fate(ctx, r); err != nil {
			logError("worker.remove_job_from_in_progress.fate", err)
		}

		return nil
	})
	if err != nil {
		logError("worker.remove_job_from_in_progress.lrem", err)
	}
}

type terminateOp func(ctx context.Context, r redis.Redis) error

// opType describes the type of terminateOp.
// It's used to distinguish between terminateOp functions.
type opType int

const (
	opTerminate = opType(iota)
	opRetry
	opDead
)

func terminateOnly(_ context.Context, _ redis.Redis) error {
	return nil
}

func terminateAndRetry(w *worker, jt *jobType, job *Job) (terminateOp, opType) {
	rawJSON, err := job.serialize()
	if err != nil {
		logError("worker.terminate_and_retry.serialize", err)
		return terminateOnly, opTerminate
	}

	fateFn := func(ctx context.Context, r redis.Redis) error {
		score := float64(nowEpochSeconds() + jt.calcBackoff(job))
		key := redisKeyRetry(w.namespace)
		if err := r.ZAdd(ctx, key, score, rawJSON); err != nil {
			return fmt.Errorf("ZADD job to %s: %w", key, err)
		}
		return nil
	}

	return fateFn, opRetry
}

func terminateAndDead(w *worker, job *Job) (terminateOp, opType) {
	rawJSON, err := job.serialize()
	if err != nil {
		logError("worker.terminate_and_dead.serialize", err)
		return terminateOnly, opTerminate
	}

	fateFn := func(ctx context.Context, r redis.Redis) error {
		// NOTE: sidekiq limits the # of jobs: only keep jobs for 6 months, and only keep a max # of jobs
		// The max # of jobs seems really horrible. Seems like operations should be on top of it.
		// conn.Send("ZREMRANGEBYSCORE", redisKeyDead(w.namespace), "-inf", now - keepInterval)
		// conn.Send("ZREMRANGEBYRANK", redisKeyDead(w.namespace), 0, -maxJobs)
		score := float64(nowEpochSeconds())
		key := redisKeyDead(w.namespace)
		if err := r.ZAdd(ctx, key, score, rawJSON); err != nil {
			return fmt.Errorf("ZADD job to %s: %w", key, err)
		}
		return err
	}

	return fateFn, opDead
}

func (w *worker) jobFate(jt *jobType, job *Job) (terminateOp, opType) {
	if jt != nil {
		failsRemaining := int64(jt.MaxFails) - job.Fails
		if failsRemaining > 0 {
			return terminateAndRetry(w, jt, job)
		}
		if jt.SkipDead {
			return terminateOnly, opTerminate
		}
	}
	return terminateAndDead(w, job)
}

// defaultBackoffCalculator returns an fastly increasing backoff counter which grows in an unbounded fashion.
func defaultBackoffCalculator(job *Job) int64 {
	fails := job.Fails
	//nolint:gosec // we don't need a crypto strong random number here
	return (fails * fails * fails * fails) + 15 + (int64(rand.Uint32N(30)) * (fails + 1))
}

func convertToBytes(value any) ([]byte, bool) {
	switch v := value.(type) {
	case string:
		return []byte(v), true
	case []byte:
		return v, true
	default:
		return nil, false
	}
}
