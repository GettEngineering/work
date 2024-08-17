package work

import (
	"context"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/GettEngineering/work/redis"
)

const (
	beatPeriod = 5 * time.Second
)

type workerPoolHeartbeater struct {
	workerPoolID string
	namespace    string // eg, "myapp-work"
	redisAdapter redis.Redis
	beatPeriod   time.Duration
	concurrency  uint
	jobNames     string
	startedAt    int64
	pid          int
	hostname     string
	workerIDs    string

	stopChan         chan struct{}
	doneStoppingChan chan struct{}
}

func newWorkerPoolHeartbeater(
	namespace string,
	redisAdapter redis.Redis,
	workerPoolID string,
	jobTypes map[string]*jobType,
	concurrency uint,
	workerIDs []string,
) *workerPoolHeartbeater {
	h := &workerPoolHeartbeater{
		workerPoolID:     workerPoolID,
		namespace:        namespace,
		redisAdapter:     redisAdapter,
		beatPeriod:       beatPeriod,
		concurrency:      concurrency,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
	}

	jobNames := make([]string, 0, len(jobTypes))
	for k := range jobTypes {
		jobNames = append(jobNames, k)
	}
	sort.Strings(jobNames)
	h.jobNames = strings.Join(jobNames, ",")

	sort.Strings(workerIDs)
	h.workerIDs = strings.Join(workerIDs, ",")

	h.pid = os.Getpid()
	host, err := os.Hostname()
	if err != nil {
		logError("heartbeat.hostname", err)
		host = "hostname_errored"
	}
	h.hostname = host

	return h
}

func (h *workerPoolHeartbeater) start() {
	go h.loop()
}

func (h *workerPoolHeartbeater) stop() {
	h.stopChan <- struct{}{}
	<-h.doneStoppingChan
}

func (h *workerPoolHeartbeater) loop() {
	ctx := context.TODO()

	h.startedAt = nowEpochSeconds()
	h.heartbeat(ctx) // do it right away
	ticker := time.Tick(h.beatPeriod)
	for {
		select {
		case <-h.stopChan:
			h.removeHeartbeat(ctx)
			h.doneStoppingChan <- struct{}{}
			return
		case <-ticker:
			h.heartbeat(ctx)
		}
	}
}

func (h *workerPoolHeartbeater) heartbeat(ctx context.Context) {
	workerPoolsKey := redisKeyWorkerPools(h.namespace)
	heartbeatKey := redisKeyHeartbeat(h.namespace, h.workerPoolID)

	err := h.redisAdapter.WithPipeline(ctx, func(r redis.Redis) error {
		if err := r.SAdd(ctx, workerPoolsKey, h.workerPoolID); err != nil {
			return err
		}
		if err := r.HSet(ctx, heartbeatKey,
			"heartbeat_at", nowEpochSeconds(),
			"started_at", h.startedAt,
			"job_names", h.jobNames,
			"concurrency", h.concurrency,
			"worker_ids", h.workerIDs,
			"host", h.hostname,
			"pid", h.pid,
		); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logError("heartbeat", err)
	}
}

func (h *workerPoolHeartbeater) removeHeartbeat(ctx context.Context) {
	workerPoolsKey := redisKeyWorkerPools(h.namespace)
	heartbeatKey := redisKeyHeartbeat(h.namespace, h.workerPoolID)

	err := h.redisAdapter.WithPipeline(ctx, func(r redis.Redis) error {
		if err := r.SRem(ctx, workerPoolsKey, h.workerPoolID); err != nil {
			return err
		}
		if err := r.Del(ctx, heartbeatKey); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logError("remove_heartbeat", err)
	}
}
