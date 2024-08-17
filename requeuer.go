package work

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/GettEngineering/work/redis"
)

type requeuer struct {
	namespace    string
	redisAdapter redis.Redis

	redisRequeueScript redis.Script
	redisRequeueKeys   []string
	redisRequeueArgs   []interface{}

	stopChan         chan struct{}
	doneStoppingChan chan struct{}

	drainChan        chan struct{}
	doneDrainingChan chan struct{}
}

func newRequeuer(namespace string, redisAdapter redis.Redis, requeueKey string, jobNames []string) *requeuer {
	keys := make([]string, 0, len(jobNames)+2+2)
	keys = append(keys, requeueKey)              // KEY[1]
	keys = append(keys, redisKeyDead(namespace)) // KEY[2]
	for _, jobName := range jobNames {
		keys = append(keys, redisKeyJobs(namespace, jobName)) // KEY[3, 4, ...]
	}

	args := []any{
		redisKeyJobsPrefix(namespace), // ARGV[1]
		0,                             // ARGV[2] -- NOTE: We're going to change this one on every call
	}

	return &requeuer{
		namespace:    namespace,
		redisAdapter: redisAdapter,

		redisRequeueScript: redisAdapter.NewScript(redisLuaZremLpushCmd, len(jobNames)+2),
		redisRequeueKeys:   keys,
		redisRequeueArgs:   args,

		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),

		drainChan:        make(chan struct{}),
		doneDrainingChan: make(chan struct{}),
	}
}

func (r *requeuer) start() {
	go r.loop()
}

func (r *requeuer) stop() {
	r.stopChan <- struct{}{}
	<-r.doneStoppingChan
}

func (r *requeuer) drain() {
	r.drainChan <- struct{}{}
	<-r.doneDrainingChan
}

func (r *requeuer) loop() {
	ctx := context.TODO()

	// Just do this simple thing for now.
	// If we have 100 processes all running requeuers,
	// there's probably too much hitting redis.
	// So later on we'l have to implement exponential backoff
	ticker := time.Tick(1000 * time.Millisecond)

	for {
		select {
		case <-r.stopChan:
			r.doneStoppingChan <- struct{}{}
			return
		case <-r.drainChan:
			for r.process(ctx) {
			}
			r.doneDrainingChan <- struct{}{}
		case <-ticker:
			for r.process(ctx) {
			}
		}
	}
}

func (r *requeuer) process(ctx context.Context) bool {
	r.redisRequeueArgs[len(r.redisRequeueArgs)-1] = nowEpochSeconds()

	res, err := r.redisRequeueScript.Run(ctx, r.redisRequeueKeys, r.redisRequeueArgs...).Text()
	if errors.Is(err, redis.Nil) {
		return false
	} else if err != nil {
		logError("requeuer.process", err)
		return false
	}

	if res == "" {
		return false
	} else if res == "dead" {
		logError("requeuer.process.dead", fmt.Errorf("no job name"))
		return true
	} else if res == "ok" {
		return true
	}

	return false
}
