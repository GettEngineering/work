package work

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/GettEngineering/work/redis"
)

const (
	periodicEnqueuerSleep   = 2 * time.Minute
	periodicEnqueuerHorizon = 4 * time.Minute
)

type periodicEnqueuer struct {
	namespace             string
	redisAdapter          redis.Redis
	periodicJobs          []*periodicJob
	scheduledPeriodicJobs []*scheduledPeriodicJob
	stopChan              chan struct{}
	doneStoppingChan      chan struct{}
}

type periodicJob struct {
	jobName  string
	spec     string
	schedule cron.Schedule
}

type scheduledPeriodicJob struct {
	scheduledAt      time.Time
	scheduledAtEpoch int64
	*periodicJob
}

func newPeriodicEnqueuer(namespace string, redisAdapter redis.Redis, periodicJobs []*periodicJob) *periodicEnqueuer {
	return &periodicEnqueuer{
		namespace:        namespace,
		redisAdapter:     redisAdapter,
		periodicJobs:     periodicJobs,
		stopChan:         make(chan struct{}),
		doneStoppingChan: make(chan struct{}),
	}
}

func (pe *periodicEnqueuer) start() {
	go pe.loop()
}

func (pe *periodicEnqueuer) stop() {
	pe.stopChan <- struct{}{}
	<-pe.doneStoppingChan
}

func (pe *periodicEnqueuer) loop() {
	ctx := context.TODO()

	// Begin reaping periodically
	timer := time.NewTimer(periodicEnqueuerSleep + time.Duration(rand.Intn(30))*time.Second)
	defer timer.Stop()

	if pe.shouldEnqueue(ctx) {
		err := pe.enqueue(ctx)
		if err != nil {
			logError("periodic_enqueuer.loop.enqueue", err)
		}
	}

	for {
		select {
		case <-pe.stopChan:
			pe.doneStoppingChan <- struct{}{}
			return
		case <-timer.C:
			timer.Reset(periodicEnqueuerSleep + time.Duration(rand.Intn(30))*time.Second)
			if pe.shouldEnqueue(ctx) {
				err := pe.enqueue(ctx)
				if err != nil {
					logError("periodic_enqueuer.loop.enqueue", err)
				}
			}
		}
	}
}

func (pe *periodicEnqueuer) enqueue(ctx context.Context) error {
	now := nowEpochSeconds()
	nowTime := time.Unix(now, 0)
	horizon := nowTime.Add(periodicEnqueuerHorizon)

	for _, pj := range pe.periodicJobs {
		for t := pj.schedule.Next(nowTime); t.Before(horizon); t = pj.schedule.Next(t) {
			epoch := t.Unix()
			id := makeUniquePeriodicID(pj.jobName, pj.spec, epoch)

			job := &Job{
				Name: pj.jobName,
				ID:   id,

				// This is technically wrong, but this lets the bytes be identical for the same periodic job instance. If we don't do this, we'd need to use a different approach -- probably giving each periodic job its own history of the past 100 periodic jobs, and only scheduling a job if it's not in the history.
				EnqueuedAt: epoch,
				Args:       nil,
			}

			rawJSON, err := job.serialize()
			if err != nil {
				return err
			}

			err = pe.redisAdapter.ZAdd(ctx, redisKeyScheduled(pe.namespace), float64(epoch), rawJSON)
			if err != nil {
				return err
			}
		}
	}

	return pe.redisAdapter.Set(ctx, redisKeyLastPeriodicEnqueue(pe.namespace), now)
}

func (pe *periodicEnqueuer) shouldEnqueue(ctx context.Context) bool {
	lastEnqueue, err := pe.redisAdapter.Get(ctx, redisKeyLastPeriodicEnqueue(pe.namespace)).Int64()

	if errors.Is(err, redis.Nil) {
		return true
	} else if err != nil {
		logError("periodic_enqueuer.should_enqueue", err)
		return true
	}

	return lastEnqueue < (nowEpochSeconds() - int64(periodicEnqueuerSleep/time.Minute))
}

func makeUniquePeriodicID(name, spec string, epoch int64) string {
	return fmt.Sprintf("periodic:%s:%s:%d", name, spec, epoch)
}
