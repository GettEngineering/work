package work

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/GettEngineering/work/redis"
)

func TestObserverStarted(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"

	tMock := int64(1425263401)
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()

	observer := newObserver(ns, redisAdapter, "abcd")
	observer.start()
	observer.observeStarted("foo", "bar", Q{"a": 1, "b": "wat"})
	//observer.observeDone("foo", "bar", nil)
	observer.drain()
	observer.stop()

	h := readHash(ctx, redisAdapter, redisKeyWorkerObservation(ns, "abcd"))
	assert.Equal(t, "foo", h["job_name"])
	assert.Equal(t, "bar", h["job_id"])
	assert.Equal(t, fmt.Sprint(tMock), h["started_at"])
	assert.Equal(t, `{"a":1,"b":"wat"}`, h["args"])
}

func TestObserverStartedDone(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"

	tMock := int64(1425263401)
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()

	observer := newObserver(ns, redisAdapter, "abcd")
	observer.start()
	observer.observeStarted("foo", "bar", Q{"a": 1, "b": "wat"})
	observer.observeDone("foo", "bar", nil)
	observer.drain()
	observer.stop()

	h := readHash(ctx, redisAdapter, redisKeyWorkerObservation(ns, "abcd"))
	assert.Equal(t, 0, len(h))
}

func TestObserverCheckin(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"

	observer := newObserver(ns, redisAdapter, "abcd")
	observer.start()

	tMock := int64(1425263401)
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()
	observer.observeStarted("foo", "bar", Q{"a": 1, "b": "wat"})

	tMockCheckin := int64(1425263402)
	setNowEpochSecondsMock(tMockCheckin)
	observer.observeCheckin("foo", "bar", "doin it")
	observer.drain()
	observer.stop()

	h := readHash(ctx, redisAdapter, redisKeyWorkerObservation(ns, "abcd"))
	assert.Equal(t, "foo", h["job_name"])
	assert.Equal(t, "bar", h["job_id"])
	assert.Equal(t, fmt.Sprint(tMock), h["started_at"])
	assert.Equal(t, `{"a":1,"b":"wat"}`, h["args"])
	assert.Equal(t, "doin it", h["checkin"])
	assert.Equal(t, fmt.Sprint(tMockCheckin), h["checkin_at"])
}

func TestObserverCheckinFromJob(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"

	observer := newObserver(ns, redisAdapter, "abcd")
	observer.start()

	tMock := int64(1425263401)
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()
	observer.observeStarted("foo", "barbar", Q{"a": 1, "b": "wat"})

	tMockCheckin := int64(1425263402)
	setNowEpochSecondsMock(tMockCheckin)

	j := &Job{Name: "foo", ID: "barbar", observer: observer}
	j.Checkin("sup")

	observer.drain()
	observer.stop()

	h := readHash(ctx, redisAdapter, redisKeyWorkerObservation(ns, "abcd"))
	assert.Equal(t, "foo", h["job_name"])
	assert.Equal(t, "barbar", h["job_id"])
	assert.Equal(t, fmt.Sprint(tMock), h["started_at"])
	assert.Equal(t, "sup", h["checkin"])
	assert.Equal(t, fmt.Sprint(tMockCheckin), h["checkin_at"])
}

func readHash(ctx context.Context, redisAdapter redis.Redis, key string) map[string]string {
	v, err := redisAdapter.HGetAll(ctx, key).Result()
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}

	return v
}
