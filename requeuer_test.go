package work

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequeue(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	cleanKeyspace(ctx, ns, redisAdapter)

	tMock := nowEpochSeconds() - 10
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()

	enqueuer := NewEnqueuer(ns, redisAdapter)
	_, err := enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("foo", 10, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("foo", 14, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("bar", 19, nil)
	assert.NoError(t, err)

	resetNowEpochSecondsMock()

	re := newRequeuer(ns, redisAdapter, redisKeyScheduled(ns), []string{"wat", "foo", "bar"})
	re.start()
	re.drain()
	re.stop()

	assert.EqualValues(t, 2, listSize(ctx, redisAdapter, redisKeyJobs(ns, "wat")))
	assert.EqualValues(t, 1, listSize(ctx, redisAdapter, redisKeyJobs(ns, "foo")))
	assert.EqualValues(t, 0, listSize(ctx, redisAdapter, redisKeyJobs(ns, "bar")))
	assert.EqualValues(t, 2, zsetSize(ctx, redisAdapter, redisKeyScheduled(ns)))

	j := jobOnQueue(ctx, redisAdapter, redisKeyJobs(ns, "foo"))
	assert.Equal(t, j.Name, "foo")

	// Because we mocked time to 10 seconds ago above, the job was put on the zset with t=10 secs ago
	// We want to ensure it's requeued with t=now.
	// On boundary conditions with the VM, nowEpochSeconds() might be 1 or 2 secs ahead of EnqueuedAt
	assert.True(t, (j.EnqueuedAt+2) >= nowEpochSeconds())
}

func TestRequeueUnknown(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"
	cleanKeyspace(ctx, ns, redisAdapter)

	tMock := nowEpochSeconds() - 10
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()

	enqueuer := NewEnqueuer(ns, redisAdapter)
	_, err := enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)

	nowish := nowEpochSeconds()
	setNowEpochSecondsMock(nowish)

	re := newRequeuer(ns, redisAdapter, redisKeyScheduled(ns), []string{"bar"})
	re.start()
	re.drain()
	re.stop()

	assert.EqualValues(t, 0, zsetSize(ctx, redisAdapter, redisKeyScheduled(ns)))
	assert.EqualValues(t, 1, zsetSize(ctx, redisAdapter, redisKeyDead(ns)))

	rank, job := jobOnZset(ctx, redisAdapter, redisKeyDead(ns))

	assert.Equal(t, nowish, rank)
	assert.Equal(t, nowish, job.FailedAt)
	assert.Equal(t, "unknown job when requeueing", job.LastErr)
}
