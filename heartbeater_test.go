package work

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/GettEngineering/work/redis"
)

func TestHeartbeater(t *testing.T) {
	ctx := context.Background()
	redisAdapter := newTestRedis(":6379")
	ns := "work"

	tMock := int64(1425263409)
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()

	jobTypes := map[string]*jobType{
		"foo": nil,
		"bar": nil,
	}

	heart := newWorkerPoolHeartbeater(ns, redisAdapter, "abcd", jobTypes, 10, []string{"ccc", "bbb"})
	heart.start()

	time.Sleep(20 * time.Millisecond)

	assert.True(t, redisInSet(ctx, redisAdapter, redisKeyWorkerPools(ns), "abcd"))

	h := readHash(ctx, redisAdapter, redisKeyHeartbeat(ns, "abcd"))
	assert.Equal(t, "1425263409", h["heartbeat_at"])
	assert.Equal(t, "1425263409", h["started_at"])
	assert.Equal(t, "bar,foo", h["job_names"])
	assert.Equal(t, "bbb,ccc", h["worker_ids"])
	assert.Equal(t, "10", h["concurrency"])

	assert.True(t, h["pid"] != "")
	assert.True(t, h["host"] != "")

	heart.stop()

	assert.False(t, redisInSet(ctx, redisAdapter, redisKeyWorkerPools(ns), "abcd"))
}

func redisInSet(ctx context.Context, redisAdapter redis.Redis, key, member string) bool {
	v, err := redisAdapter.SIsMember(ctx, key, member)
	if err != nil {
		panic("could not delete retry/dead queue: " + err.Error())
	}
	return v
}
