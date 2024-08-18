package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/benmanns/goworker"
	goredisv8 "github.com/go-redis/redis/v8"
	"github.com/gocraft/health"
	"github.com/gomodule/redigo/redis"

	workredis "github.com/GettEngineering/work/redis"
	goredisv8adapter "github.com/GettEngineering/work/redis/adapters/goredisv8"
	redigoadapter "github.com/GettEngineering/work/redis/adapters/redigo"
)

var (
	namespace    = "bench_test"
	redisAdapter = newRedis(":6379")
)

func myJob(_ string, _ ...any) error {
	atomic.AddInt64(&totcount, 1)
	return nil
}

func main() {
	ctx := context.Background()
	stream := health.NewStream().AddSink(&health.WriterSink{Writer: os.Stdout})
	stream.Event("wat")
	cleanKeyspace(ctx)

	queues := []string{"myqueue", "myqueue2", "myqueue3", "myqueue4", "myqueue5"}
	numJobs := 100000 / len(queues)

	job := stream.NewJob("enqueue_all")
	for _, q := range queues {
		enqueueJobs(ctx, q, numJobs)
	}
	job.Complete(health.Success)

	goworker.Register("MyClass", myJob)

	go monitor()

	// Blocks until process is told to exit via unix signal
	if err := goworker.Work(); err != nil {
		log.Println("Error:", err.Error())
	}
}

var totcount int64

func monitor() {
	t := time.Tick(1 * time.Second)

	curT := 0
	c1 := int64(0)
	c2 := int64(0)
	prev := int64(0)

	for range t {
		curT++
		v := atomic.AddInt64(&totcount, 0)
		log.Printf("after %d seconds, count is %d\n", curT, v)
		if curT == 1 {
			c1 = v
		} else if curT == 3 {
			c2 = v
		}
		if v == prev {
			break
		}
		prev = v
	}
	log.Println("Jobs/sec: ", float64(c2-c1)/2.0)
	os.Exit(0)
}

func enqueueJobs(ctx context.Context, queue string, count int) {
	for i := 0; i < count; i++ {
		err := redisAdapter.RPush(ctx, "bench_test:queue:"+queue, `{"class":"MyClass","args":[]}`)
		if err != nil {
			panic("could not rpush: " + err.Error())
		}
	}
}

func cleanKeyspace(ctx context.Context) {
	keys, err := redisAdapter.Keys(ctx, namespace+"*")
	if err != nil {
		panic("could not get keys: " + err.Error())
	}
	for _, k := range keys {
		if err := redisAdapter.Del(ctx, k); err != nil {
			panic("could not del: " + err.Error())
		}
	}
}

func newRedis(addr string) workredis.Redis {
	switch os.Getenv("REDIS_ADAPTER") {
	case "redigo":
		pool := &redis.Pool{
			MaxActive:   20,
			MaxIdle:     20,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", addr)
				if err != nil {
					return nil, fmt.Errorf("dial redis: %w", err)
				}
				return c, nil
			},
			Wait: true,
		}
		return redigoadapter.NewAdapter(pool)
	default:
		rdb := goredisv8.NewClient(&goredisv8.Options{
			Addr:     addr,
			Password: "",
			DB:       0,
		})
		return goredisv8adapter.NewAdapter(rdb)
	}
}
