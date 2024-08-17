package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	goredisv8 "github.com/go-redis/redis/v8"
	"github.com/gocraft/health"
	"github.com/gomodule/redigo/redis"

	"github.com/GettEngineering/work"
	workredis "github.com/GettEngineering/work/redis"
	goredisv8adapter "github.com/GettEngineering/work/redis/adapters/goredisv8"
	redigoadapter "github.com/GettEngineering/work/redis/adapters/redigo"
)

var namespace = "bench_test"
var redisAdapter = newRedis(":6379")

type workContext struct{}

func epsilonHandler(job *work.Job) error {
	//fmt.Println("hi")
	//a := job.Args[0]
	//fmt.Printf("job: %s arg: %v\n", job.Name, a)
	atomic.AddInt64(&totcount, 1)
	return nil
}

func main() {
	ctx := context.Background()
	stream := health.NewStream().AddSink(&health.WriterSink{os.Stdout})
	cleanKeyspace(ctx)

	numJobs := 10
	jobNames := []string{}

	for i := 0; i < numJobs; i++ {
		jobNames = append(jobNames, fmt.Sprintf("job%d", i))
	}

	job := stream.NewJob("enqueue_all")
	enqueueJobs(jobNames, 10000)
	job.Complete(health.Success)

	workerPool := work.NewWorkerPool(workContext{}, 20, namespace, redisAdapter)
	for _, jobName := range jobNames {
		workerPool.Job(jobName, epsilonHandler)
	}
	go monitor()

	job = stream.NewJob("run_all")
	workerPool.Start()
	workerPool.Drain()
	job.Complete(health.Success)
	select {}
}

var totcount int64

func monitor() {
	t := time.Tick(1 * time.Second)

	curT := 0
	c1 := int64(0)
	c2 := int64(0)
	prev := int64(0)

DALOOP:
	for {
		select {
		case <-t:
			curT++
			v := atomic.AddInt64(&totcount, 0)
			fmt.Printf("after %d seconds, count is %d\n", curT, v)
			if curT == 1 {
				c1 = v
			} else if curT == 3 {
				c2 = v
			}
			if v == prev {
				break DALOOP
			}
			prev = v
		}
	}
	fmt.Println("Jobs/sec: ", float64(c2-c1)/2.0)
	os.Exit(0)
}

func enqueueJobs(jobs []string, count int) {
	enq := work.NewEnqueuer(namespace, redisAdapter)
	for _, jobName := range jobs {
		for i := 0; i < count; i++ {
			enq.Enqueue(jobName, work.Q{"i": i})
		}
	}
}

func cleanKeyspace(ctx context.Context) {
	keys, err := redisAdapter.Keys(ctx, namespace+"*")
	if err != nil {
		panic("could not get keys: " + err.Error())
	}
	for _, k := range keys {
		//fmt.Println("deleting ", k)
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
					return nil, err
				}
				return c, nil
			},
			Wait: true,
		}
		return redigoadapter.NewRedigoAdapter(pool)
	case "goredisv8":
		fallthrough
	default:
		rdb := goredisv8.NewClient(&goredisv8.Options{
			Addr:     addr,
			Password: "",
			DB:       0,
		})
		return goredisv8adapter.NewGoredisAdapter(rdb)
	}
}
