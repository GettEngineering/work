package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"time"

	goredisv8 "github.com/go-redis/redis/v8"
	"github.com/gomodule/redigo/redis"

	"github.com/GettEngineering/work"
	workredis "github.com/GettEngineering/work/redis"
	goredisv8adapter "github.com/GettEngineering/work/redis/adapters/goredisv8"
	redigoadapter "github.com/GettEngineering/work/redis/adapters/redigo"
)

var (
	redisHostPort  = flag.String("redis", ":6379", "redis hostport")
	redisNamespace = flag.String("ns", "work", "redis namespace")
)

func epsilonHandler(_ *work.Job) error {
	log.Println("epsilon")
	time.Sleep(time.Second)

	if rand.IntN(2) == 0 { //nolint:gosec // we don't need a crypto strong random number here
		return fmt.Errorf("random error")
	}
	return nil
}

type workContext struct{}

func main() {
	ctx := context.Background()
	flag.Parse()
	log.Println("Installing some fake data")

	redisAdapter := newRedis(*redisHostPort)
	cleanKeyspace(ctx, redisAdapter, *redisNamespace)

	// Enqueue some jobs:
	go func() {
		err := redisAdapter.SAdd(ctx, *redisNamespace+":known_jobs", "foobar")
		if err != nil {
			panic("could not add known_jobs: " + err.Error())
		}
	}()

	go func() {
		for {
			en := work.NewEnqueuer(*redisNamespace, redisAdapter)
			for i := 0; i < 20; i++ {
				_, err := en.Enqueue("foobar", work.Q{"i": i})
				if err != nil {
					panic("could not enqueue: " + err.Error())
				}
			}

			time.Sleep(1 * time.Second)
		}
	}()

	wp := work.NewWorkerPool(workContext{}, 5, *redisNamespace, redisAdapter)
	wp.Job("foobar", epsilonHandler)
	wp.Start()

	select {}
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

func cleanKeyspace(ctx context.Context, redisAdpter workredis.Redis, namespace string) {
	keys, err := redisAdpter.Keys(ctx, namespace+"*")
	if err != nil {
		panic("could not get keys: " + err.Error())
	}
	for _, k := range keys {
		if err := redisAdpter.Del(ctx, k); err != nil {
			panic("could not del: " + err.Error())
		}
	}
}
