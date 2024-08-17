package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	goredisv8 "github.com/go-redis/redis/v8"
	"github.com/gomodule/redigo/redis"

	"github.com/GettEngineering/work"
	workredis "github.com/GettEngineering/work/redis"
	goredisv8adapter "github.com/GettEngineering/work/redis/adapters/goredisv8"
	redigoadapter "github.com/GettEngineering/work/redis/adapters/redigo"
)

var redisHostPort = flag.String("redis", ":6379", "redis hostport")
var redisNamespace = flag.String("ns", "work", "redis namespace")
var jobName = flag.String("job", "", "job name")
var jobArgs = flag.String("args", "{}", "job arguments")

func main() {
	flag.Parse()

	if *jobName == "" {
		fmt.Println("no job specified")
		os.Exit(1)
	}

	redisAdapter := newRedis(*redisHostPort)

	var args map[string]interface{}
	err := json.Unmarshal([]byte(*jobArgs), &args)
	if err != nil {
		fmt.Println("invalid args:", err)
		os.Exit(1)
	}

	en := work.NewEnqueuer(*redisNamespace, redisAdapter)
	en.Enqueue(*jobName, args)
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
