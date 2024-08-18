package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	goredisv8 "github.com/go-redis/redis/v8"
	"github.com/gomodule/redigo/redis"

	workredis "github.com/GettEngineering/work/redis"
	goredisv8adapter "github.com/GettEngineering/work/redis/adapters/goredisv8"
	redigoadapter "github.com/GettEngineering/work/redis/adapters/redigo"
	"github.com/GettEngineering/work/webui"
)

var (
	redisHostPort  = flag.String("redis", ":6379", "redis hostport")
	redisDatabase  = flag.String("database", "0", "redis database")
	redisNamespace = flag.String("ns", "work", "redis namespace")
	webHostPort    = flag.String("listen", ":5040", "hostport to listen for HTTP JSON API")
)

func main() {
	flag.Parse()

	log.Println("Starting workwebui:")
	log.Println("redis = ", *redisHostPort)
	log.Println("database = ", *redisDatabase)
	log.Println("namespace = ", *redisNamespace)
	log.Println("listen = ", *webHostPort)

	database, err := strconv.Atoi(*redisDatabase)
	if err != nil {
		log.Printf("Error: %v is not a valid database value", *redisDatabase)
		return
	}

	redisAdapter := newRedis(*redisHostPort, database)

	server := webui.NewServer(*redisNamespace, redisAdapter, *webHostPort)
	server.Start()

	c := make(chan os.Signal, 1)
	//nolint:staticcheck // os.Kill is for Windows compatibility
	signal.Notify(c, os.Kill, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	<-c

	server.Stop()

	log.Println("\nQuitting...")
}

func newRedis(addr string, database int) workredis.Redis {
	switch os.Getenv("REDIS_ADAPTER") {
	case "redigo":
		pool := &redis.Pool{
			MaxActive:   3,
			MaxIdle:     3,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				return redis.DialURL(addr, redis.DialDatabase(database))
			},
			Wait: true,
		}
		return redigoadapter.NewAdapter(pool)
	default:
		rdb := goredisv8.NewClient(&goredisv8.Options{
			Addr:     addr,
			Password: "",
			DB:       database,
		})
		return goredisv8adapter.NewAdapter(rdb)
	}
}
