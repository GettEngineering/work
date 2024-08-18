package redis

import (
	"context"
	"errors"
	"time"
)

var (
	Nil               = errors.New("redis: nil") //nolint:errname,revive,stylecheck // the same name as in the go-redis package
	ErrPipeInProgress = errors.New("multi or pipeline already in progress")
)

type Script interface {
	Run(ctx context.Context, keys []string, args ...any) ScriptReply
}

type ScriptReply interface {
	Err() error
	Int64() (int64, error)
	Slice() ([]any, error)
	Text() (string, error)
}

type Reply interface {
	Bytes() ([]byte, error)
	Int64() (int64, error)
	Result() (string, error)
}

type IntReply interface {
	Result() (int64, error)
}

type StringStringMapReply interface {
	Result() (map[string]string, error)
}

type ZRanger interface {
	Next() bool
	Val() (any, float64)
	Len() int
}

type Redis interface {
	NewScript(src string, keyCount int) Script

	// Get gets the value of key. If the key does not exist, the reply returns RedisNil.
	Get(ctx context.Context, key string) Reply
	// Decr decrements the integer value of key by one.
	Decr(ctx context.Context, key string) error
	// Del removes the specified keys.
	Del(ctx context.Context, keys ...string) error
	// Exists returns true if key exists.
	Exists(ctx context.Context, key string) (bool, error)
	// Expire sets a timeout on key without any options.
	Expire(ctx context.Context, key string, expiration time.Duration) error
	// HGet returns the value associated with field in the hash stored at key.
	HGet(ctx context.Context, key, field string) Reply
	// HGetAll returns all fields and values of the hash stored at key.
	HGetAll(ctx context.Context, key string) StringStringMapReply
	// HIncrBy increments the number stored at field in the hash stored at key by incr.
	HIncrBy(ctx context.Context, key, field string, incr int64) error
	// HSet sets the specified fields to their respective values in the hash stored at key.
	// This command overwrites the values of specified fields that exist in the hash.
	// If key doesn't exist, a new key holding a hash is created.
	HSet(ctx context.Context, key string, fieldvals ...any) error
	// Incr increments the number stored at key by one.
	Incr(ctx context.Context, key string) error
	// Keys returns all keys matching pattern.
	Keys(ctx context.Context, pattern string) ([]string, error)
	// LIndex returns the element at index index in the list stored at key.
	LIndex(ctx context.Context, key string, index int64) Reply
	// LLen returns the length of the list stored at key.
	LLen(ctx context.Context, key string) IntReply
	// LPush insert the specified value at the head of the list stored at key.
	LPush(ctx context.Context, key string, value any) error
	// LRem removes the first element that equals to value from the list stored at key.
	LRem(ctx context.Context, key string, value any) error
	// RPop removes and returns the last elements of the list stored at key.
	RPop(ctx context.Context, key string) Reply
	// RPush inserts the specified value at the tail of the list stored at key.
	RPush(ctx context.Context, key string, value any) error
	// SAdd adds the specified members to the set stored at key.
	SAdd(ctx context.Context, key string, members ...any) error
	// SCard returns the number of elements in the set stored at key.
	SCard(ctx context.Context, key string) (int64, error)
	// Set sets key to hold the string value without any options.
	Set(ctx context.Context, key string, value any) error
	// SIsMemeber returns true if member is a member of the set stored at key.
	SIsMember(ctx context.Context, key string, member any) (bool, error)
	// SMembers returns all the members of the set value stored at key.
	SMembers(ctx context.Context, key string) ([]string, error)
	// Remove the specified member from the set stored at key. If specified members is not a member of this set,
	// it's ignored. Does nothing if key does not exist.
	SRem(ctx context.Context, key string, member any) error
	// ZAdd adds the specified member with a score to the sorted set stored at key
	// without any options.
	ZAdd(ctx context.Context, key string, score float64, member any) error
	// ZCard returns the number of elements in the sorted set stored at key.
	ZCard(ctx context.Context, key string) (int64, error)
	// ZRangeByScoreWithScores returns all the elements with their scores in the sorted set at key with a score between
	// min and max (including elements with score equal to min or max).
	ZRangeByScoreWithScores(ctx context.Context, key string, minScore, maxScore string, offset, count int64) (ZRanger, error)
	// ZRangeWithScores returns all the elements with their scores in the sorted set at key with a rank between
	// start and stop.
	ZRangeWithScores(ctx context.Context, key string, start, stop int64) (ZRanger, error)
	// WithPipeline sends commands in a batch.
	// Redis commands cannot be called in different goroutines inside the function f.
	WithPipeline(ctx context.Context, f func(Redis) error) error
	// WithMulti wraps queued commands with MULTI/EXEC.
	// Redis commands cannot be called in different goroutines inside the function f.
	WithMulti(ctx context.Context, f func(Redis) error) error
}
