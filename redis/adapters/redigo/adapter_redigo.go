package redigo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"

	workredis "github.com/GettEngineering/work/redis"
)

var _ workredis.Redis = (*RedigoAdapter)(nil)

type redisReplyBase interface {
	set(val any, err error)
}

type RedigoAdapter struct {
	pool      *redis.Pool
	multiConn redis.Conn
	stack     []redisReplyBase
}

func NewRedigoAdapter(pool *redis.Pool) *RedigoAdapter {
	return &RedigoAdapter{
		pool: pool,
	}
}

func (r *RedigoAdapter) NewScript(src string, keyCount int) workredis.Script {
	s := redis.NewScript(keyCount, src)

	return &redigoScript{
		pool:   r.pool,
		script: s,
	}
}

func (r *RedigoAdapter) Get(_ context.Context, key string) workredis.Reply {
	reply := redigoReply{}

	if r.multiConn == nil {
		conn := r.pool.Get()
		defer conn.Close()

		reply.reply, reply.err = conn.Do("GET", key)
	} else {
		reply.err = r.multiConn.Send("GET", key)
	}

	return &reply
}

func (r *RedigoAdapter) Decr(_ context.Context, key string) error {
	return r.do("DECR", key)
}

func (r *RedigoAdapter) Del(_ context.Context, keys ...string) error {
	args := make([]any, 0, len(keys))
	for _, key := range keys {
		args = append(args, key)
	}
	return r.do("DEL", args...)
}

func (r *RedigoAdapter) Exists(_ context.Context, key string) (bool, error) {
	reply, err := r.doInt64("EXISTS", key)

	return reply == 1, err
}

func (r *RedigoAdapter) Expire(_ context.Context, key string, expiration time.Duration) error {
	return r.do("EXPIRE", key, int(expiration.Seconds()))
}

func (r *RedigoAdapter) HIncrBy(_ context.Context, key, field string, incr int64) error {
	return r.do("HINCRBY", key, field, incr)
}

func (r *RedigoAdapter) HGet(_ context.Context, key, field string) workredis.Reply {
	reply := redigoReply{}

	if r.multiConn == nil {
		conn := r.pool.Get()
		defer conn.Close()

		reply.reply, reply.err = conn.Do("HGET", key, field)
	} else {
		reply.err = r.multiConn.Send("HGET", key, field)
	}

	return &reply
}

// Supports pipelining.
func (r *RedigoAdapter) HGetAll(_ context.Context, key string) workredis.StringStringMapReply {
	reply := redigoStringStringMapReply{}
	r.doWithReply(&reply, "HGETALL", key)

	return &reply
}

func (r *RedigoAdapter) HSet(_ context.Context, key string, fieldvals ...any) error {
	args := make([]any, 0, len(fieldvals)+1)
	args = append(args, key)
	args = append(args, fieldvals...)

	return r.do("HSET", args...)
}

func (r *RedigoAdapter) Incr(_ context.Context, key string) error {
	return r.do("INCR", key)
}

func (r *RedigoAdapter) Keys(_ context.Context, pattern string) ([]string, error) {
	return r.doStrings("KEYS", pattern)
}

// Supports pipelining.
func (r *RedigoAdapter) LIndex(_ context.Context, key string, index int64) workredis.Reply {
	reply := redigoReply{}
	r.doWithReply(&reply, "LINDEX", key, index)

	return &reply
}

// Supports pipelining.
func (r *RedigoAdapter) LLen(_ context.Context, key string) workredis.IntReply {
	reply := redigoIntReply{}
	r.doWithReply(&reply, "LLEN", key)

	return &reply
}

func (r *RedigoAdapter) LPush(_ context.Context, key string, value any) error {
	_, err := r.doInt64("LPUSH", key, value)
	return err
}

func (r *RedigoAdapter) LRem(_ context.Context, key string, value any) error {
	return r.do("LREM", key, 1, value)
}

func (r *RedigoAdapter) RPop(_ context.Context, key string) workredis.Reply {
	reply := redigoReply{}
	r.doWithReply(&reply, "RPOP", key)

	return &reply
}

func (r *RedigoAdapter) RPush(_ context.Context, key string, value any) error {
	_, err := r.doInt64("RPUSH", key, value)
	return err
}

func (r *RedigoAdapter) SAdd(_ context.Context, key string, members ...any) error {
	_, err := r.doInt64("SADD", append([]any{key}, members...)...)
	return err
}

func (r *RedigoAdapter) SCard(_ context.Context, key string) (int64, error) {
	return r.doInt64("SCARD", key)
}

func (r *RedigoAdapter) Set(_ context.Context, key string, value any) error {
	return r.do("SET", key, value)
}

func (r *RedigoAdapter) SIsMember(_ context.Context, key string, member any) (bool, error) {
	if r.multiConn == nil {
		conn := r.pool.Get()
		defer conn.Close()

		return redis.Bool(conn.Do("SISMEMBER", key, member))
	}

	err := r.multiConn.Send("SISMEMBER", key, member)
	return false, err
}

func (r *RedigoAdapter) SMembers(_ context.Context, key string) ([]string, error) {
	return r.doStrings("SMEMBERS", key)
}

func (r *RedigoAdapter) SRem(_ context.Context, key string, member any) error {
	return r.do("SREM", key, member)
}

func (r *RedigoAdapter) ZAdd(_ context.Context, key string, score float64, member any) error {
	_, err := r.doInt64("ZADD", key, score, member)
	return err
}

func (r *RedigoAdapter) ZCard(_ context.Context, key string) (int64, error) {
	return r.doInt64("ZCARD", key)
}

// Doesn't support pipelining, returns an error.
func (r *RedigoAdapter) ZRangeByScoreWithScores(
	_ context.Context,
	key string,
	min, max string,
	offset, count int64,
) (workredis.ZRanger, error) {
	if r.multiConn != nil {
		return nil, errors.New("cannot use ZRangeByScoreWithScores in a pipeline")
	}

	conn := r.pool.Get()
	defer conn.Close()

	reply, err := redis.Values(conn.Do("ZRANGEBYSCORE", key, min, max, "WITHSCORES", "LIMIT", offset, count))
	if err != nil {
		return nil, err
	}

	var vals []zval

	if err := redis.ScanSlice(reply, &vals); err != nil {
		return nil, fmt.Errorf("failed to scan ZRANGEBYSCORE reply: %w", err)
	}

	return newRedigoZrange(vals), nil
}

func (r *RedigoAdapter) ZRangeWithScores(
	_ context.Context,
	key string,
	start, stop int64,
) (workredis.ZRanger, error) {
	if r.multiConn != nil {
		return nil, errors.New("cannot use ZRangeWithScores in a pipeline")
	}

	conn := r.pool.Get()
	defer conn.Close()

	reply, err := redis.Values(conn.Do("ZRANGE", key, start, stop, "WITHSCORES"))
	if err != nil {
		return nil, err
	}

	var vals []zval

	if err := redis.ScanSlice(reply, &vals); err != nil {
		return nil, fmt.Errorf("failed to scan ZRANGE reply: %w", err)
	}

	return newRedigoZrange(vals), nil
}

func (r *RedigoAdapter) WithPipeline(_ context.Context, fn func(workredis.Redis) error) error {
	if r.multiConn != nil {
		return workredis.ErrPipeInProgress
	}

	rcopy := *r
	rcopy.multiConn = r.pool.Get()
	defer rcopy.multiConn.Close()

	if err := fn(&rcopy); err != nil {
		return err
	}

	if err := rcopy.multiConn.Flush(); err != nil {
		return err
	}

	for _, reply := range rcopy.stack {
		r, rerr := rcopy.multiConn.Receive()
		reply.set(r, rerr)
	}

	rcopy.stack = nil

	return nil
}

func (r *RedigoAdapter) WithMulti(_ context.Context, fn func(workredis.Redis) error) error {
	if r.multiConn != nil {
		return workredis.ErrPipeInProgress
	}

	rcopy := *r
	rcopy.multiConn = r.pool.Get()
	defer rcopy.multiConn.Close()

	if err := rcopy.multiConn.Send("MULTI"); err != nil {
		return err
	}

	if err := fn(&rcopy); err != nil {
		rcopy.multiConn.Do("DISCARD")
		return err
	}

	_, err := rcopy.multiConn.Do("EXEC")

	return err
}

func (r *RedigoAdapter) do(cmd string, args ...any) error {
	var err error

	if r.multiConn == nil {
		conn := r.pool.Get()
		defer conn.Close()

		_, err = conn.Do(cmd, args...)
	} else {
		err = r.multiConn.Send(cmd, args...)
	}

	return err
}

func (r *RedigoAdapter) doInt64(cmd string, args ...any) (int64, error) {
	var (
		reply int64
		err   error
	)

	if r.multiConn == nil {
		conn := r.pool.Get()
		defer conn.Close()

		reply, err = redis.Int64(conn.Do(cmd, args...))
	} else {
		err = r.multiConn.Send(cmd, args...)
	}

	return reply, err
}

func (r *RedigoAdapter) doStrings(cmd string, args ...any) ([]string, error) {
	var (
		reply []string
		err   error
	)

	if r.multiConn == nil {
		conn := r.pool.Get()
		defer conn.Close()

		reply, err = redis.Strings(conn.Do(cmd, args...))
	} else {
		err = r.multiConn.Send(cmd, args...)
	}

	return reply, err
}

func (r *RedigoAdapter) doWithReply(reply redisReplyBase, cmd string, args ...any) {
	if r.multiConn == nil {
		conn := r.pool.Get()
		defer conn.Close()

		reply.set(conn.Do(cmd, args...))
	} else {
		reply.set(nil, r.multiConn.Send(cmd, args...))
		r.stack = append(r.stack, reply)
	}
}

var (
	_ redisReplyBase                 = (*baseRedigoReply)(nil)
	_ workredis.Script               = (*redigoScript)(nil)
	_ workredis.ScriptReply          = (*redigoScriptReply)(nil)
	_ workredis.Reply                = (*redigoReply)(nil)
	_ workredis.IntReply             = (*redigoIntReply)(nil)
	_ workredis.StringStringMapReply = (*redigoStringStringMapReply)(nil)
)

type redigoScript struct {
	pool   *redis.Pool
	script *redis.Script
}

func (s redigoScript) Run(_ context.Context, keys []string, args ...any) workredis.ScriptReply {
	conn := s.pool.Get()
	defer conn.Close()

	keysAndArgs := make([]any, 0, len(keys)+len(args))
	for _, key := range keys {
		keysAndArgs = append(keysAndArgs, key)
	}
	keysAndArgs = append(keysAndArgs, args...)

	reply, err := s.script.Do(conn, keysAndArgs...)

	return redigoScriptReply{err: err, reply: reply}
}

type redigoScriptReply struct {
	err   error
	reply any
}

func (r redigoScriptReply) Err() error {
	if (r.err == nil && r.reply == nil) || errors.Is(r.err, redis.ErrNil) {
		return workredis.Nil
	}

	return r.err
}

func (r redigoScriptReply) Int64() (int64, error) {
	reply, err := redis.Int64(r.reply, r.err)
	if errors.Is(err, redis.ErrNil) {
		return 0, workredis.Nil
	}

	return reply, err
}

func (r redigoScriptReply) Slice() ([]any, error) {
	reply, err := redis.Values(r.reply, r.err)
	if errors.Is(err, redis.ErrNil) {
		return nil, workredis.Nil
	}

	return reply, err
}

func (r redigoScriptReply) Text() (string, error) {
	reply, err := redis.String(r.reply, r.err)
	if errors.Is(err, redis.ErrNil) {
		return "", workredis.Nil
	}

	return reply, err
}

type baseRedigoReply struct {
	err   error
	reply any
}

func (r *baseRedigoReply) set(val any, err error) {
	r.reply = val
	r.err = err
}

type redigoReply struct {
	baseRedigoReply
}

func (r *redigoReply) Bytes() ([]byte, error) {
	reply, err := redis.Bytes(r.reply, r.err)
	if errors.Is(err, redis.ErrNil) {
		return nil, workredis.Nil
	}

	return reply, err
}

func (r *redigoReply) Int64() (int64, error) {
	reply, err := redis.Int64(r.reply, r.err)
	if errors.Is(err, redis.ErrNil) {
		return 0, workredis.Nil
	}

	return reply, err
}

func (r *redigoReply) Result() (string, error) {
	reply, err := redis.String(r.reply, r.err)
	if errors.Is(err, redis.ErrNil) {
		return "", workredis.Nil
	}

	return reply, err
}

type redigoIntReply struct {
	baseRedigoReply
}

func (r *redigoIntReply) Result() (int64, error) {
	reply, err := redis.Int64(r.reply, r.err)
	if errors.Is(err, redis.ErrNil) {
		return 0, workredis.Nil
	}

	return reply, err
}

type redigoStringStringMapReply struct {
	baseRedigoReply
}

func (r *redigoStringStringMapReply) Result() (map[string]string, error) {
	vals, err := redis.Strings(r.reply, r.err)
	if errors.Is(err, redis.ErrNil) {
		return nil, workredis.Nil
	}

	m := make(map[string]string, len(vals)/2)
	for i := 0; i < len(vals)-1; i += 2 {
		key := vals[i]
		value := vals[i+1]

		m[key] = value
	}

	return m, err
}

type zval struct {
	Member string
	Score  float64
}

type redigoZrange struct {
	vals    []zval
	i       int
	lastInd int
}

func newRedigoZrange(vals []zval) *redigoZrange {
	return &redigoZrange{
		vals:    vals,
		i:       -1,
		lastInd: len(vals) - 1,
	}
}

func (z *redigoZrange) Len() int {
	return len(z.vals)
}

func (z *redigoZrange) Next() bool {
	if z.i >= z.lastInd {
		return false
	}

	z.i++

	return true
}

func (z *redigoZrange) Val() (any, float64) {
	if z.i == -1 {
		return nil, 0
	}
	return z.vals[z.i].Member, z.vals[z.i].Score
}
