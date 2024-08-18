package goredisv8

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"

	workredis "github.com/GettEngineering/work/redis"
)

var _ workredis.Redis = (*Adapter)(nil)

type Adapter struct {
	rds  *redis.Client
	pipe redis.Pipeliner
}

func NewAdapter(rds *redis.Client) *Adapter {
	return &Adapter{
		rds: rds,
	}
}

func (r *Adapter) NewScript(src string, _ int) workredis.Script {
	return goredisScript{
		rds:    r.rds,
		script: redis.NewScript(src),
	}
}

func (r *Adapter) Get(ctx context.Context, key string) workredis.Reply {
	if r.pipe != nil {
		return &goredisReply{reply: r.pipe.Get(ctx, key)}
	}

	return &goredisReply{reply: r.rds.Get(ctx, key)}
}

func (r *Adapter) Decr(ctx context.Context, key string) error {
	if r.pipe != nil {
		_, err := r.pipe.Decr(ctx, key).Result()
		return err
	}

	_, err := r.pipe.Decr(ctx, key).Result()

	return err
}

func (r *Adapter) Del(ctx context.Context, keys ...string) error {
	if r.pipe != nil {
		return r.pipe.Del(ctx, keys...).Err()
	}

	return r.rds.Del(ctx, keys...).Err()
}

func (r *Adapter) Exists(ctx context.Context, key string) (bool, error) {
	var (
		reply int64
		err   error
	)

	if r.pipe != nil {
		reply, err = r.pipe.Exists(ctx, key).Result()
	} else {
		reply, err = r.rds.Exists(ctx, key).Result()
	}

	return reply == 1, err
}

func (r *Adapter) Expire(ctx context.Context, key string, expiration time.Duration) error {
	if r.pipe != nil {
		return r.pipe.Expire(ctx, key, expiration).Err()
	}

	return r.rds.Expire(ctx, key, expiration).Err()
}

func (r *Adapter) HGet(ctx context.Context, key, field string) workredis.Reply {
	if r.pipe != nil {
		return &goredisReply{reply: r.pipe.HGet(ctx, key, field)}
	}

	return &goredisReply{r.rds.HGet(ctx, key, field)}
}

func (r *Adapter) HGetAll(ctx context.Context, key string) workredis.StringStringMapReply {
	if r.pipe != nil {
		return r.pipe.HGetAll(ctx, key)
	}

	return r.rds.HGetAll(ctx, key)
}

func (r *Adapter) HIncrBy(ctx context.Context, key, field string, incr int64) error {
	if r.pipe != nil {
		_, err := r.pipe.HIncrBy(ctx, key, field, incr).Result()
		return err
	}

	_, err := r.rds.HIncrBy(ctx, key, field, incr).Result()

	return err
}

func (r *Adapter) HSet(ctx context.Context, key string, fieldvals ...any) error {
	if r.pipe != nil {
		return r.pipe.HSet(ctx, key, fieldvals...).Err()
	}

	return r.rds.HSet(ctx, key, fieldvals...).Err()
}

func (r *Adapter) Incr(ctx context.Context, key string) error {
	if r.pipe != nil {
		return r.pipe.Incr(ctx, key).Err()
	}

	return r.rds.Incr(ctx, key).Err()
}

func (r *Adapter) Keys(ctx context.Context, pattern string) ([]string, error) {
	if r.pipe != nil {
		return r.pipe.Keys(ctx, pattern).Result()
	}

	return r.rds.Keys(ctx, pattern).Result()
}

func (r *Adapter) LIndex(ctx context.Context, key string, index int64) workredis.Reply {
	if r.pipe != nil {
		return &goredisReply{reply: r.pipe.LIndex(ctx, key, index)}
	}

	return &goredisReply{reply: r.rds.LIndex(ctx, key, index)}
}

func (r *Adapter) LLen(ctx context.Context, key string) workredis.IntReply {
	if r.pipe != nil {
		return r.pipe.LLen(ctx, key)
	}

	return r.rds.LLen(ctx, key)
}

func (r *Adapter) LPush(ctx context.Context, key string, value any) error {
	if r.pipe != nil {
		return r.pipe.LPush(ctx, key, value).Err()
	}

	return r.rds.LPush(ctx, key, value).Err()
}

func (r *Adapter) LRem(ctx context.Context, key string, value any) error {
	if r.pipe != nil {
		_, err := r.pipe.LRem(ctx, key, 1, value).Result()
		return err
	}

	_, err := r.rds.LRem(ctx, key, 1, value).Result()

	return err
}

func (r *Adapter) RPop(ctx context.Context, key string) workredis.Reply {
	if r.pipe != nil {
		return &goredisReply{reply: r.pipe.RPop(ctx, key)}
	}

	return &goredisReply{reply: r.rds.RPop(ctx, key)}
}

func (r *Adapter) RPush(ctx context.Context, key string, value any) error {
	if r.pipe != nil {
		return r.pipe.RPush(ctx, key, value).Err()
	}

	return r.rds.RPush(ctx, key, value).Err()
}

func (r *Adapter) SAdd(ctx context.Context, key string, members ...any) error {
	if r.pipe != nil {
		return r.pipe.SAdd(ctx, key, members...).Err()
	}

	return r.rds.SAdd(ctx, key, members...).Err()
}

func (r *Adapter) SCard(ctx context.Context, key string) (int64, error) {
	if r.pipe != nil {
		return r.pipe.SCard(ctx, key).Result()
	}

	return r.rds.SCard(ctx, key).Result()
}

func (r *Adapter) Set(ctx context.Context, key string, value any) error {
	if r.pipe != nil {
		return r.pipe.Set(ctx, key, value, 0).Err()
	}

	return r.rds.Set(ctx, key, value, 0).Err()
}

func (r *Adapter) SIsMember(ctx context.Context, key string, member any) (bool, error) {
	if r.pipe != nil {
		return r.pipe.SIsMember(ctx, key, member).Result()
	}

	return r.rds.SIsMember(ctx, key, member).Result()
}

func (r *Adapter) SMembers(ctx context.Context, key string) ([]string, error) {
	if r.pipe != nil {
		return r.pipe.SMembers(ctx, key).Result()
	}

	return r.rds.SMembers(ctx, key).Result()
}

func (r *Adapter) SRem(ctx context.Context, key string, member any) error {
	if r.pipe != nil {
		_, err := r.pipe.SRem(ctx, key, member).Result()
		return err
	}

	_, err := r.rds.SRem(ctx, key, member).Result()

	return err
}

func (r *Adapter) ZAdd(ctx context.Context, key string, score float64, member any) error {
	if r.pipe != nil {
		return r.pipe.ZAdd(ctx, key, &redis.Z{Score: score, Member: member}).Err()
	}

	return r.rds.ZAdd(ctx, key, &redis.Z{Score: score, Member: member}).Err()
}

func (r *Adapter) ZCard(ctx context.Context, key string) (int64, error) {
	if r.pipe != nil {
		return r.pipe.ZCard(ctx, key).Result()
	}

	return r.rds.ZCard(ctx, key).Result()
}

func (r *Adapter) ZRangeByScoreWithScores(
	ctx context.Context,
	key string,
	minScore, maxScore string,
	offset, count int64,
) (workredis.ZRanger, error) {
	var zres *redis.ZSliceCmd

	args := &redis.ZRangeBy{Min: minScore, Max: maxScore, Offset: offset, Count: count}

	if r.pipe != nil {
		zres = r.pipe.ZRangeByScoreWithScores(ctx, key, args)
	} else {
		zres = r.rds.ZRangeByScoreWithScores(ctx, key, args)
	}

	zs, err := zres.Result()
	if err != nil {
		return nil, err
	}

	return newGoredisZrange(zs), nil
}

func (r *Adapter) ZRangeWithScores(
	ctx context.Context,
	key string,
	start, stop int64,
) (workredis.ZRanger, error) {
	var zres *redis.ZSliceCmd

	if r.pipe != nil {
		zres = r.pipe.ZRangeWithScores(ctx, key, start, stop)
	} else {
		zres = r.rds.ZRangeWithScores(ctx, key, start, stop)
	}

	zs, err := zres.Result()
	if err != nil {
		return nil, err
	}

	return newGoredisZrange(zs), nil
}

func (r *Adapter) WithPipeline(ctx context.Context, fn func(workredis.Redis) error) error {
	if r.pipe != nil {
		return workredis.ErrPipeInProgress
	}

	rcopy := *r
	rcopy.pipe = r.rds.Pipeline()

	if err := fn(&rcopy); err != nil {
		if discardErr := rcopy.pipe.Discard(); discardErr != nil {
			return errors.Join(err, discardErr)
		}
		return err
	}

	_, err := rcopy.pipe.Exec(ctx)

	return err
}

func (r *Adapter) WithMulti(ctx context.Context, fn func(workredis.Redis) error) error {
	if r.pipe != nil {
		return workredis.ErrPipeInProgress
	}

	rcopy := *r
	rcopy.pipe = r.rds.TxPipeline()

	if err := fn(&rcopy); err != nil {
		if discardErr := rcopy.pipe.Discard(); discardErr != nil {
			return errors.Join(err, discardErr)
		}
		return err
	}

	_, err := rcopy.pipe.Exec(ctx)

	return err
}

var (
	_ workredis.Script      = (*goredisScript)(nil)
	_ workredis.ScriptReply = (*goredisScriptReply)(nil)
	_ workredis.Reply       = (*goredisReply)(nil)
	_ workredis.ZRanger     = (*goredisZrange)(nil)
)

type goredisScript struct {
	rds    *redis.Client
	script *redis.Script
}

func (s goredisScript) Run(ctx context.Context, keys []string, args ...any) workredis.ScriptReply {
	return goredisScriptReply{
		reply: s.script.Run(ctx, s.rds, keys, args...),
	}
}

type goredisScriptReply struct {
	reply *redis.Cmd
}

func (r goredisScriptReply) Err() error {
	err := r.reply.Err()
	if errors.Is(err, redis.Nil) {
		return workredis.Nil
	}

	return err
}

func (r goredisScriptReply) Int64() (int64, error) {
	res, err := r.reply.Int64()
	if errors.Is(err, redis.Nil) {
		return 0, workredis.Nil
	}

	return res, err
}

func (r goredisScriptReply) Slice() ([]any, error) {
	res, err := r.reply.Slice()
	if errors.Is(err, redis.Nil) {
		return nil, workredis.Nil
	}

	return res, err
}

func (r goredisScriptReply) Text() (string, error) {
	res, err := r.reply.Text()
	if errors.Is(err, redis.Nil) {
		return "", workredis.Nil
	}

	return res, err
}

type goredisReply struct {
	reply *redis.StringCmd
}

func (r *goredisReply) Bytes() ([]byte, error) {
	reply, err := r.reply.Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, workredis.Nil
	}

	return reply, err
}

func (r *goredisReply) Int64() (int64, error) {
	reply, err := r.reply.Int64()
	if errors.Is(err, redis.Nil) {
		return 0, workredis.Nil
	}

	return reply, err
}

func (r *goredisReply) Result() (string, error) {
	reply, err := r.reply.Result()
	if errors.Is(err, redis.Nil) {
		return "", workredis.Nil
	}

	return reply, err
}

type goredisZrange struct {
	zs      []redis.Z
	i       int
	lastInd int
}

func newGoredisZrange(zs []redis.Z) *goredisZrange {
	return &goredisZrange{
		zs:      zs,
		i:       -1,
		lastInd: len(zs) - 1,
	}
}

func (z *goredisZrange) Len() int {
	return len(z.zs)
}

func (z *goredisZrange) Next() bool {
	if z.i >= z.lastInd {
		return false
	}

	z.i++

	return true
}

func (z *goredisZrange) Val() (member any, score float64) {
	if z.i == -1 {
		return nil, 0
	}
	return z.zs[z.i].Member, z.zs[z.i].Score
}
