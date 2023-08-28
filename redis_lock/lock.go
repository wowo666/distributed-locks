package rlock

import (
	"context"
	_ "embed"
	"errors"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/google/uuid"
)

var (
	// go:embed lua/unlock.lua
	luaUnlock string

	ErrFailedToAddLock = errors.New("竞争锁失败")
	ErrLockNotHeld     = errors.New("未获取到锁")
)

type Client struct {
	client *redis.Pool
	// genValFc 用于生成值，将来可以考虑暴露出去允许用户自定义
	genValFc
}

type GenLockValue interface {
	Value() string
}

func NewClient(c *redis.Pool) *Client {
	return &Client{
		client: c,
	}
}

type Locker struct {
	client     *redis.Pool
	key        string
	value      string
	expiration time.Duration
}

// 锁
func newLocker(client *redis.Pool, key string, val string, expiration time.Duration) *Locker {
	return &Locker{
		client:     client,
		key:        key,
		value:      val,
		expiration: expiration,
	}
}

// Lock 加锁
func (c *Client) Lock(ctx context.Context, key string, expiration time.Duration) (*Locker, error) {
	value := uuid.New().String()
	conn := c.client.Get()
	defer conn.Close()

	res, err := redis.String(conn.Do("SET", key, value, "EX", expiration.Seconds(), "NX"))
	if err != nil {
		return nil, err
	}
	if res != "OK" {
		return nil, ErrFailedToAddLock
	}
	return newLocker(c.client, key, value, expiration), nil
}

// UnLock 释放锁
func (l *Locker) UnLock(ctx context.Context) error {
	conn := l.client.Get()
	defer conn.Close()
	res, err := redis.Int64(redis.NewScript(1, luaUnlock).Do(conn, l.key, l.value))
	if err != nil {
		return err
	}
	if err == redis.ErrNil {
		return ErrLockNotHeld
	}
	if err != nil {
		return err
	}
	if res != 1 {
		return ErrLockNotHeld
	}
	return nil
}
