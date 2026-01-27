package database

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"unbound-future-backend/config"
)

var RDB *redis.Client

func InitRedis(cfg *config.Config) error {
	opt, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		return fmt.Errorf("failed to parse redis url: %w", err)
	}

	// 设置超时时间，避免网络问题导致长时间阻塞
	opt.DialTimeout = 10 * time.Second
	opt.ReadTimeout = 30 * time.Second  // 增加读取超时时间
	opt.WriteTimeout = 10 * time.Second
	opt.PoolTimeout = 10 * time.Second
	opt.PoolSize = 100  // 增加连接池大小
	opt.MinIdleConns = 10  // 最小空闲连接数

	RDB = redis.NewClient(opt)

	if err := RDB.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("failed to connect to redis: %w", err)
	}

	return nil
}
