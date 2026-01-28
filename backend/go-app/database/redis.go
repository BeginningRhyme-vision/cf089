package database

import (
	"context"
	"fmt"
	"log"
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
	// 对于大批次写入操作，需要更长的超时时间
	opt.DialTimeout = 10 * time.Second
	opt.ReadTimeout = 60 * time.Second  // 增加读取超时时间（用于大批次读取）
	opt.WriteTimeout = 60 * time.Second // 增加写入超时时间（用于大批次写入，如 2000 个任务的 pipeline）
	opt.PoolTimeout = 30 * time.Second  // 增加连接池超时
	opt.PoolSize = 200  // 增加连接池大小，避免 BatchFetch 和 AddTasksToJob 之间的连接竞争
	opt.MinIdleConns = 20  // 最小空闲连接数，确保有足够的连接可用

	log.Printf("Initializing Redis: addr=%s db=%d username=%s (from config)", opt.Addr, opt.DB, opt.Username)
	RDB = redis.NewClient(opt)

	if err := RDB.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("failed to connect to redis: %w", err)
	}
	log.Printf("Redis connected successfully: addr=%s db=%d", opt.Addr, opt.DB)

	return nil
}
