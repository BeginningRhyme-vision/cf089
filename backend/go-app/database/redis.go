package database

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"unbound-future-backend/config"
)

var RDB *redis.Client

func InitRedis(cfg *config.Config) error {
	opt, err := redis.ParseURL(cfg.Redis.URL)
	if err != nil {
		return fmt.Errorf("failed to parse redis url: %w", err)
	}

	RDB = redis.NewClient(opt)

	if err := RDB.Ping(context.Background()).Err(); err != nil {
		return fmt.Errorf("failed to connect to redis: %w", err)
	}

	return nil
}
