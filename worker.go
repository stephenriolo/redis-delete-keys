package main

import (
	"fmt"
	"time"

	"github.com/go-redis/redis"
)

type Worker struct {
	redisConn *redis.Client
}

type WorkerSettings struct {
	url                  string
	password             string
	db                   int
	readTimeoutInSeconds int
}

func NewWorker(settings WorkerSettings) (*Worker, error) {
	conn := redis.NewClient(&redis.Options{
		Addr:        settings.url,
		Password:    settings.password,
		DB:          settings.db,
		ReadTimeout: time.Duration(settings.readTimeoutInSeconds) * time.Second,
	})

	// Verify the connection.
	_, err := conn.Ping().Result()
	if err != nil {
		return nil, err
	}

	return &Worker{
		redisConn: conn,
	}, nil
}

func (w *Worker) Start(scanCount int64, pattern string) error {
	var cursor uint64
	var err error

	for {
		var tempKeys []string
		if tempKeys, cursor, err = w.redisConn.Scan(cursor, pattern, scanCount).Result(); err != nil {
			return fmt.Errorf("failed to get batch from scan with error %v", err.Error())
		}

		if len(tempKeys) > 0 {
			cmd := w.redisConn.Unlink(tempKeys...)
			if cmd.Err() != nil {
				return fmt.Errorf("failed to unlink batch from redis with error %v", err.Error())
			}
		}

		if cursor == 0 {
			break
		}
	}

	return nil
}
