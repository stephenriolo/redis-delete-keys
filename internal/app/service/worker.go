package service

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

type Worker struct {
	redisConn *redis.Client
}

type WorkerSettings struct {
	URL                  string
	Password             string
	DB                   int
	ReadTimeoutInSeconds int
}

func NewWorker(settings WorkerSettings) (*Worker, error) {
	conn := redis.NewClient(&redis.Options{
		Addr:        settings.URL,
		Password:    settings.Password,
		DB:          settings.DB,
		ReadTimeout: time.Duration(settings.ReadTimeoutInSeconds) * time.Second,
	})

	// Verify the connection.
	_, err := conn.Ping(context.TODO()).Result()
	if err != nil {
		return nil, err
	}

	return &Worker{
		redisConn: conn,
	}, nil
}

func (w *Worker) Start(scanCount int64, pattern string, softMode bool) error {
	var cursor uint64
	var err error

	for {
		var tempKeys []string
		if tempKeys, cursor, err = w.redisConn.Scan(context.TODO(), cursor, pattern, scanCount).Result(); err != nil {
			if softMode {
				log.Printf("found the following keys %+v\n", tempKeys)
			}
			return fmt.Errorf("failed to get batch from scan with error %v", err.Error())
		}

		if !softMode && len(tempKeys) > 0 {
			cmd := w.redisConn.Unlink(context.TODO(), tempKeys...)
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
