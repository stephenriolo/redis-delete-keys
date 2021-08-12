package main

import (
	"log"
)

const (
	redisConn                 = ""
	redisPass                 = ""
	redisDB                   = 0
	redisReadTimeoutInSeconds = 20

	redisScanCount    = 50
	redisMatchPattern = ""
)

func main() {
	worker, err := NewWorker(WorkerSettings{
		url:                  redisConn,
		password:             redisPass,
		db:                   redisDB,
		readTimeoutInSeconds: redisReadTimeoutInSeconds,
	})

	if err != nil {
		log.Fatalf("failed to start worker with error %v", err.Error())
	}

	err = worker.Start(redisScanCount, redisMatchPattern)
}
