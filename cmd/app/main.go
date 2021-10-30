package main

import (
	"log"

	"github.com/stephenriolo/redis-delete-keys/internal/app/service"
)

var (
	redisConn                 = ""
	redisPass                 = ""
	redisDB                   = 0
	redisReadTimeoutInSeconds = 20

	redisScanCount    = 50
	redisMatchPattern = ""
)

func main() {
	worker, err := service.NewWorker(service.WorkerSettings{
		URL:                  redisConn,
		Password:             redisPass,
		DB:                   redisDB,
		ReadTimeoutInSeconds: redisReadTimeoutInSeconds,
	})

	if err != nil {
		log.Fatalf("failed to start worker with error %v", err.Error())
	}

	err = worker.Start(int64(redisScanCount), redisMatchPattern, true)
	log.Print(err)
}
