package service

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/ory/dockertest"
	"github.com/stretchr/testify/require"
)

const (
	testRedisPass                 = ""
	testRedisDB                   = 0
	testRedisReadTimeoutInSeconds = 20

	testRedisScanCount         = 50
	testRedisInsertKey1        = "key:test:%d"
	testRedisInsertKey2        = "key2:test:%d"
	testRedisMatchPatternKeys1 = "key:*"
	testRedisMatchPatternKeys2 = "key2:*"

	testElementsToCreate = 100000
)

var testRedisURL = ""

func TestMain(m *testing.M) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resourceRedis, err := pool.Run("redis", "5-alpine", nil)
	if err != nil {
		log.Fatalf("Failed to start redis: %+v", err)
	}

	// determine the port the container is listening on
	testRedisURL = net.JoinHostPort("localhost", resourceRedis.GetPort("6379/tcp"))

	// wait for the container to be ready
	err = pool.Retry(func() error {
		var e error
		client := redis.NewClient(&redis.Options{Addr: testRedisURL})
		defer client.Close()

		_, e = client.Ping(context.TODO()).Result()
		return e
	})

	if err != nil {
		log.Fatalf("Failed to ping Redis: %+v", err)
	}

	code := m.Run()

	if err := pool.Purge(resourceRedis); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	// Run tests.
	os.Exit(code)
}

func TestWorker(t *testing.T) {
	worker, err := NewWorker(WorkerSettings{
		URL:                  testRedisURL,
		Password:             testRedisPass,
		DB:                   testRedisDB,
		ReadTimeoutInSeconds: testRedisReadTimeoutInSeconds,
	})

	require.NoError(t, err)
	redisPipe := worker.redisConn.Pipeline()
	log.Println("Inserting elements with key prefix")
	for i := 0; i < testElementsToCreate; i++ {
		redisPipe.Set(context.TODO(), fmt.Sprintf(testRedisInsertKey1, i), "", time.Hour)
	}
	_, err = redisPipe.Exec(context.TODO())
	require.NoError(t, err)

	log.Println("Inserting elements with key2 prefix")
	for i := 0; i < testElementsToCreate; i++ {
		redisPipe.Set(context.TODO(), fmt.Sprintf(testRedisInsertKey2, i), "", time.Hour)
	}
	_, err = redisPipe.Exec(context.TODO())
	require.NoError(t, err)

	sliceCmd := worker.redisConn.Keys(context.TODO(), testRedisMatchPatternKeys1)
	require.NoError(t, sliceCmd.Err())
	keys1, err := sliceCmd.Result()
	require.NoError(t, err)
	require.Len(t, keys1, testElementsToCreate)

	sliceCmd = worker.redisConn.Keys(context.TODO(), testRedisMatchPatternKeys2)
	require.NoError(t, sliceCmd.Err())
	keys2, err := sliceCmd.Result()
	require.NoError(t, err)
	require.Len(t, keys2, testElementsToCreate)

	log.Println("Starting to delete keys with pattern keys:*")
	err = worker.Start(testRedisScanCount, testRedisMatchPatternKeys1, false)
	require.NoError(t, err)

	sliceCmd = worker.redisConn.Keys(context.TODO(), testRedisMatchPatternKeys1)
	require.NoError(t, sliceCmd.Err())
	keys1, err = sliceCmd.Result()
	require.NoError(t, err)
	require.Len(t, keys1, 0)

	sliceCmd = worker.redisConn.Keys(context.TODO(), testRedisMatchPatternKeys2)
	require.NoError(t, sliceCmd.Err())
	keys2, err = sliceCmd.Result()
	require.NoError(t, err)
	require.Len(t, keys2, testElementsToCreate)
}
