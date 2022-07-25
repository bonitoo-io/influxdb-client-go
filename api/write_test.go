// Copyright 2020-2021 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package api

import (
	"fmt"
	iwrite "github.com/influxdata/influxdb-client-go/v2/internal/write"
	ilog "log"
	"math"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/influxdata/influxdb-client-go/v2/api/http"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/influxdata/influxdb-client-go/v2/internal/test"
	"github.com/influxdata/influxdb-client-go/v2/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteAPIWriteDefaultTag(t *testing.T) {
	service := test.NewTestService(t, "http://localhost:8888")
	opts := write.DefaultOptions().
		SetBatchSize(1)
	opts.AddDefaultTag("dft", "a")
	writeAPI := NewWriteAPI("my-org", "my-bucket", service, opts)
	point := write.NewPoint("test",
		map[string]string{
			"vendor": "AWS",
		},
		map[string]interface{}{
			"mem_free": 1234567,
		}, time.Unix(60, 60))
	writeAPI.WritePoint(point)
	writeAPI.Close()
	require.Len(t, service.Lines(), 1)
	assert.Equal(t, "test,dft=a,vendor=AWS mem_free=1234567i 60000000060", service.Lines()[0])
}

func TestWriteAPIImpl_Write(t *testing.T) {
	service := test.NewTestService(t, "http://localhost:8888")
	writeAPI := NewWriteAPI("my-org", "my-bucket", service, write.DefaultOptions().SetBatchSize(5))
	points := test.GenPoints(10)
	for _, p := range points {
		writeAPI.WritePoint(p)
	}
	writeAPI.Close()
	require.Len(t, service.Lines(), 10)
	for i, p := range points {
		line := write.PointToLineProtocol(p, writeAPI.writeOptions.Precision())
		//cut off last \n char
		line = line[:len(line)-1]
		assert.Equal(t, service.Lines()[i], line)
	}
}

func TestGzipWithFlushing(t *testing.T) {
	service := test.NewTestService(t, "http://localhost:8888")
	log.Log.SetLogLevel(log.DebugLevel)
	writeAPI := NewWriteAPI("my-org", "my-bucket", service, write.DefaultOptions().SetBatchSize(5).SetUseGZip(true))
	points := test.GenPoints(5)
	for _, p := range points {
		writeAPI.WritePoint(p)
	}
	start := time.Now()
	writeAPI.waitForFlushing()
	end := time.Now()
	fmt.Printf("Flash duration: %dns\n", end.Sub(start).Nanoseconds())
	assert.Len(t, service.Lines(), 5)
	assert.True(t, service.WasGzip())

	service.Close()
	writeAPI.writeOptions.SetUseGZip(false)
	for _, p := range points {
		writeAPI.WritePoint(p)
	}
	writeAPI.waitForFlushing()
	assert.Len(t, service.Lines(), 5)
	assert.False(t, service.WasGzip())

	writeAPI.Close()
}
func TestFlushInterval(t *testing.T) {
	service := test.NewTestService(t, "http://localhost:8888")
	writeAPI := NewWriteAPI("my-org", "my-bucket", service, write.DefaultOptions().SetBatchSize(10).SetFlushInterval(30))
	points := test.GenPoints(5)
	for _, p := range points {
		writeAPI.WritePoint(p)
	}
	require.Len(t, service.Lines(), 0)
	<-time.After(time.Millisecond * 50)
	require.Len(t, service.Lines(), 5)
	writeAPI.Close()

	service.Close()
}

func TestBufferOverwrite(t *testing.T) {
	log.Log.SetLogLevel(log.DebugLevel)
	ilog.SetFlags(ilog.Ldate | ilog.Lmicroseconds)
	hs := test.NewTestService(t, "http://localhost:8086")
	// sleep takes at least more than 10ms (sometimes 15ms) on Windows https://github.com/golang/go/issues/44343
	baseRetryInterval := uint(1)
	if runtime.GOOS == "windows" {
		baseRetryInterval = 20
	}
	// Buffer limit 15000, bach is 5000 => buffer for 3 batches
	opts := write.DefaultOptions().SetRetryInterval(baseRetryInterval).SetRetryBufferLimit(15000)

	srv := NewWriteAPI("my-org", "my-bucket", hs, opts)
	// Set permanent reply error to force writes fail and retry
	hs.SetReplyError(&http.Error{
		StatusCode: 429,
	})
	// This batch will fail and it will be added to retry queue
	b1 := iwrite.NewBatch("1\n", opts.MaxRetryTime())
	err := srv.sendBatch(b1)
	assert.NotNil(t, err)
	//assert.Equal(t, uint(baseRetryInterval), srv.retryDelay)
	assertBetween(t, srv.retryDelay, baseRetryInterval, baseRetryInterval*2)
	assert.Equal(t, 1, srv.retryQueue.Len())

	<-time.After(time.Millisecond * time.Duration(srv.retryDelay))
	b2 := iwrite.NewBatch("2\n", opts.MaxRetryTime())
	// First batch will be tried to write again and this one will added to retry queue
	err = srv.sendBatch(b2)
	assert.NotNil(t, err)
	assertBetween(t, srv.retryDelay, baseRetryInterval*2, baseRetryInterval*4)
	assert.Equal(t, 2, srv.retryQueue.Len())

	<-time.After(time.Millisecond * time.Duration(srv.retryDelay))
	b3 := iwrite.NewBatch("3\n", opts.MaxRetryTime())
	// First batch will be tried to write again and this one will added to retry queue
	err = srv.sendBatch(b3)
	assert.NotNil(t, err)
	assertBetween(t, srv.retryDelay, baseRetryInterval*4, baseRetryInterval*8)
	assert.Equal(t, 3, srv.retryQueue.Len())

	// Write early and overwrite
	b4 := iwrite.NewBatch("4\n", opts.MaxRetryTime())
	// No write will occur, because retry delay has not passed yet
	// However new bach will be added to retry queue. Retry queue has limit 3,
	// so first batch will be discarded
	priorRetryDelay := srv.retryDelay
	err = srv.sendBatch(b4)
	assert.NoError(t, err)
	assert.Equal(t, priorRetryDelay, srv.retryDelay) // Accumulated retry delay should be retained despite batch discard
	assert.Equal(t, 3, srv.retryQueue.Len())

	// Overwrite
	<-time.After(time.Millisecond * time.Duration(srv.retryDelay) / 2)
	b5 := iwrite.NewBatch("5\n", opts.MaxRetryTime())
	// Second batch will be tried to write again
	// However, write will fail and as new batch is added to retry queue
	// the second batch will be discarded
	err = srv.sendBatch(b5)
	assert.Nil(t, err) // No error should be returned, because no write was attempted (still waiting for retryDelay to expire)
	assert.Equal(t, 3, srv.retryQueue.Len())

	<-time.After(time.Millisecond * time.Duration(srv.retryDelay))
	// Clear error and let write pass
	hs.SetReplyError(nil)
	// Batches from retry queue will be sent first
	err = srv.sendBatch(iwrite.NewBatch("6\n", opts.MaxRetryTime()))
	assert.Nil(t, err)
	assert.Equal(t, 0, srv.retryQueue.Len())
	require.Len(t, hs.Lines(), 4)
	assert.Equal(t, "3", hs.Lines()[0])
	assert.Equal(t, "4", hs.Lines()[1])
	assert.Equal(t, "5", hs.Lines()[2])
	assert.Equal(t, "6", hs.Lines()[3])
}

// TODO: cannot reliably test new batches and scheduled retries
// leaving for now
func TestRetryStrategy(t *testing.T) {
	log.Log.SetLogLevel(log.DebugLevel)
	hs := test.NewTestService(t, "http://localhost:8086")
	opts := write.DefaultOptions().SetRetryInterval(1)
	writeAPI := NewWriteAPI("my-org", "my-bucket", hs, opts)

	// Set permanent reply error to force writes fail and retry
	hs.SetReplyError(&http.Error{
		StatusCode: 429,
	})
	// This batch will fail and it be added to retry queue
	b1 := iwrite.NewBatch("1\n", opts.MaxRetryTime())
	err := writeAPI.sendBatch(b1)
	assert.NotNil(t, err)
	assert.EqualValues(t, 1, writeAPI.retryDelay)
	assert.Equal(t, 1, writeAPI.retryQueue.Len())

	//wait retry delay + little more
	<-time.After(time.Millisecond*time.Duration(writeAPI.retryDelay) + time.Microsecond*5)
	// First batch will be tried to write again and this one will added to retry queue
	b2 := iwrite.NewBatch("2\n", opts.MaxRetryTime())
	err = writeAPI.sendBatch(b2)
	assert.NotNil(t, err)
	assertBetween(t, writeAPI.retryDelay, 2, 4)
	assert.Equal(t, 2, writeAPI.retryQueue.Len())

	//wait retry delay + little more
	<-time.After(time.Millisecond*time.Duration(writeAPI.retryDelay) + time.Microsecond*5)
	// First batch will be tried to write again and this one will added to retry queue
	b3 := iwrite.NewBatch("3\n", opts.MaxRetryTime())
	err = writeAPI.sendBatch(b3)
	assert.NotNil(t, err)
	assertBetween(t, writeAPI.retryDelay, 4, 8)
	assert.Equal(t, 3, writeAPI.retryQueue.Len())

	//wait retry delay + little more
	<-time.After(time.Millisecond*time.Duration(writeAPI.retryDelay) + time.Microsecond*5)
	// First batch will be tried to write again and this one will added to retry queue
	b4 := iwrite.NewBatch("4\n", opts.MaxRetryTime())
	err = writeAPI.sendBatch(b4)
	assert.NotNil(t, err)
	assertBetween(t, writeAPI.retryDelay, 8, 16)
	assert.Equal(t, 4, writeAPI.retryQueue.Len())

	<-time.After(time.Millisecond*time.Duration(writeAPI.retryDelay) + time.Microsecond*5)
	// Clear error and let write pass
	hs.SetReplyError(nil)
	// Batches from retry queue will be sent first
	err = writeAPI.sendBatch(iwrite.NewBatch("5\n", opts.MaxRetryTime()))
	assert.Nil(t, err)
	assert.Equal(t, 0, writeAPI.retryQueue.Len())
	require.Len(t, hs.Lines(), 5)
	assert.Equal(t, "1", hs.Lines()[0])
	assert.Equal(t, "2", hs.Lines()[1])
	assert.Equal(t, "3", hs.Lines()[2])
	assert.Equal(t, "4", hs.Lines()[3])
	assert.Equal(t, "5", hs.Lines()[4])
}

func TestRetry(t *testing.T) {
	service := test.NewTestService(t, "http://localhost:8888")
	log.Log.SetLogLevel(log.DebugLevel)
	writeAPI := NewWriteAPI("my-org", "my-bucket", service, write.DefaultOptions().SetBatchSize(5).SetRetryInterval(10000))
	points := test.GenPoints(15)
	for i := 0; i < 5; i++ {
		writeAPI.WritePoint(points[i])
	}
	writeAPI.waitForFlushing()
	require.Len(t, service.Lines(), 5)
	service.Close()
	service.SetReplyError(&http.Error{
		StatusCode: 429,
		RetryAfter: 1,
	})
	for i := 0; i < 5; i++ {
		writeAPI.WritePoint(points[i])
	}
	writeAPI.waitForFlushing()
	require.Len(t, service.Lines(), 0)
	service.Close()
	for i := 5; i < 10; i++ {
		writeAPI.WritePoint(points[i])
	}
	writeAPI.waitForFlushing()
	require.Len(t, service.Lines(), 0)
	<-time.After(time.Second + 50*time.Millisecond)
	for i := 10; i < 15; i++ {
		writeAPI.WritePoint(points[i])
	}
	writeAPI.waitForFlushing()
	require.Len(t, service.Lines(), 15)
	assert.True(t, strings.HasPrefix(service.Lines()[7], "test,hostname=host_7"))
	assert.True(t, strings.HasPrefix(service.Lines()[14], "test,hostname=host_14"))
	writeAPI.Close()
}

func TestWriteError(t *testing.T) {
	service := test.NewTestService(t, "http://localhost:8888")
	log.Log.SetLogLevel(log.DebugLevel)
	service.SetReplyError(&http.Error{
		StatusCode: 400,
		Code:       "write",
		Message:    "error",
	})
	writeAPI := NewWriteAPI("my-org", "my-bucket", service, write.DefaultOptions().SetBatchSize(5))
	errCh := writeAPI.Errors()
	var recErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		recErr = <-errCh
		wg.Done()
	}()
	points := test.GenPoints(15)
	for i := 0; i < 5; i++ {
		writeAPI.WritePoint(points[i])
	}
	writeAPI.waitForFlushing()
	wg.Wait()
	require.NotNil(t, recErr)
	writeAPI.Close()
}

func TestWriteErrorCallback(t *testing.T) {
	service := test.NewTestService(t, "http://localhost:8888")
	log.Log.SetLogLevel(log.DebugLevel)
	service.SetReplyError(&http.Error{
		StatusCode: 429,
		Code:       "write",
		Message:    "error",
	})
	// sleep takes at least more than 10ms (sometimes 15ms) on Windows https://github.com/golang/go/issues/44343
	retryInterval := uint(1)
	if runtime.GOOS == "windows" {
		retryInterval = 20
	}
	writeAPI := NewWriteAPI("my-org", "my-bucket", service, write.DefaultOptions().SetBatchSize(1).SetRetryInterval(retryInterval))
	writeAPI.SetWriteFailedCallback(func(batch string, error http.Error, retryAttempts uint) bool {
		return retryAttempts < 2
	})
	points := test.GenPoints(10)
	// first batch will be discarded by callback after 3 write attempts, second batch should survive with only one failed attempt
	for i, j := 0, 0; i < 6; i++ {
		writeAPI.WritePoint(points[i])
		writeAPI.waitForFlushing()
		w := int(math.Pow(5, float64(j)) * float64(retryInterval))
		fmt.Printf("Waiting %dms\n", w)
		<-time.After(time.Duration(w) * time.Millisecond)
		j++
		if j == 3 {
			j = 0
		}
	}
	service.SetReplyError(nil)
	writeAPI.SetWriteFailedCallback(func(batch string, error http.Error, retryAttempts uint) bool {
		return true
	})
	for i := 6; i < 10; i++ {
		writeAPI.WritePoint(points[i])
	}
	writeAPI.waitForFlushing()
	assert.Len(t, service.Lines(), 9)

	writeAPI.Close()
}

func TestClosing(t *testing.T) {
	service := test.NewTestService(t, "http://localhost:8888")
	log.Log.SetLogLevel(log.DebugLevel)
	writeAPI := NewWriteAPI("my-org", "my-bucket", service, write.DefaultOptions().SetBatchSize(5).SetRetryInterval(10000))
	points := test.GenPoints(15)
	for i := 0; i < 5; i++ {
		writeAPI.WritePoint(points[i])
	}
	writeAPI.Close()
	require.Len(t, service.Lines(), 5)

	writeAPI = NewWriteAPI("my-org", "my-bucket", service, write.DefaultOptions().SetBatchSize(5).SetRetryInterval(10000))
	service.Close()
	service.SetReplyError(&http.Error{
		StatusCode: 425,
	})
	_ = writeAPI.Errors()
	for i := 0; i < 15; i++ {
		writeAPI.WritePoint(points[i])
	}
	start := time.Now()
	writeAPI.Close()
	diff := time.Since(start)
	fmt.Println("Diff", diff)
	assert.Len(t, service.Lines(), 0)

}

func TestPow(t *testing.T) {
	assert.EqualValues(t, 1, pow(10, 0))
	assert.EqualValues(t, 10, pow(10, 1))
	assert.EqualValues(t, 4, pow(2, 2))
	assert.EqualValues(t, 1, pow(1, 2))
	assert.EqualValues(t, 125, pow(5, 3))
}

func assertBetween(t *testing.T, val, min, max uint) {
	t.Helper()
	assert.True(t, val >= min && val <= max, fmt.Sprintf("%d is outside <%d;%d>", val, min, max))
}

func TestComputeRetryDelay(t *testing.T) {
	hs := test.NewTestService(t, "http://localhost:8888")
	opts := write.DefaultOptions()
	writeAPI := NewWriteAPI("my-org", "my-bucket", hs, opts)
	assertBetween(t, writeAPI.computeRetryDelay(0), 5_000, 10_000)
	assertBetween(t, writeAPI.computeRetryDelay(1), 10_000, 20_000)
	assertBetween(t, writeAPI.computeRetryDelay(2), 20_000, 40_000)
	assertBetween(t, writeAPI.computeRetryDelay(3), 40_000, 80_000)
	assertBetween(t, writeAPI.computeRetryDelay(4), 80_000, 125_000)
	assert.EqualValues(t, 125_000, writeAPI.computeRetryDelay(5))
}
