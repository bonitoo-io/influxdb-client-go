// Copyright 2020-2021 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package api

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	http2 "github.com/influxdata/influxdb-client-go/v2/api/http"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/influxdata/influxdb-client-go/v2/internal/log"
	iwrite "github.com/influxdata/influxdb-client-go/v2/internal/write"
)

// WriteFailedCallback is synchronously notified in case non-blocking write fails.
// batch contains complete payload, error holds detailed error information,
// retryAttempts means number of retries, 0 if it failed during first write.
// It must return true if WriteAPI should continue with retrying, false will discard the batch.
type WriteFailedCallback func(batch string, error http2.Error, retryAttempts uint) bool

// WriteAPI is Write client interface with non-blocking methods for writing time series data asynchronously in batches into an InfluxDB server.
// WriteAPI can be used concurrently.
// When using multiple goroutines for writing, use a single WriteAPI instance in all goroutines.
type WriteAPI interface {
	// WriteRecord writes asynchronously line protocol record into bucket.
	// WriteRecord adds record into the buffer which is sent on the background when it reaches the batch size.
	// Blocking alternative is available in the WriteAPIBlocking interface
	WriteRecord(line string)
	// WritePoint writes asynchronously Point into bucket.
	// WritePoint adds Point into the buffer which is sent on the background when it reaches the batch size.
	// Blocking alternative is available in the WriteAPIBlocking interface
	WritePoint(point *write.Point)
	// Flush forces all pending writes from the buffer to be sent
	Flush()
	// Errors returns a channel for reading errors which occurs during async writes.
	// Must be called before performing any writes for errors to be collected.
	// The chan is unbuffered and must be drained or the writer will block.
	Errors() <-chan error
	// SetWriteFailedCallback sets callback allowing custom handling of failed writes.
	// If callback returns true, failed batch will be retried, otherwise discarded.
	SetWriteFailedCallback(cb WriteFailedCallback)
}

// WriteAPIImpl provides main implementation for WriteAPI
type WriteAPIImpl struct {
	service     *iwrite.Service
	writeBuffer []string
	retryTimer  *time.Timer

	flushTimer           *time.Timer
	errCh                chan error
	writeCh              chan *iwrite.Batch
	bufferCh             chan string
	writeStop            chan struct{}
	bufferStop           chan struct{}
	bufferFlush          chan struct{}
	doneCh               chan struct{}
	bufferInfoCh         chan writeBuffInfoReq
	writeInfoCh          chan writeBuffInfoReq
	writeOptions         *write.Options
	closingMu            *sync.Mutex
	isErrChReader        int32
	retryQueue           *iwrite.Queue
	retryDelay           uint
	retryAttempts        uint
	retryExponentialBase int
	writeFailedCB        WriteFailedCallback
}

type writeBuffInfoReq struct {
	writeBuffLen int
}

// NewWriteAPI returns new non-blocking write client for writing data to  bucket belonging to org
func NewWriteAPI(org string, bucket string, service http2.Service, writeOptions *write.Options) *WriteAPIImpl {
	retryBufferLimit := writeOptions.RetryBufferLimit() / writeOptions.BatchSize()
	w := &WriteAPIImpl{
		service:              iwrite.NewService(org, bucket, service, writeOptions),
		errCh:                make(chan error, 1),
		writeBuffer:          make([]string, 0, writeOptions.BatchSize()+1),
		writeCh:              make(chan *iwrite.Batch),
		bufferCh:             make(chan string),
		bufferStop:           make(chan struct{}),
		writeStop:            make(chan struct{}),
		bufferFlush:          make(chan struct{}),
		doneCh:               make(chan struct{}),
		bufferInfoCh:         make(chan writeBuffInfoReq),
		writeInfoCh:          make(chan writeBuffInfoReq),
		writeOptions:         writeOptions,
		closingMu:            &sync.Mutex{},
		retryQueue:           iwrite.NewQueue(int(retryBufferLimit)),
		retryExponentialBase: 2,
	}

	go w.bufferProc()
	go w.writeProc()

	return w
}

// SetWriteFailedCallback sets callback allowing custom handling of failed writes.
// If callback returns true, failed batch will be retried, otherwise discarded.
func (w *WriteAPIImpl) SetWriteFailedCallback(cb WriteFailedCallback) {
	w.writeFailedCB = cb
}

// Errors returns a channel for reading errors which occurs during async writes.
// Must be called before performing any writes for errors to be collected.
// New error is skipped when channel is not read.
func (w *WriteAPIImpl) Errors() <-chan error {
	w.setErrChanRead()
	return w.errCh
}

// Flush forces all pending writes from the buffer to be sent
func (w *WriteAPIImpl) Flush() {
	w.bufferFlush <- struct{}{}
	w.waitForFlushing()
}

func (w *WriteAPIImpl) scheduleRetry(b *iwrite.Batch) {
	log.Debug("Write proc: scheduling write")
	w.retryTimer = time.AfterFunc(time.Duration(w.retryDelay)*time.Millisecond, func() {
		log.Debug("Write proc: writing scheduled batch")
		w.writeCh <- b
	})
}

func (w *WriteAPIImpl) waitForFlushing() {
	for {
		w.bufferInfoCh <- writeBuffInfoReq{}
		writeBuffInfo := <-w.bufferInfoCh
		if writeBuffInfo.writeBuffLen == 0 {
			break
		}
		log.Info("Waiting buffer is flushed")
		<-time.After(time.Millisecond)
	}
	for {
		w.writeInfoCh <- writeBuffInfoReq{}
		writeBuffInfo := <-w.writeInfoCh
		if writeBuffInfo.writeBuffLen == 0 {
			break
		}
		log.Info("Waiting buffer is flushed")
		<-time.After(time.Millisecond)
	}
}

func (w *WriteAPIImpl) bufferProc() {
	log.Info("Buffer proc started")
	w.flushTimer = time.NewTimer(time.Duration(w.writeOptions.FlushInterval()) * time.Millisecond)
x:
	for {
		select {
		case line := <-w.bufferCh:
			w.writeBuffer = append(w.writeBuffer, line)
			if len(w.writeBuffer) == int(w.writeOptions.BatchSize()) {
				w.flushBuffer()
			}
		case <-w.flushTimer.C:
			w.flushBuffer()
		case <-w.bufferFlush:
			w.flushBuffer()
		case <-w.bufferStop:
			w.flushBuffer()
			break x
		case buffInfo := <-w.bufferInfoCh:
			buffInfo.writeBuffLen = len(w.bufferInfoCh)
			w.bufferInfoCh <- buffInfo
		}
	}
	log.Info("Buffer proc finished")
	w.doneCh <- struct{}{}
}

func (w *WriteAPIImpl) flushBuffer() {
	if len(w.writeBuffer) > 0 {
		log.Info("sending batch")
		batch := iwrite.NewBatch(buffer(w.writeBuffer), w.writeOptions.MaxRetryTime())
		w.writeCh <- batch
		w.writeBuffer = w.writeBuffer[:0]
		w.resetFlushTimer()
	}
}
func (w *WriteAPIImpl) resetFlushTimer() {
	w.flushTimer.Stop()
	w.flushTimer.Reset(time.Duration(w.writeOptions.FlushInterval()) * time.Millisecond)
}

func (w *WriteAPIImpl) isErrChanRead() bool {
	return atomic.LoadInt32(&w.isErrChReader) > 0
}

func (w *WriteAPIImpl) setErrChanRead() {
	atomic.StoreInt32(&w.isErrChReader, 1)
}

func (w *WriteAPIImpl) writeProc() {
	log.Info("Write proc started")
x:
	for {
		select {
		case batch := <-w.writeCh:
			err := w.sendBatch(batch)
			if err != nil && w.isErrChanRead() {
				select {
				case w.errCh <- err:
				default:
					log.Warn("Cannot write error to error channel, it is not read")
				}
			}

		case <-w.writeStop:
			log.Info("Write proc: received stop")
			break x
		case buffInfo := <-w.writeInfoCh:
			buffInfo.writeBuffLen = len(w.writeCh)
			w.writeInfoCh <- buffInfo
		}
	}
	log.Info("Write proc finished")
	w.doneCh <- struct{}{}
}

// sendBatch handles writes of batches and handles retrying.
// It first checks retry queue, cause it has highest priority.
// If there are some batches in retry queue, those are written and incoming batch is added to end of retry queue.
// Immediate write is allowed only in case there was success or not retryable error.
// Otherwise delay is checked based on recent batch.
// If write of batch fails with retryable error (connection errors and HTTP code >= 429),
// Batch retry time is calculated based on #of attempts.
// If writes continues failing and # of attempts reaches maximum or total retry time reaches maxRetryTime,
// batch is discarded.
func (w *WriteAPIImpl) sendBatch(b *iwrite.Batch) error {
	//return w.service.HandleWrite( b)
	log.Debug("Write proc: received write request")
	batchToWrite := b
	retrying := w.retryAttempts > 0
	// Check discarded batches
	if !w.retryQueue.IsEmpty() {
		for {
			rb := w.retryQueue.First()
			// Discard batches at beginning of retryQueue that have already expired
			if time.Now().After(rb.Expires) {
				log.Warn("Write proc: oldest batch in retry queue expired, discarding")
				w.retryQueue.RemoveIfFirst(rb)
				// if requested batch was discarded
				if rb == b {
					batchToWrite = nil
				}
				continue
			}
			break
		}
	}

	if retrying && b.RetryAttempts == 0 {
		// new batches must be added to que
		log.Warn("Write proc: cannot write before emptying retry queue, storing batch to queue")
		if w.retryQueue.Push(b) {
			log.Warn("Write proc: Retry buffer full, discarding oldest batch")
		}
		return errors.New("cannot write before emptying retry queue")
	}
	// Can we write? In case of retryable error we must wait a bit
	if w.retryDelay > 0 && time.Now().Before(w.service.LastWriteAttempt.Add(time.Millisecond*time.Duration(w.retryDelay))) {
		log.Warn("Write proc: cannot write yet, storing batch to queue")
		if b.RetryAttempts == 0 && w.retryQueue.Push(b) {
			log.Warn("Write proc: Retry buffer full, discarding oldest batch")
		}
		return fmt.Errorf("cannot write yet, %dms to wait", time.Now().Sub(w.service.LastWriteAttempt.Add(time.Millisecond*time.Duration(w.retryDelay))).Milliseconds())
	}
	if batchToWrite == nil && retrying && !w.retryQueue.IsEmpty() {
		log.Debug("Write proc: taking batch from retry queue")
		batchToWrite = w.retryQueue.First()
	}
	// write batch
	if batchToWrite != nil {
		perror := w.service.WriteBatch(context.Background(), batchToWrite)
		if perror != nil {
			if w.writeOptions.MaxRetries() != 0 && (perror.StatusCode == 0 || perror.StatusCode >= http.StatusTooManyRequests) {
				log.Errorf("Write error: %s, batch kept for retrying\n", perror.Error())
				if perror.RetryAfter > 0 {
					w.retryDelay = perror.RetryAfter * 1000
				} else {
					w.retryDelay = w.computeRetryDelay(w.retryAttempts)
				}
				if w.writeFailedCB != nil && !w.writeFailedCB(batchToWrite.Batch, *perror, batchToWrite.RetryAttempts) {
					log.Warn("Callback rejected batch, discarding")
					w.retryQueue.RemoveIfFirst(batchToWrite)
					return perror
				}
				// store new batch (not taken from queue)
				if batchToWrite.RetryAttempts == 0 {
					if w.retryQueue.Push(b) {
						log.Warn("Retry buffer full, discarding oldest batch")
					}
					w.scheduleRetry(b)
				} else if batchToWrite.RetryAttempts == w.writeOptions.MaxRetries() {
					log.Warn("Reached maximum number of retries, discarding batch")
					w.retryQueue.RemoveIfFirst(batchToWrite)
				}
				batchToWrite.RetryAttempts++
				w.retryAttempts++
				log.Debugf("Write proc: next wait for write is %dms\n", w.retryDelay)
			} else {
				log.Errorf("Write error: %s\n", perror.Error())
			}
			return fmt.Errorf("write failed (attempts %d): %w", batchToWrite.RetryAttempts, perror)
		}

		w.retryDelay = 0
		w.retryAttempts = 0
		if retrying {
			w.retryQueue.RemoveIfFirst(batchToWrite)
			if !w.retryQueue.IsEmpty() {
				w.retryDelay = 1
				w.scheduleRetry(w.retryQueue.First())
			}
		}
	}
	return nil
}

// Close finishes outstanding write operations,
// stop background routines and closes all channels
func (w *WriteAPIImpl) Close() {
	w.closingMu.Lock()
	defer w.closingMu.Unlock()
	if w.writeCh != nil {
		// Flush outstanding metrics
		w.Flush()

		// stop and wait for buffer proc
		close(w.bufferStop)
		<-w.doneCh

		close(w.bufferFlush)
		close(w.bufferCh)

		// stop and wait for write proc
		close(w.writeStop)
		<-w.doneCh

		close(w.writeCh)
		close(w.writeInfoCh)
		close(w.bufferInfoCh)
		w.writeCh = nil

		close(w.errCh)
		w.errCh = nil
	}
}

// WriteRecord writes asynchronously line protocol record into bucket.
// WriteRecord adds record into the buffer which is sent on the background when it reaches the batch size.
// Blocking alternative is available in the WriteAPIBlocking interface
func (w *WriteAPIImpl) WriteRecord(line string) {
	b := []byte(line)
	b = append(b, 0xa)
	w.bufferCh <- string(b)
}

// WritePoint writes asynchronously Point into bucket.
// WritePoint adds Point into the buffer which is sent on the background when it reaches the batch size.
// Blocking alternative is available in the WriteAPIBlocking interface
func (w *WriteAPIImpl) WritePoint(point *write.Point) {
	line, err := w.service.EncodePoints(point)
	if err != nil {
		log.Errorf("point encoding error: %s\n", err.Error())
		if w.errCh != nil {
			w.errCh <- err
		}
	} else {
		w.bufferCh <- line
	}
}

// computeRetryDelay calculates retry delay
// Retry delay is calculated as random value within the interval
// [retry_interval * exponential_base^(attempts) and retry_interval * exponential_base^(attempts+1)]
func (w *WriteAPIImpl) computeRetryDelay(attempts uint) uint {
	minDelay := int(w.writeOptions.RetryInterval() * pow(w.writeOptions.ExponentialBase(), attempts))
	maxDelay := int(w.writeOptions.RetryInterval() * pow(w.writeOptions.ExponentialBase(), attempts+1))
	retryDelay := uint(rand.Intn(maxDelay-minDelay) + minDelay)
	if retryDelay > w.writeOptions.MaxRetryInterval() {
		retryDelay = w.writeOptions.MaxRetryInterval()
	}
	return retryDelay
}

// pow computes x**y
func pow(x, y uint) uint {
	p := uint(1)
	if y == 0 {
		return 1
	}
	for i := uint(1); i <= y; i++ {
		p = p * x
	}
	return p
}

func buffer(lines []string) string {
	return strings.Join(lines, "")
}
