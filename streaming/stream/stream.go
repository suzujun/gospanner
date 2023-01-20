package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"golang.org/x/sync/semaphore"
)

type (
	Client interface {
		ListChangeRecords(ctx context.Context, params *ChangeRecordStreamParams) error
	}
	client struct {
		cli       *spanner.Client
		queueCh   chan *fetchParams
		receiveCh chan *DataChangeRecord
		tokens    map[string]struct{}
	}
	Params struct {
		Spanner *spanner.Client
	}
)

func NewClient(params *Params) Client {
	const maxLen = 10000
	return &client{
		cli:       params.Spanner,
		queueCh:   make(chan *fetchParams, maxLen),
		receiveCh: make(chan *DataChangeRecord, maxLen),
		tokens:    make(map[string]struct{}),
	}
}

type manageParams struct {
	stream     string
	queueCh    chan<- *fetchParams
	receiveCh  chan<- *DataChangeRecord
	tokens     map[string]struct{}
	tokenMutex sync.RWMutex
}

func (p *manageParams) contains(token string) bool {
	p.tokenMutex.RLock()
	if _, ok := p.tokens[token]; ok {
		p.tokenMutex.RUnlock()
		return true
	}
	p.tokenMutex.RUnlock()
	p.tokenMutex.Lock()
	defer p.tokenMutex.Unlock()
	p.tokens[token] = struct{}{}
	return false
}

type ChangeRecordStreamParams struct {
	StreamName      string    // required
	StartAt         time.Time // required
	EndAt           *time.Time
	Token           *string
	HeartbeatMillis int64                    // required
	ReceiveCh       chan<- *DataChangeRecord // required
}

func (c *client) ListChangeRecords(ctx context.Context, params *ChangeRecordStreamParams) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	const maxLen = 10000
	var wg sync.WaitGroup
	queueCh := make(chan *fetchParams, maxLen)
	queueCh <- &fetchParams{
		startAt:         params.StartAt,
		endAt:           params.EndAt,
		token:           params.Token,
		heartbeatMillis: params.HeartbeatMillis,
		level:           1,
	}
	manage := &manageParams{
		stream:    params.StreamName,
		receiveCh: params.ReceiveCh,
		queueCh:   queueCh,
		tokens:    make(map[string]struct{}),
	}
	var total int64
	go func() {
		// debug 用途
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				fmt.Printf("ticker: total=%d, queue=%d, receive=%d\n", atomic.LoadInt64(&total), len(manage.queueCh), len(manage.receiveCh))
			}
		}
	}()
	const concurrency = 5
	sem := semaphore.NewWeighted(concurrency)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case queue, ok := <-queueCh:
			if !ok {
				wg.Wait()
				return nil
			}
			if err := sem.Acquire(ctx, 1); err != nil {
				return err
			}
			wg.Add(1)
			atomic.AddInt64(&total, 1)
			go func() {
				defer func() {
					atomic.AddInt64(&total, 1)
					wg.Done()
					sem.Release(1)
				}()
				err = c.fetch(ctx, manage, queue)
				if err != nil {
					close(queueCh)
				}
			}()
		}
	}
}

type fetchParams struct {
	startAt         time.Time
	endAt           *time.Time
	token           *string
	heartbeatMillis int64
	level           int
}

func (c *client) fetch(ctx context.Context, manage *manageParams, queue *fetchParams) error {
	const baseSQL = `SELECT ChangeRecord FROM READ_%s(
	start_timestamp => @startTimestamp,
	end_timestamp => @endTimestamp,
	partition_token => @partitionToken,
	heartbeat_milliseconds => @heartbeatMillis
)`
	status := "start"
	start := time.Now()
	go func() {
		// debug 用途
		timer := time.NewTicker(time.Second)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				if status == "done" {
					return
				}
				duration := time.Since(start).Truncate(time.Second).String()
				fmt.Printf("wait done of fetch: %s: %s: %+v\n", status, duration, queue)
			}
		}
	}()
	defer func() {
		status = "done"
	}()
	stmt := spanner.NewStatement(fmt.Sprintf(baseSQL, manage.stream))
	stmt.Params = map[string]interface{}{
		// @see https://cloud.google.com/spanner/docs/change-streams/details?hl=ja#change_stream_query_syntax
		"startTimestamp":  queue.startAt, // required
		"endTimestamp":    queue.endAt,
		"partitionToken":  queue.token,
		"heartbeatMillis": int64(300000), // required
	}
	iter := c.cli.Single().Query(ctx, stmt)
	status = "iter"
	var total int
	err := iter.Do(func(row *spanner.Row) error {
		total++
		defer func() {
			status = fmt.Sprintf("iter:do[%d]:fin2", total)
		}()
		status = fmt.Sprintf("iter:do[%d]", total)
		records := []*ChangeRecord{}
		if err := row.Columns(&records); err != nil {
			return err
		}
		status = fmt.Sprintf("iter:do[%d]:columns", total)
		for i, record := range records {
			status = fmt.Sprintf("iter:do[%d]:Records[%d]:DataChangeRecords", total, i)
			for j, changeRecord := range record.DataChangeRecords {
				status = fmt.Sprintf("iter:do[%d]:DataChangeRecords[%d][%d]", total, i, j)
				manage.receiveCh <- changeRecord
				timer := time.NewTimer(time.Second * 5)
				select {
				case manage.receiveCh <- changeRecord:
				case <-ctx.Done():
					return ctx.Err()
				case <-timer.C:
					return errors.New("stream: failed to timeout error of push change record")
				}
			}
			status = fmt.Sprintf("iter:do[%d]:Records[%d]:ChildPartitionsRecords", total, i)
			for j, patitionsRecord := range record.ChildPartitionsRecords {
				for k, partition := range patitionsRecord.ChildPartitions {
					status = fmt.Sprintf("iter:do[%d]:ChildPartitionsRecords[%d][%d][%d]", total, i, j, k)
					if manage.contains(partition.Token) {
						continue
					}
					queue := *queue // copy
					queue.startAt = patitionsRecord.StartTimestamp
					queue.token = &partition.Token
					queue.level++
					timer := time.NewTimer(time.Second * 5)
					select {
					case manage.queueCh <- &queue:
					case <-ctx.Done():
						return ctx.Err()
					case <-timer.C:
						return errors.New("stream: failed to timeout error of push queue")
					}
				}
			}
			status = fmt.Sprintf("iter:do[%d]:Records[%d]:fin", total, i)
		}
		status = fmt.Sprintf("iter:do[%d]:fin", total)
		return nil
	})
	status = fmt.Sprintf("last")
	return err
}
