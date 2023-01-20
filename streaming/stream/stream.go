package stream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/googleapis/gax-go/v2/apierror"
	"golang.org/x/sync/semaphore"
)

type (
	Client interface {
		ListChangeRecords(ctx context.Context, params *ListChangeRecordsParams, opts ...Option) (err error)
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

var (
	ErrTimeoutOfReceiveCh = errors.New("stream: failed to timeout error of push change record")
	ErrTimeoutOfQueueCh   = errors.New("stream: failed to timeout error of push queue")
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

type ListChangeRecordsParams struct {
	StreamName string    // required
	StartAt    time.Time // required
	EndAt      time.Time // 未指定 及び 未来日時指定するとリアルタイム監視となり処理が終了しない
	Token      string
	ReceiveCh  chan<- *DataChangeRecord // required
}

func (c *client) ListChangeRecords(ctx context.Context, params *ListChangeRecordsParams, opts ...Option) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	dopts := &options{
		//nolint:gomnd
		maxMailbox:      10000,
		heartbeatMillis: 300000,
		queuingTimeout:  time.Second * 5,
		clientReceived:  func(stream, errmsg string) {},
		clientQueued:    func(stream, errmsg string) {},
		clientStarted:   func(stream string) {},
		clientCompleted: func(stream, errmsg string, duration int64) {},
	}
	for _, opt := range opts {
		opt(dopts)
	}
	queueCh := make(chan *fetchParams, dopts.maxMailbox)
	queueCh <- &fetchParams{
		startAt:         params.StartAt,
		endAt:           params.EndAt,
		token:           params.Token,
		heartbeatMillis: dopts.heartbeatMillis,
		level:           1,
	}
	manage := &manageParams{
		stream:    params.StreamName,
		receiveCh: params.ReceiveCh,
		queueCh:   queueCh,
		tokens:    make(map[string]struct{}),
		opts:      dopts,
	}
	var total int64
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if atomic.LoadInt64(&total) == 0 {
					close(queueCh)
				}
			}
		}
	}()
	const concurrency = 5
	sem := semaphore.NewWeighted(concurrency)
	for {
		select {
		case <-ctx.Done():
			return
		case queue, ok := <-queueCh:
			if !ok {
				return nil
			}
			atomic.AddInt64(&total, 1)
			if err := sem.Acquire(ctx, 1); err != nil {
				atomic.AddInt64(&total, -1)
				return err
			}
			go func() {
				defer func() {
					sem.Release(1)
					atomic.AddInt64(&total, -1)
				}()
				err = c.fetch(ctx, manage, queue)
				if err != nil {
					cancel()
				}
			}()
		}
	}
}

type manageParams struct {
	stream     string
	queueCh    chan<- *fetchParams
	receiveCh  chan<- *DataChangeRecord
	tokens     map[string]struct{}
	tokenMutex sync.RWMutex
	opts       *options
}

func (p *manageParams) PushResult(ctx context.Context, record *DataChangeRecord) error {
	var code string
	defer func() {
		p.opts.clientReceived(p.stream, code)
	}()
	timer := time.NewTimer(p.opts.queuingTimeout)
	select {
	case p.receiveCh <- record:
		code = "OK"
		return nil
	case <-ctx.Done():
		code = "ContextDone"
		return ctx.Err()
	case <-timer.C:
		code = "Timeout"
		return ErrTimeoutOfReceiveCh
	}
}

func (p *manageParams) PushQueue(ctx context.Context, queue *fetchParams) error {
	var code string
	defer func() {
		p.opts.clientQueued(p.stream, code)
	}()
	timer := time.NewTimer(p.opts.queuingTimeout)
	select {
	case p.queueCh <- queue:
		code = "OK"
		return nil
	case <-ctx.Done():
		code = "ContextDone"
		return ctx.Err()
	case <-timer.C:
		code = "Timeout"
		return ErrTimeoutOfQueueCh
	}
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

type fetchParams struct {
	startAt         time.Time
	endAt           time.Time
	token           string
	heartbeatMillis int64
	level           int
}

func (p *fetchParams) endAtPtr() *time.Time {
	if p.endAt.IsZero() {
		return nil
	}
	return &p.endAt
}

func (p *fetchParams) tokenPtr() *string {
	if len(p.token) == 0 {
		return nil
	}
	return &p.token
}

func (p *fetchParams) newStatement(stream string) spanner.Statement {
	const baseSQL = `SELECT ChangeRecord FROM READ_%s(
		start_timestamp => @startTimestamp,
		end_timestamp => @endTimestamp,
		partition_token => @partitionToken,
		heartbeat_milliseconds => @heartbeatMillis
	)`
	stmt := spanner.NewStatement(fmt.Sprintf(baseSQL, stream))
	stmt.Params = map[string]interface{}{
		// @see https://cloud.google.com/spanner/docs/change-streams/details?hl=ja#change_stream_query_syntax
		"startTimestamp":  p.startAt, // required
		"endTimestamp":    p.endAtPtr(),
		"partitionToken":  p.tokenPtr(),
		"heartbeatMillis": p.heartbeatMillis, // required
	}
	return stmt
}

func (c *client) fetch(ctx context.Context, manage *manageParams, queue *fetchParams) (err error) {
	start := time.Now()
	manage.opts.clientStarted(manage.stream)
	defer func() {
		var code string
		var aerr *apierror.APIError
		if errors.As(err, &aerr) {
			code = aerr.GRPCStatus().Code().String()
		}
		duration := time.Since(start).Milliseconds()
		manage.opts.clientCompleted(manage.stream, code, duration)
	}()

	stmt := queue.newStatement(manage.stream)
	iter := c.cli.Single().Query(ctx, stmt)
	err = iter.Do(func(row *spanner.Row) error {
		records := []*ChangeRecord{}
		if err := row.Columns(&records); err != nil {
			return err
		}
		for _, record := range records {
			for _, changeRecord := range record.DataChangeRecords {
				if err := manage.PushResult(ctx, changeRecord); err != nil {
					return err
				}
			}
			for _, patitionsRecord := range record.ChildPartitionsRecords {
				for _, partition := range patitionsRecord.ChildPartitions {
					if manage.contains(partition.Token) {
						continue
					}
					queue := *queue // copy
					queue.startAt = patitionsRecord.StartTimestamp
					queue.token = partition.Token
					queue.level++
					if err := manage.PushQueue(ctx, &queue); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
	return err
}
