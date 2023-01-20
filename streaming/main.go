package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/suzujun/gospanner/streaming/stream"
	"golang.org/x/sync/errgroup"
)

func main() {
	ctx := context.Background()
	err := run(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("Done!")
}

func run(ctx context.Context) error {
	var projectID, instance, database, streamName string
	flag.StringVar(&projectID, "project_id", "", "gcp project id.")
	flag.StringVar(&instance, "instance", "", "instance of spanner.")
	flag.StringVar(&database, "database", "", "database of spanner.")
	flag.StringVar(&streamName, "stream", "", "stream of spanner.")
	flag.Parse()
	dbname := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instance, database)
	spannerCli, err := spanner.NewClient(ctx, dbname)
	if err != nil {
		return err
	}
	cli := stream.NewClient(&stream.Params{Spanner: spannerCli})
	receiveCh := make(chan *stream.DataChangeRecord, 10000)
	params := &stream.ChangeRecordStreamParams{
		StreamName:      streamName,
		StartAt:         time.Date(2023, 1, 17, 16, 35, 0, 0, time.UTC),
		EndAt:           nil,
		Token:           nil,
		HeartbeatMillis: 300000,
		ReceiveCh:       receiveCh,
	}
	eg, ectx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		// println("receive:start")
		// defer println("receive:end")
		for {
			select {
			case <-ectx.Done():
				fmt.Println("receive chan finished! with context done")
				return nil
			case record, ok := <-receiveCh:
				if !ok {
					fmt.Println("receive chan finished!")
					return nil
				}
				fmt.Printf("received > %+v\n", record)
			}
		}
	})
	eg.Go(func() error {
		// println("stream:start")
		// defer fmt.Println("change record stream finished!")
		return cli.ListChangeRecords(ctx, params)
	})
	println("main:end")
	return eg.Wait()
}
