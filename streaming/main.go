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
	streamCli := stream.NewClient(&stream.Params{Spanner: spannerCli})
	app := &app{
		spanner: spannerCli,
		stream:  streamCli,
	}
	return app.run(ctx, streamName)
}

type app struct {
	spanner *spanner.Client
	stream  stream.Client
}

func (a *app) run(ctx context.Context, streamName string) error {
	receiveCh := make(chan *stream.DataChangeRecord, 10000)
	params := &stream.ListChangeRecordsParams{
		StreamName: streamName,
		StartAt:    time.Date(2023, 1, 17, 16, 35, 0, 0, time.UTC),
		EndAt:      time.Date(2023, 1, 20, 23-9, 15, 0, 0, time.UTC),
		ReceiveCh:  receiveCh,
	}
	eg, ectx := errgroup.WithContext(ctx)
	eg.Go(func() error {
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
				fmt.Printf("\nReceived: table=%s, modType=%s\n", record.TableName, record.ModType)
				for i, column := range record.ColumnTypes {
					fmt.Printf("- ColumnTypes[%d] > %+v\n", i, column)
				}
				for i, mod := range record.Mods {
					fmt.Printf("- Mods[%d] > %+v\n", i, mod)
				}
				err := a.listRecords(ctx, record, adminFields)
				if err != nil {
					println("listRecords error", err.Error())
				}
			}
		}
	})
	eg.Go(func() error {
		defer close(receiveCh)
		return a.stream.ListChangeRecords(ctx, params)
	})
	return eg.Wait()
}

func (a *app) listRecords(ctx context.Context, record *stream.DataChangeRecord, fields []string) error {
	var total int
	keys := record.PrimaryKeySets()
	iter := a.spanner.Single().Read(ctx, record.TableName, keys, fields)
	err := iter.Do(func(row *spanner.Row) error {
		admin := &Admin{}
		defer func() {
			fmt.Printf("- Data[%d] > %+v\n", total, admin)
			total++
		}()
		return row.ToStruct(admin)
	})
	return err
}

// example source

var adminFields = []string{"AdminId", "Email", "Role", "Disabled", "CreatedAt", "UpdatedAt"}

type Admin struct {
	Id        string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty" spanner:"AdminId"`
	Email     string `protobuf:"bytes,2,opt,name=email,proto3" json:"email,omitempty"`
	Role      int64  `protobuf:"varint,3,opt,name=role,proto3" json:"role,omitempty"`
	Disabled  bool   `protobuf:"varint,4,opt,name=disabled,proto3" json:"disabled,omitempty"`
	CreatedAt int64  `protobuf:"varint,5,opt,name=created_at,json=createdAt,proto3" json:"created_at,omitempty"`
	UpdatedAt int64  `protobuf:"varint,6,opt,name=updated_at,json=updatedAt,proto3" json:"updated_at,omitempty"`
}
