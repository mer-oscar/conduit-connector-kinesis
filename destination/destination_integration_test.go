package destination

import (
	"context"
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

var cfg map[string]string = map[string]string{
	"useSingleShard":      "false",
	"streamARN":           "arn:aws:kinesis:us-east-1:000000000000:stream/stream1",
	"aws.region":          "us-east-1",
	"aws.accessKeyId":     "accesskeymock",
	"aws.secretAccessKey": "accesssecretmock",
	"aws.url":             "http://localhost:4566",
}

func setupDestinationTest(ctx context.Context, client *kinesis.Client, is *is.I) string {
	streamName := "stream-destination"
	// create stream
	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: &streamName,
	})
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			fmt.Println("stream already exists")
		}
	} else {
		is.NoErr(err)
	}

	time.Sleep(time.Second * 5)
	describe, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: &streamName,
	})
	if err != nil {
		// try to set up the stream again
		return setupDestinationTest(ctx, client, is)
	}
	is.NoErr(err)

	return *describe.StreamDescription.StreamARN
}

func cleanupTest(ctx context.Context, client *kinesis.Client, streamARN string) {
	_, err := client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		EnforceConsumerDeletion: aws.Bool(true),
		StreamARN:               &streamARN,
	})

	if err != nil {
		fmt.Println("failed to delete stream")
	}
}

func TestTeardown_Open(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	con := Destination{}

	err := con.Configure(ctx, cfg)
	is.NoErr(err)

	con.config.StreamARN = setupDestinationTest(ctx, con.client, is)
	defer cleanupTest(ctx, con.client, con.config.StreamARN)

	err = con.Open(ctx)
	is.NoErr(err)

	err = con.Teardown(ctx)
	is.NoErr(err)
}

func TestWrite_PutRecords(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	con := Destination{}

	err := con.Configure(ctx, cfg)
	is.NoErr(err)

	con.config.StreamARN = setupDestinationTest(ctx, con.client, is)

	cases := []struct {
		testName      string
		expectedError error
		records       []sdk.Record
	}{
		{
			"happy path",
			nil,
			makeRecords(500, false),
		},
	}

	// setup table test
	for _, tt := range cases {
		recs := tt.records
		t.Run(tt.testName, func(t *testing.T) {
			var err error
			if tt.expectedError != nil {
				// handle err
				is.Equal(err, tt.expectedError)
			}

			count, err := con.Write(ctx, recs)
			if err != nil {
				// handle err
				is.Equal(err, tt.expectedError)
			}

			is.NoErr(err)
			is.Equal(count, len(tt.records))

			listShards, err := con.client.ListShards(ctx, &kinesis.ListShardsInput{
				StreamARN: &con.config.StreamARN,
			})
			is.NoErr(err)

			var recs []types.Record
			for _, shard := range listShards.Shards {
				si, err := con.client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
					ShardId:           shard.ShardId,
					ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
					StreamARN:         &con.config.StreamARN,
				})
				is.NoErr(err)

				getRecs, err := con.client.GetRecords(ctx, &kinesis.GetRecordsInput{
					StreamARN:     &con.config.StreamARN,
					ShardIterator: si.ShardIterator,
				})
				is.NoErr(err)

				recs = append(recs, getRecs.Records...)
			}

			is.Equal(count, len(recs))
		})
	}

	cleanupTest(ctx, con.client, con.config.StreamARN)

	err = con.Teardown(ctx)
	is.NoErr(err)
}

func TestWrite_PutRecord(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	con := Destination{}

	cfg["use_single_shard"] = "true"

	err := con.Configure(ctx, cfg)
	is.NoErr(err)

	con.config.StreamARN = setupDestinationTest(ctx, con.client, is)

	cases := []struct {
		testName                 string
		expectedError            error
		expectedNumberOfRequests int
		records                  []sdk.Record
	}{
		{
			"happy path - <500 records",
			nil,
			499,
			makeRecords(499, false),
		},
	}

	// setup table test
	for _, tt := range cases {
		t.Run(tt.testName, func(t *testing.T) {
			count, err := con.Write(ctx, tt.records)
			if err != nil {
				// handle err
				is.Equal(err, tt.expectedError)
			}

			is.NoErr(err)
			is.Equal(count, len(tt.records))

			listShards, err := con.client.ListShards(ctx, &kinesis.ListShardsInput{
				StreamARN: &con.config.StreamARN,
			})
			is.NoErr(err)

			var recs []types.Record
			for _, shard := range listShards.Shards {
				si, err := con.client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
					ShardId:           shard.ShardId,
					ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
					StreamARN:         &con.config.StreamARN,
				})
				is.NoErr(err)

				getRecs, err := con.client.GetRecords(ctx, &kinesis.GetRecordsInput{
					StreamARN:     &con.config.StreamARN,
					ShardIterator: si.ShardIterator,
				})
				is.NoErr(err)

				recs = append(recs, getRecs.Records...)
			}

			is.Equal(count, len(recs))
		})
	}

	cleanupTest(ctx, con.client, con.config.StreamARN)

	err = con.Teardown(ctx)
	is.NoErr(err)
}

func makeRecords(count int, greaterThan5MB bool) []sdk.Record {
	var records []sdk.Record
	oneMB := (1024 * 1024) - 300000

	for i := 0; i < count; i++ {
		data := make([]byte, 16)
		_, _ = rand.Read(data)

		if greaterThan5MB {
			data = make([]byte, oneMB)
			_, _ = rand.Read(data)
		}
		key := strconv.Itoa(i)
		rec := sdk.Util.Source.NewRecordCreate(
			nil,
			nil,
			sdk.RawData(key),
			sdk.RawData(data),
		)

		records = append(records, rec)
	}
	return records
}
