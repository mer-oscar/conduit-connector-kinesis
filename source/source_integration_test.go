package source

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

var cfg map[string]string = map[string]string{
	"streamARN":           "arn:aws:kinesis:us-east-1:000000000000:stream/stream1",
	"aws.region":          "us-east-1",
	"aws.accessKeyId":     "accesskeymock",
	"aws.secretAccessKey": "accesssecretmock",
}

func LocalKinesisClient(ctx context.Context, srcConfig Config, is *is.I) *kinesis.Client {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(srcConfig.AWSRegion),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				srcConfig.AWSAccessKeyID,
				srcConfig.AWSSecretAccessKey,
				"")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               "http://localhost:4566",
				SigningRegion:     srcConfig.AWSRegion,
				HostnameImmutable: true,
			}, nil
		})),
	)
	is.NoErr(err)

	return kinesis.NewFromConfig(cfg)
}

func TestTeardown_Open(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	con := Source{}

	err := con.Configure(ctx, cfg)
	is.NoErr(err)

	con.client = LocalKinesisClient(ctx, con.config, is)
	con.config.StreamARN = setupSourceTest(ctx, con.client, is)

	err = con.Open(ctx, nil)
	is.NoErr(err)

	// cleanupTest deletes the stream
	cleanupTest(ctx, con.client, con.config.StreamARN)
	con.consumerARN = ""

	err = con.Teardown(ctx)
	is.NoErr(err)
}

func TestRead(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	con := Source{
		buffer: make(chan sdk.Record, 1),
	}

	err := con.Configure(ctx, cfg)
	is.NoErr(err)

	con.client = LocalKinesisClient(ctx, con.config, is)
	con.config.StreamARN = setupSourceTest(ctx, con.client, is)

	defer func() {
		cleanupTest(ctx, con.client, con.config.StreamARN)
		con.consumerARN = ""

		err := con.Teardown(ctx)
		is.NoErr(err)
	}()

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

	length := len(recs)
	fmt.Println(length, "read records using getRecords")

	err = con.Open(ctx, nil)
	is.NoErr(err)

	for i := 0; i < 5; i++ {
		_, err := con.Read(ctx)
		is.NoErr(err)
	}

	_, err = con.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)

	seqNumber := make(chan string, 1)

	// send a new message
	go func() {
		putRecResp, err := con.client.PutRecord(ctx, &kinesis.PutRecordInput{
			StreamARN:    &con.config.StreamARN,
			Data:         []byte("some data here"),
			PartitionKey: aws.String("5"),
		})
		is.NoErr(err)

		seqNumber <- *putRecResp.SequenceNumber
	}()

	// receive value so we know theres one in the buffer before calling read
	sequenceNumber := <-seqNumber

	rec, err := con.Read(ctx)
	is.NoErr(err)

	is.Equal(sequenceNumber, rec.Metadata["sequenceNumber"])

	// send value and then block read until it comes in
	go func() {
		_, err := con.client.PutRecord(ctx, &kinesis.PutRecordInput{
			StreamARN:    &con.config.StreamARN,
			Data:         []byte("some data here again"),
			PartitionKey: aws.String("6"),
		})
		is.NoErr(err)
	}()

	// expect message to be read from subscription before timeout
	_, err = con.Read(ctx)
	is.NoErr(err)

	// full 5 second timeout
	_, err = con.Read(ctx)
	is.Equal(err, sdk.ErrBackoffRetry)
}

func setupSourceTest(ctx context.Context, client *kinesis.Client, is *is.I) string {
	streamName := "stream-source"
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
		return setupSourceTest(ctx, client, is)
	}
	is.NoErr(err)

	var recs []types.PutRecordsRequestEntry
	for i := 0; i < 5; i++ {
		kRec := types.PutRecordsRequestEntry{
			PartitionKey: aws.String(strconv.Itoa(i)),
			Data:         []byte(fmt.Sprintf("%d - some data here", i)),
		}

		recs = append(recs, kRec)
	}

	// push some messages to it
	_, err = client.PutRecords(ctx, &kinesis.PutRecordsInput{
		StreamName: &streamName,
		Records:    recs,
	})
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
