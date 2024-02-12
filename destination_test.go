package kinesis_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"

	aws "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
	kinesis "github.com/mer-oscar/conduit-connector-kinesis"
	client "github.com/mer-oscar/conduit-connector-kinesis/test"
	"go.uber.org/mock/gomock"
)

var cfg map[string]string = map[string]string{
	"use_single_shard": "false",
	"stream_arn":       "aws:streamARNTest",
	"aws_region":       "us-east",
}

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := kinesis.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestTeardown_Open(t *testing.T) {
	is := is.New(t)
	con := kinesis.Destination{}

	ctrl := gomock.NewController(t)
	mockClient := client.NewMockKinesisClient(ctrl)

	destConfig := kinesis.DestinationConfig{}

	err := sdk.Util.ParseConfig(cfg, &destConfig)
	is.NoErr(err)
	con.Client = mockClient

	name := fmt.Sprintf("%s-name", destConfig.StreamARN)

	mockClient.EXPECT().DescribeStream(context.Background(), gomock.Any(), gomock.Any()).Times(1).Return(&aws.DescribeStreamOutput{
		StreamDescription: &types.StreamDescription{
			StreamARN:  &destConfig.StreamARN,
			StreamName: &name,
		},
	}, nil)

	err = con.Open(context.Background())
	is.NoErr(err)

	err = con.Teardown(context.Background())
	is.NoErr(err)
}

func setupWriteTest(t *testing.T, useSingleShard bool) kinesis.Destination {
	con := kinesis.Destination{}
	ctrl := gomock.NewController(t)
	mockClient := client.NewMockKinesisClient(ctrl)
	destConfig := kinesis.DestinationConfig{}

	_ = sdk.Util.ParseConfig(cfg, &destConfig)
	con.Client = mockClient
	destConfig.UseSingleShard = useSingleShard
	destConfig.StreamName = fmt.Sprintf("%s-name", destConfig.StreamARN)

	return con
}

func TestWrite_PutRecords(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	con := setupWriteTest(t, false)

	cases := []struct {
		testName                 string
		isErr                    bool
		expectedNumberOfRequests int
		records                  []sdk.Record
	}{
		{
			"happy path - <500 records",
			false,
			1,
			makeRecords(499, false),
		},
	}
	// setup table test
	for _, tt := range cases {
		t.Run(tt.testName, func(t *testing.T) {
			count, err := con.Write(ctx, tt.records)
			if tt.isErr {
				// handle err
			}

			is.NoErr(err)
			is.Equal(count, len(tt.records))
		})
	}
}

// func TestWrite_PutRecord(t *testing.T) {
// 	ctx := context.Background()
// 	is := is.New(t)
// 	con := setupWriteTest(t, true)

// 	cases := []struct {
// 		records []sdk.Record
// 	}{}

// 	// setup table test
// 	count, err := con.Write(ctx)
// }

func makeRecords(count int, greaterThan5MB bool) []sdk.Record {
	var records []sdk.Record
	for i := 0; i < count; i++ {
		data := make([]byte, 16)
		rand.Read(data)

		key := fmt.Sprintf("%d", i)
		rec := sdk.Util.Source.NewRecordCreate(
			[]byte(uuid.NewString()),
			nil,
			sdk.RawData(key),
			sdk.RawData(data),
		)

		records = append(records, rec)
	}
	return records
}
