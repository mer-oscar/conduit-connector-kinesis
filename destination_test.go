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
	test "github.com/mer-oscar/conduit-connector-kinesis/test"
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
	mockClient := test.NewMockKinesisClient(ctrl)

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

func TestWrite_PutRecords(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	con := kinesis.Destination{}
	ctrl := gomock.NewController(t)
	client := test.NewMockKinesisClient(ctrl)

	err := con.Configure(ctx, cfg)
	is.NoErr(err)
	con.Client = client

	cases := []struct {
		testName                 string
		expectedError            error
		expectedNumberOfRequests int
		records                  []sdk.Record
	}{
		{
			"happy path - <500 records",
			nil,
			1,
			makeRecords(499, false),
		},
	}
	var failedCount int32

	// setup table test
	for _, tt := range cases {
		t.Run(tt.testName, func(t *testing.T) {
			var err error
			if tt.expectedError != nil {
				// handle err
				err = tt.expectedError
			}

			kRecs := make([]types.PutRecordsResultEntry, len(tt.records))

			client.EXPECT().PutRecords(gomock.Any(), gomock.Any(), gomock.Any()).Times(tt.expectedNumberOfRequests).
				Return(&aws.PutRecordsOutput{
					Records:           kRecs,
					FailedRecordCount: &failedCount,
				}, err)

			count, err := con.Write(ctx, tt.records)
			if err != nil {
				// handle err
				is.Equal(err, tt.expectedError)
			}

			is.NoErr(err)
			is.Equal(count, len(tt.records))
		})
	}
}

func TestWrite_PutRecord(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	destConfig := kinesis.DestinationConfig{}

	_ = sdk.Util.ParseConfig(cfg, &destConfig)
	destConfig.UseSingleShard = true
	destConfig.StreamName = fmt.Sprintf("%s-name", destConfig.StreamARN)

	con := kinesis.Destination{}
	ctrl := gomock.NewController(t)
	client := test.NewMockKinesisClient(ctrl)

	cfg["use_single_shard"] = "true"

	err := con.Configure(ctx, cfg)
	is.NoErr(err)
	con.Client = client

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
			var err error
			if tt.expectedError != nil {
				// handle err
				err = tt.expectedError
			}

			client.EXPECT().PutRecord(gomock.Any(), gomock.Any(), gomock.Any()).Times(tt.expectedNumberOfRequests).
				Return(&aws.PutRecordOutput{}, err)

			count, err := con.Write(ctx, tt.records)
			if err != nil {
				// handle err
				is.Equal(err, tt.expectedError)
			}

			is.NoErr(err)
			is.Equal(count, len(tt.records))
		})
	}
}

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

func toKinesisRecords(records []sdk.Record) []types.PutRecordsRequestEntry {
	var kinesisRecs []types.PutRecordsRequestEntry
	for _, rec := range records {
		key := string(rec.Key.Bytes())
		kRec := types.PutRecordsRequestEntry{
			Data:         rec.Bytes(),
			PartitionKey: &key,
		}

		kinesisRecs = append(kinesisRecs, kRec)
	}

	return kinesisRecs
}
