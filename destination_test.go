package kinesis_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"strconv"
	"testing"

	aws "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
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

	destConfig := kinesis.DestinationConfig{}
	err := sdk.Util.ParseConfig(cfg, &destConfig)
	is.NoErr(err)

	err = con.Configure(ctx, cfg)
	is.NoErr(err)
	con.Client = client

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
		{
			"happy path - large size",
			nil,
			makeRecords(5, true),
		},
	}

	// setup table test
	for _, tt := range cases {
		recs := tt.records
		var failedCount int32

		t.Run(tt.testName, func(t *testing.T) {
			var err error
			if tt.expectedError != nil {
				// handle err
				is.Equal(err, tt.expectedError)
			}

			reqs := makeRequestFromRecords(destConfig.StreamARN, destConfig.StreamName, tt.records)

			for _, req := range reqs {
				kRecsResult := make([]types.PutRecordsResultEntry, len(req.Records))
				client.EXPECT().PutRecords(gomock.Any(), req, gomock.Any()).AnyTimes().
					Return(&aws.PutRecordsOutput{
						Records:           kRecsResult,
						FailedRecordCount: &failedCount,
					}, err)
			}

			count, err := con.Write(ctx, recs)
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
	oneMB := 1024 * 1024
	for i := 0; i < count; i++ {
		data := make([]byte, 16)
		rand.Read(data)

		if greaterThan5MB {
			data = make([]byte, oneMB)
			rand.Read(data)
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

func makeRequestFromRecords(streamARN, streamName string, records []sdk.Record) []*aws.PutRecordsInput {
	var entries []types.PutRecordsRequestEntry
	var reqs []*aws.PutRecordsInput
	var mib5 int = 5 * (10 << 2)
	var mib1 int = 1 * (10 << 2)

	sizeLimit := mib5

	// create the put records request
	for j := 0; j < len(records); j++ {
		if len(records[j].Bytes()) > mib1 {
			fmt.Println("test record: " + string(records[j].Key.Bytes()) + " larger than 1MB, skipping...")
			continue
		}

		sizeLimit -= len(records[j].Bytes())

		// size limit reached
		if sizeLimit <= 0 {
			reqs = append(reqs, &aws.PutRecordsInput{
				StreamARN:  &streamARN,
				StreamName: &streamName,
				Records:    entries,
			})

			sizeLimit = mib5
			entries = []types.PutRecordsRequestEntry{}

			j--

			continue
		}

		key := string(records[j].Key.Bytes())
		recordEntry := types.PutRecordsRequestEntry{
			Data:         records[j].Bytes(),
			PartitionKey: &key,
		}
		entries = append(entries, recordEntry)
	}

	reqs = append(reqs, &aws.PutRecordsInput{
		StreamARN:  &streamARN,
		StreamName: &streamName,
		Records:    entries,
	})

	return reqs
}
