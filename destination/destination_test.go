package destination

import (
	"context"
	"crypto/rand"
	"fmt"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

var cfg map[string]string = map[string]string{
	"use_single_shard": "false",
	"stream_arn":       "aws:stream1",
	"aws_region":       "us-east",
}

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestTeardown_Open(t *testing.T) {
	is := is.New(t)
	con := Destination{}

	destConfig := Config{}

	err := sdk.Util.ParseConfig(cfg, &destConfig)
	is.NoErr(err)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(destConfig.AWSRegion),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				destConfig.AWSAccessKeyID,
				destConfig.AWSSecretAccessKey,
				"")),
	)
	is.NoErr(err)

	con.client = kinesis.NewFromConfig(cfg, func(o *kinesis.Options) {
		o.BaseEndpoint = aws.String("https://localhost:4567")
	})

	err = con.Open(context.Background())
	is.NoErr(err)

	err = con.Teardown(context.Background())
	is.NoErr(err)
}

func TestWrite_PutRecords(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)
	con := Destination{}

	destConfig := Config{}
	err := sdk.Util.ParseConfig(cfg, &destConfig)
	is.NoErr(err)

	err = con.Configure(ctx, cfg)
	is.NoErr(err)

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
		})
	}
}

func TestWrite_PutRecord(t *testing.T) {
	ctx := context.Background()
	is := is.New(t)

	destConfig := Config{}

	_ = sdk.Util.ParseConfig(cfg, &destConfig)
	destConfig.UseSingleShard = true
	destConfig.StreamName = fmt.Sprintf("%s-name", destConfig.StreamARN)

	con := Destination{}

	cfg["use_single_shard"] = "true"

	err := con.Configure(ctx, cfg)
	is.NoErr(err)

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
		})
	}
}

func makeRecords(count int, greaterThan5MB bool) []sdk.Record {
	var records []sdk.Record
	oneMB := (1024 * 1024)

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
