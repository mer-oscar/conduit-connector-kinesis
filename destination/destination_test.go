package destination

import (
	"context"
	"crypto/rand"
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
	"useSingleShard":      "false",
	"streamARN":           "arn:aws:kinesis:us-east-1:000000000000:stream/stream1",
	"aws.region":          "us-east-1",
	"aws.accessKeyId":     "accesskeymock",
	"aws.secretAccessKey": "accesssecretmock",
}

func LocalKinesisClient(ctx context.Context, destConfig Config, is *is.I) *kinesis.Client {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(destConfig.AWSRegion),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				destConfig.AWSAccessKeyID,
				destConfig.AWSSecretAccessKey,
				"")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               "http://localhost:4566",
				SigningRegion:     destConfig.AWSRegion,
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
	con := Destination{}

	err := con.Configure(ctx, cfg)
	is.NoErr(err)

	con.client = LocalKinesisClient(ctx, con.config, is)

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

	con.client = LocalKinesisClient(ctx, con.config, is)

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
		})
	}

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

	con.client = LocalKinesisClient(ctx, con.config, is)

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
