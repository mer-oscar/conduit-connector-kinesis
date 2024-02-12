package kinesis_test

import (
	"context"
	"fmt"
	"testing"

	aws "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
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

func TestPutRecords(t *testing.T) {
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

	// setup table test
}
