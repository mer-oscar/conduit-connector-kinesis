package kinesis_test

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matryer/is"
	kinesis "github.com/mer-oscar/conduit-connector-kinesis"
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
	con := kinesis.NewDestination()
	ctrl := gomock.NewController(t)
	m := NewMockKinesisClient(ctrl)

	err := con.Configure(context.Background(), cfg)
	is.NoErr(err)
	err = con.Open(context.Background())
	is.NoErr(err)
	err = con.Teardown(context.Background())
	is.NoErr(err)
}
