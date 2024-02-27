package main

import (
	"context"
	"fmt"

	"github.com/mer-oscar/conduit-connector-kinesis/destination"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var cfg map[string]string = map[string]string{
	"streamARN":           "arn:aws:kinesis:us-east-1:000000000000:stream/stream1",
	"aws.region":          "us-east-1",
	"aws.accessKeyId":     "accesskeymock",
	"aws.secretAccessKey": "accesssecretmock",
	"aws.url":             "http://localhost:4566",
}

func main() {
	ctx := context.Background()
	var configDst destination.Config

	err := sdk.Util.ParseConfig(cfg, &configDst)
	if err != nil {
		fmt.Println("invalid config: %w", err)
	}

	fmt.Println(configDst)

	// Configure the creds for the client
	var cfgOptions []func(*config.LoadOptions) error
	cfgOptions = append(cfgOptions, config.WithRegion(configDst.AWSRegion))
	cfgOptions = append(cfgOptions, config.WithCredentialsProvider(
		credentials.NewStaticCredentialsProvider(
			configDst.AWSAccessKeyID,
			configDst.AWSSecretAccessKey,
			"")))

	if configDst.AWSURL != "" {
		cfgOptions = append(cfgOptions, config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               configDst.AWSURL,
				SigningRegion:     configDst.AWSRegion,
				HostnameImmutable: true,
			}, nil
		},
		)))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx,
		cfgOptions...,
	)
	if err != nil {
		fmt.Println("failed to load aws config with given credentials : %w", err)
	}

	client := kinesis.NewFromConfig(awsCfg)

	listShards, err := client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamARN: &configDst.StreamARN,
	})
	if err != nil {
		fmt.Println("failed to get shards : %w", err)
		return
	}

	fmt.Println(len(listShards.Shards))

	var recs []types.Record
	for _, shard := range listShards.Shards {
		si, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			ShardId:           shard.ShardId,
			ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
			StreamARN:         &configDst.StreamARN,
		})
		if err != nil {
			fmt.Println("failed to get iterators : %w", err)
			return
		}

		getRecs, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
			StreamARN:     &configDst.StreamARN,
			ShardIterator: si.ShardIterator,
		})
		if err != nil {
			fmt.Println("failed to get records : %w", err)
			return
		}

		recs = append(recs, getRecs.Records...)
	}

	fmt.Println(len(recs), recs)
}
