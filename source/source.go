package source

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Source struct {
	sdk.UnimplementedSource

	config Config

	// client is the Client for the AWS Kinesis API
	client *kinesis.Client

	shards      []Shard
	buffer      chan sdk.Record
	consumerARN string
}

type Shard struct {
	ShardID     *string
	EventStream *kinesis.SubscribeToShardEventStream
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{
		buffer: make(chan sdk.Record, 1),
	}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	// Parameters is a map of named Parameters that describe how to configure
	// the Source. Parameters can be generated from SourceConfig with paramgen.
	return Config{}.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	// Configure is the first function to be called in a connector. It provides
	// the connector with the configuration that can be validated and stored.
	// In case the configuration is not valid it should return an error.
	// Testing if your connector can reach the configured data source should be
	// done in Open, not in Configure.
	// The SDK will validate the configuration and populate default values
	// before calling Configure. If you need to do more complex validations you
	// can do them manually here.

	sdk.Logger(ctx).Info().Msg("Configuring Source...")
	err := sdk.Util.ParseConfig(cfg, &s.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	// Configure the creds for the client
	var cfgOptions []func(*config.LoadOptions) error
	cfgOptions = append(cfgOptions, config.WithRegion(s.config.AWSRegion))
	cfgOptions = append(cfgOptions, config.WithCredentialsProvider(
		credentials.NewStaticCredentialsProvider(
			s.config.AWSAccessKeyID,
			s.config.AWSSecretAccessKey,
			"")))

	if s.config.AWSURL != "" {
		cfgOptions = append(cfgOptions, config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				PartitionID:       "aws",
				URL:               s.config.AWSURL,
				SigningRegion:     s.config.AWSRegion,
				HostnameImmutable: true,
			}, nil
		},
		)))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx,
		cfgOptions...,
	)
	if err != nil {
		return fmt.Errorf("failed to load aws config with given credentials : %w", err)
	}
	s.client = kinesis.NewFromConfig(awsCfg)

	return nil
}

func (s *Source) Open(ctx context.Context, _ sdk.Position) error {
	// Open is called after Configure to signal the plugin it can prepare to
	// start producing records. If needed, the plugin should open connections in
	// this function. The position parameter will contain the position of the
	// last record that was successfully processed, Source should therefore
	// start producing records after this position. The context passed to Open
	// will be cancelled once the plugin receives a stop signal from Conduit.

	// DescribeStream to know that the stream ARN is valid and usable, ie test connection
	_, err := s.client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: &s.config.StreamARN,
	})
	if err != nil {
		sdk.Logger(ctx).Error().Msg("error when attempting to test connection to stream")
		return err
	}

	// register consumer
	consumerResponse, err := s.client.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
		StreamARN:    &s.config.StreamARN,
		ConsumerName: aws.String("conduit-connector-kinesis-source"),
	})
	if err != nil {
		return fmt.Errorf("error registering consumer: %w", err)
	}

	s.consumerARN = *consumerResponse.Consumer.ConsumerARN

	go s.startSubscriptions(ctx)

	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	rec := <-s.buffer
	return rec, nil
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	// Ack signals to the implementation that the record with the supplied
	// position was successfully processed. This method might be called after
	// the context of Read is already cancelled, since there might be
	// outstanding acks that need to be delivered. When Teardown is called it is
	// guaranteed there won't be any more calls to Ack.
	// Ack can be called concurrently with Read.
	positionSplit := strings.Split(string(position), "_")

	shardID := positionSplit[0]
	seqNumber := positionSplit[1]

	sdk.Logger(ctx).Debug().Msg(fmt.Sprintf("ack'd record with shardID: %s and sequence number: %s", shardID, seqNumber))
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	// Teardown signals to the plugin that there will be no more calls to any
	// other function. After Teardown returns, the plugin should be ready for a
	// graceful shutdown.
	for _, stream := range s.shards {
		if stream.EventStream != nil {
			stream.EventStream.Close()
		}
	}

	if s.consumerARN != "" {
		_, err := s.client.DeregisterStreamConsumer(ctx, &kinesis.DeregisterStreamConsumerInput{
			ConsumerARN: &s.consumerARN,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func toRecords(kinRecords []types.Record, shardID *string) []sdk.Record {
	var sdkRecs []sdk.Record

	for _, rec := range kinRecords {
		// composite key of partition key and sequence number
		position := *shardID + "_" + *rec.SequenceNumber

		sdkRec := sdk.Util.Source.NewRecordCreate(
			sdk.Position(position),
			sdk.Metadata{
				"shardId":        *shardID,
				"sequenceNumber": *rec.SequenceNumber,
			},
			sdk.RawData(position),
			sdk.RawData(rec.Data),
		)

		sdkRecs = append(sdkRecs, sdkRec)
	}

	return sdkRecs
}

// keep subscriptions alive for 5 minutes, then refresh
func (s *Source) startSubscriptions(ctx context.Context) error {
	var startingPosition types.StartingPosition
	if s.config.StartFromLatest {
		startingPosition.Type = types.ShardIteratorTypeLatest
	} else {
		startingPosition.Type = types.ShardIteratorTypeTrimHorizon
	}

	// get shards
	listShardsResponse, err := s.client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamARN: &s.config.StreamARN,
	})
	if err != nil {
		return fmt.Errorf("error retrieving kinesis shards: %w", err)
	}

	for _, shard := range listShardsResponse.Shards {
		s.shards = append(s.shards, Shard{
			ShardID: shard.ShardId,
		})
	}

	// get iterators for shards
	for _, shard := range s.shards {
		shard := shard

		subscriptionResponse, err := s.client.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
			ConsumerARN:      &s.consumerARN,
			ShardId:          shard.ShardID,
			StartingPosition: &startingPosition,
		})
		if err != nil {
			return fmt.Errorf("error creating stream subscription: %w", err)
		}

		shard.EventStream = subscriptionResponse.GetStream()

		go func() {
			event := <-shard.EventStream.Events()
			if event == nil {
				return
			}

			eventValue := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent).Value

			if len(eventValue.Records) > 0 {
				recs := toRecords(eventValue.Records, shard.ShardID)

				for _, record := range recs {
					s.buffer <- record
				}
			}
		}()
	}

	// block for 5 minutes then refresh the streams
	<-time.After((4 * time.Minute) + (59 * time.Second))
	for _, shard := range s.shards {
		// close the streams so we can reopen them later
		if shard.EventStream != nil {
			shard.EventStream.Close()
		}
	}

	s.shards = make([]Shard, 0)

	go s.startSubscriptions(ctx)

	return nil
}
