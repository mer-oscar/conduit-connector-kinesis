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
	"github.com/google/uuid"
)

type Source struct {
	sdk.UnimplementedSource

	config Config

	// client is the Client for the AWS Kinesis API
	client *kinesis.Client

	streamMap   map[string]*kinesis.SubscribeToShardEventStream
	buffer      chan sdk.Record
	consumerARN *string
}

type Shard struct {
	ShardID     *string
	EventStream *kinesis.SubscribeToShardEventStream
}

func New() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{
		buffer:    make(chan sdk.Record, 1),
		streamMap: make(map[string]*kinesis.SubscribeToShardEventStream),
	}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() map[string]sdk.Parameter {
	return Config{}.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
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

	awsCfg, err := config.LoadDefaultConfig(ctx, cfgOptions...)
	if err != nil {
		return fmt.Errorf("failed to load aws config with given credentials : %w", err)
	}

	s.client = kinesis.NewFromConfig(awsCfg)

	return nil
}

func (s *Source) Open(ctx context.Context, pos sdk.Position) error {
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
		ConsumerName: aws.String("conduit-connector-kinesis-source-" + uuid.NewString()),
	})
	if err != nil {
		return fmt.Errorf("error registering consumer: %w", err)
	}

	s.consumerARN = consumerResponse.Consumer.ConsumerARN
	err = s.subscribeShards(ctx)
	if err != nil {
		return err
	}

	go s.listenEvents(ctx)

	return nil
}

func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	case rec := <-s.buffer:
		return rec, nil
	}
}

func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	positionSplit := strings.Split(string(position), "_")

	shardID := positionSplit[0]
	seqNumber := positionSplit[1]

	sdk.Logger(ctx).Debug().Msg(fmt.Sprintf("ack'd record with shardID: %s and sequence number: %s", shardID, seqNumber))
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	for _, stream := range s.streamMap {
		if stream != nil {
			stream.Close()
		}
	}

	if s.consumerARN != nil {
		_, err := s.client.DeregisterStreamConsumer(ctx, &kinesis.DeregisterStreamConsumerInput{
			ConsumerARN: s.consumerARN,
			StreamARN:   &s.config.StreamARN,
		})

		if err != nil {
			return err
		}
	}

	return nil
}

func toRecords(kinRecords []types.Record, shardID string) []sdk.Record {
	var sdkRecs []sdk.Record

	for _, rec := range kinRecords {
		// composite key of partition key and sequence number
		position := shardID + "_" + *rec.SequenceNumber

		sdkRec := sdk.Util.Source.NewRecordCreate(
			sdk.Position(position),
			sdk.Metadata{
				"shardId":        shardID,
				"sequenceNumber": *rec.SequenceNumber,
			},
			sdk.RawData(position),
			sdk.RawData(rec.Data),
		)

		sdkRecs = append(sdkRecs, sdkRec)
	}

	return sdkRecs
}

func (s *Source) listenEvents(ctx context.Context) {
	for shardID, eventStream := range s.streamMap {
		shardID, eventStream := shardID, eventStream

		go func() {
			for {
				event := <-eventStream.Events()
				if event == nil {
					break
				}

				eventValue := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent).Value

				if len(eventValue.Records) > 0 {
					recs := toRecords(eventValue.Records, shardID)

					for _, record := range recs {
						s.buffer <- record
					}
				}
			}
		}()
	}

	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Minute * 5):
		for shardID, stream := range s.streamMap {
			if stream != nil {
				stream.Close()
			}

			err := s.resubscribeShard(ctx, shardID)
			if err != nil {
				fmt.Println("error resub: ", err.Error())
				sdk.Logger(ctx).Error().Msg("error resubscribing to shard")
			}
		}
	}
}

func (s *Source) subscribeShards(ctx context.Context) error {
	// get shards
	listShardsResponse, err := s.client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamARN: &s.config.StreamARN,
	})
	if err != nil {
		return fmt.Errorf("error retrieving kinesis shards: %w", err)
	}

	var startingPosition types.StartingPosition
	if s.config.StartFromLatest {
		startingPosition.Type = types.ShardIteratorTypeLatest
	} else {
		startingPosition.Type = types.ShardIteratorTypeTrimHorizon
	}

	s.config.StartFromLatest = true

	// get iterators for shards
	for _, shard := range listShardsResponse.Shards {
		subscriptionResponse, err := s.client.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
			ConsumerARN:      s.consumerARN,
			ShardId:          shard.ShardId,
			StartingPosition: &startingPosition,
		})
		if err != nil {
			return fmt.Errorf("error creating stream subscription: %w", err)
		}

		s.streamMap[*shard.ShardId] = subscriptionResponse.GetStream()
	}

	return nil
}

func (s *Source) resubscribeShard(ctx context.Context, shardID string) error {
	subscriptionResponse, err := s.client.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
		ConsumerARN: s.consumerARN,
		ShardId:     aws.String(shardID),
		StartingPosition: &types.StartingPosition{
			Type: types.ShardIteratorTypeLatest,
		},
	})
	if err != nil {
		return fmt.Errorf("error creating stream subscription: %w", err)
	}

	s.streamMap[shardID] = subscriptionResponse.GetStream()

	return nil
}
