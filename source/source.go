package source

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/oklog/ulid/v2"
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

type kinesisPosition struct {
	SequenceNumber string
	ShardID        string
}

func New() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{
		buffer:    make(chan sdk.Record, 100),
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
		sdk.Logger(ctx).Err(err).Msg("error when attempting to test connection to stream")
		return err
	}

	// register consumer
	consumerResponse, err := s.client.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
		StreamARN:    &s.config.StreamARN,
		ConsumerName: aws.String("conduit-connector-kinesis-source-" + ulid.Make().String()),
	})
	if err != nil {
		return fmt.Errorf("error registering consumer: %w", err)
	}

	sdk.Logger(ctx).Info().Msg("kinesis consumer registered: " + *consumerResponse.Consumer.ConsumerName)

	s.consumerARN = consumerResponse.Consumer.ConsumerARN
	err = s.subscribeShards(ctx, pos)
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
	pos, err := parsePosition(position)
	if err != nil {
		return err
	}

	sdk.Logger(ctx).Debug().Msg(fmt.Sprintf("ack'd record with shardID: %s and sequence number: %s", pos.ShardID, pos.SequenceNumber))
	return nil
}

func (s *Source) Teardown(ctx context.Context) error {
	// if s.consumerARN != nil {
	// 	_, err := s.client.DeregisterStreamConsumer(ctx, &kinesis.DeregisterStreamConsumerInput{
	// 		ConsumerARN: s.consumerARN,
	// 		StreamARN:   &s.config.StreamARN,
	// 	})

	// 	if err != nil {
	// 		return err
	// 	}
	// }

	for _, stream := range s.streamMap {
		if stream != nil {
			stream.Close()
		}
	}

	return nil
}

func toRecords(kinRecords []types.Record, shardID string) []sdk.Record {
	var sdkRecs []sdk.Record

	for _, rec := range kinRecords {
		var kinPos kinesisPosition
		kinPos.SequenceNumber = *rec.SequenceNumber
		kinPos.ShardID = shardID

		kinPosBytes, _ := json.Marshal(kinPos)
		sdkRec := sdk.Util.Source.NewRecordCreate(
			sdk.Position(kinPosBytes),
			sdk.Metadata{
				"shardId":        "kinesis-" + shardID,
				"sequenceNumber": "kinesis-" + *rec.SequenceNumber,
			},
			sdk.RawData(kinPosBytes),
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
				select {
				case event := <-eventStream.Events():
					if event == nil {
						err := s.resubscribeShard(ctx, shardID)
						if err != nil {
							sdk.Logger(ctx).Err(err).Msg("error resubscribing to shard")
							return
						}
					}

					eventValue := event.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent).Value

					if len(eventValue.Records) > 0 {
						recs := toRecords(eventValue.Records, shardID)

						for _, record := range recs {
							s.buffer <- record
						}
					}
				case <-ctx.Done():
					return
				// refresh the subscription after 5 minutes since that is when kinesis subscriptions go stale
				case <-time.After(time.Minute*4 + time.Second*55):
					for shardID, stream := range s.streamMap {
						if stream != nil {
							stream.Close()
						}

						err := s.resubscribeShard(ctx, shardID)
						if err != nil {
							sdk.Logger(ctx).Err(err).Msg("error resubscribing to shard")
						}
					}
				}
			}
		}()
	}
}

func (s *Source) subscribeShards(ctx context.Context, position sdk.Position) error {
	// get shards
	listShardsResponse, err := s.client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamARN: &s.config.StreamARN,
	})
	if err != nil {
		return fmt.Errorf("error retrieving kinesis shards: %w", err)
	}

	var startingPosition types.StartingPosition
	switch {
	case s.config.StartFromLatest:
		startingPosition.Type = types.ShardIteratorTypeLatest
	case !s.config.StartFromLatest:
		startingPosition.Type = types.ShardIteratorTypeTrimHorizon
	case position != nil:
		pos, err := parsePosition(position)
		if err != nil {
			return err
		}

		startingPosition.Type = types.ShardIteratorTypeAfterSequenceNumber
		startingPosition.SequenceNumber = &pos.SequenceNumber
	}

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

func parsePosition(pos sdk.Position) (kinesisPosition, error) {
	var kinPos kinesisPosition
	err := json.Unmarshal(pos, &kinPos)
	if err != nil {
		return kinPos, err
	}

	return kinPos, nil
}
