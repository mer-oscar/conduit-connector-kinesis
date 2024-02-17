package destination

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
)

const (
	defaultBatchSize = 500
)

type Destination struct {
	sdk.UnimplementedDestination

	config Config

	// client is the Client for the AWS Kinesis API
	client *kinesis.Client
}

// NewDestination creates a Destination and wrap it in the default middleware.
func NewDestination() sdk.Destination {
	middlewares := sdk.DefaultDestinationMiddleware()
	for i, m := range middlewares {
		switch dest := m.(type) {
		case sdk.DestinationWithBatch:
			dest.DefaultBatchSize = defaultBatchSize
			middlewares[i] = dest
		default:
		}
	}

	return sdk.DestinationWithMiddleware(&Destination{}, middlewares...)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	// Parameters is a map of named Parameters that describe how to configure
	// the Destination. Parameters can be generated from DestinationConfig with
	// paramgen.
	return Config{}.Parameters()
}

func (d *Destination) Configure(ctx context.Context, cfg map[string]string) error {
	// Configure is the first function to be called in a connector. It provides
	// the connector with the configuration that can be validated and stored.
	// In case the configuration is not valid it should return an error.
	// Testing if your connector can reach the configured data source should be
	// done in Open, not in Configure.
	// The SDK will validate the configuration and populate default values
	// before calling Configure. If you need to do more complex validations you
	// can do them manually here.
	sdk.Logger(ctx).Info().Msg("Configuring Destination...")
	err := sdk.Util.ParseConfig(cfg, &d.config)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	// Open is called after Configure to signal the plugin it can prepare to
	// start writing records. If needed, the plugin should open connections in
	// this function.

	// Configure the creds for the client
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(d.config.AWSRegion),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				d.config.AWSAccessKeyID,
				d.config.AWSSecretAccessKey,
				"")),
	)
	if err != nil {
		return fmt.Errorf("failed to load aws config with given credentials : %w", err)
	}

	d.client = kinesis.NewFromConfig(cfg)

	// DescribeStream to know that the stream ARN is valid and usable, ie test connection
	_, err = d.client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: &d.config.StreamARN,
	})
	if err != nil {
		sdk.Logger(ctx).Error().Msg("error when attempting to test connection to stream")
		return err
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	if d.config.UseSingleShard {
		partition := uuid.New().String()
		var count int
		for _, record := range records {
			_, err := d.client.PutRecord(ctx, &kinesis.PutRecordInput{
				PartitionKey: &partition,
				Data:         record.Bytes(),
				StreamARN:    &d.config.StreamARN,
				StreamName:   &d.config.StreamName,
			})
			if err != nil {
				return count, err
			}

			count++
		}

		return count, nil
	}

	var entries []types.PutRecordsRequestEntry
	var reqs []*kinesis.PutRecordsInput

	// create the put records request
	for j := 0; j < len(records); j++ {
		key := string(records[j].Key.Bytes())
		recordEntry := types.PutRecordsRequestEntry{
			Data:         records[j].Bytes(),
			PartitionKey: &key,
		}
		entries = append(entries, recordEntry)
	}

	reqs = append(reqs, &kinesis.PutRecordsInput{
		StreamARN:  &d.config.StreamARN,
		StreamName: &d.config.StreamName,
		Records:    entries,
	})

	var written int

	// iterate through the PutRecords response and sum the successful record writes
	// over the entire batch of requests
	for _, req := range reqs {
		output, err := d.client.PutRecords(ctx, req)
		if err != nil {
			written += len(output.Records) - int(*output.FailedRecordCount)
			return written, err
		}
		written += len(output.Records)
	}

	return written, nil
}

func (d *Destination) Teardown(ctx context.Context) error {
	// no shutdown required
	return nil
}
