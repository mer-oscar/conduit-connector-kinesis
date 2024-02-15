package kinesis

//go:generate paramgen -output=paramgen_dest.go DestinationConfig

import (
	"context"
	"fmt"

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

	config DestinationConfig

	// client is the Client for the AWS Kinesis API
	Client KinesisClient
}

type DestinationConfig struct {
	// Config includes parameters that are the same in the source and destination.
	Config

	// UseSingleShard is a boolean that determines whether to add records in batches using KinesisClient.PutRecords
	// (ordering not guaranteed, records written to multiple shards)
	// or to add records to a single shard using KinesisClient.PutRecord
	// (preserves ordering, records written to a single shard)
	// Defaults to false to take advantage of batching performance
	UseSingleShard bool `json:"use_single_shard" validate:"required" default:"true"`
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
	return d.config.Parameters()
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

	d.Client = kinesis.New(kinesis.Options{
		Region: d.config.AWSRegion,
	})

	return nil
}

func (d *Destination) Open(ctx context.Context) error {
	// Open is called after Configure to signal the plugin it can prepare to
	// start writing records. If needed, the plugin should open connections in
	// this function.

	// DescribeStream to know that the stream ARN is valid and usable, ie test connection
	stream, err := d.Client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamARN: &d.config.StreamARN,
	})
	if err != nil {
		sdk.Logger(ctx).Error().Msg("error when attempting to test connection to stream")
		return err
	}

	// add the stream name with requests also
	d.config.StreamName = *stream.StreamDescription.StreamName

	return nil
}

func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	// Write writes len(r) records from r to the destination right away without
	// caching. It should return the number of records written from r
	// (0 <= n <= len(r)) and any error encountered that caused the write to
	// stop early. Write must return a non-nil error if it returns n < len(r).

	if d.config.UseSingleShard {
		return d.putRecord(ctx, records)
	}

	return d.putRecords(ctx, records)
}

func (d *Destination) Teardown(ctx context.Context) error {
	// Teardown signals to the plugin that all records were written and there
	// will be no more calls to any other function. After Teardown returns, the
	// plugin should be ready for a graceful shutdown.
	if d.Client != nil {
		d.Client = nil
	}

	return nil
}

func (d *Destination) putRecord(ctx context.Context, records []sdk.Record) (int, error) {
	partition := uuid.New().String()
	var count int
	for _, record := range records {
		_, err := d.Client.PutRecord(ctx, &kinesis.PutRecordInput{
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

func (d *Destination) putRecords(ctx context.Context, records []sdk.Record) (int, error) {
	// Kinesis put records requests have a size limit of 5 MB per request, 1 MB per record,
	// and a count limit of 500 records per request, so generate the records requests first
	var entries []types.PutRecordsRequestEntry
	var reqs []*kinesis.PutRecordsInput
	var mib5 int = 5 << (10 * 2)
	var mib1 int = 1 << (10 * 2)

	sizeLimit := mib5

	// create the put records request
	for j := 0; j < len(records); j++ {
		if len(records[j].Bytes()) > mib1 {
			sdk.Logger(ctx).Error().Msg("record: " + string(records[j].Key.Bytes()) + " larger than 1MB, skipping...")
			continue
		}

		sizeLimit -= len(records[j].Bytes())

		// size limit reached
		if sizeLimit <= 0 {
			reqs = append(reqs, &kinesis.PutRecordsInput{
				StreamARN:  &d.config.StreamARN,
				StreamName: &d.config.StreamName,
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

	if len(entries) == 0 {
		return 0, fmt.Errorf("records were too big to insert")
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
		output, err := d.Client.PutRecords(ctx, req)
		if err != nil {
			written += len(output.Records) - int(*output.FailedRecordCount)
			return written, err
		}
		written += len(output.Records)
	}

	return written, nil
}
