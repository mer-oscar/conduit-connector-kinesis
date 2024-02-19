package destination

import "github.com/mer-oscar/conduit-connector-kinesis/common"

//go:generate paramgen -output=config_paramgen.go Config
type Config struct {
	// Config includes parameters that are the same in the source and destination.
	common.Config

	// UseSingleShard is a boolean that determines whether to add records in batches using KinesisClient.PutRecords
	// (ordering not guaranteed, records written to multiple shards)
	// or to add records to a single shard using KinesisClient.PutRecord
	// (preserves ordering, records written to a single shard)
	// Defaults to false to take advantage of batching performance
	UseSingleShard bool `json:"useSingleShard" validate:"required" default:"true"`
}
