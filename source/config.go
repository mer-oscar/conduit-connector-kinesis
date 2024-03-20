package source

import "github.com/mer-oscar/conduit-connector-kinesis/common"

//go:generate paramgen -output=config_paramgen.go Config
type Config struct {
	common.Config

	// startFromLatest defaults to false. When true, sets the iterator type to
	// LATEST (iterates from the point that the connection begins = CDC). when false, sets the iterator type
	// to TRIM_HORIZON (iterates from the oldest record in the shard = snapshot). Iterators eventually
	// shift to latest after snapshot has been written
	StartFromLatest bool `json:"startFromLatest" default:"false"`
}
