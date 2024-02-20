package source

import "github.com/mer-oscar/conduit-connector-kinesis/common"

//go:generate paramgen -output=config_paramgen.go Config
type Config struct {
	common.Config

	// startFromLatest defaults to false. When true, sets the iterator type to
	// LATEST (iterates from the point that the connection begins). when false, sets the iterator type
	// to TRIM_HORIZON (iterates from the oldest record in the shard).
	StartFromLatest bool `json:"startFromLatest"`
}
