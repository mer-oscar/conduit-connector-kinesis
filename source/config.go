package source

import "github.com/mer-oscar/conduit-connector-kinesis/common"

//go:generate paramgen -output=config_paramgen.go Config
type Config struct {
	common.Config

	StartFromLatest bool `json:"start_from_latest" default:"true"`
}
