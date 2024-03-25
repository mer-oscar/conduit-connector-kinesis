package kinesis

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
)

// version is set during the build process with ldflags (see Makefile).
// Default version matches default from runtime/debug.
var version = "(devel)"

// Specification returns the connector's specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:        "kinesis",
		Summary:     "A Conduit Connector for AWS Kinesis Data Streaming",
		Description: "A source and destination connector for AWS Kinesis Data Streaming",
		Version:     version,
		Author:      "Oscar Villavicencio",
	}
}
