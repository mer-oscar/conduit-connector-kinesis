package kinesis

import (
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/mer-oscar/conduit-connector-kinesis/destination"
	"github.com/mer-oscar/conduit-connector-kinesis/source"
)

// Connector combines all constructors for each plugin in one struct.
var Connector = sdk.Connector{
	NewSpecification: Specification,
	NewSource:        source.New,
	NewDestination:   destination.New,
}
