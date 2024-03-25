package main

import (
	sdk "github.com/conduitio/conduit-connector-sdk"

	kinesis "github.com/mer-oscar/conduit-connector-kinesis"
)

func main() {
	sdk.Serve(kinesis.Connector)
}
