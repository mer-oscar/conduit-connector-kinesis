package kinesis_test

import (
	"context"
	"testing"

	kinesis "github.com/mer-oscar/conduit-connector-kinesis"
	"github.com/matryer/is"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := kinesis.NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
