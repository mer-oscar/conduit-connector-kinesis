package kinesis_test

import (
	"context"
	"testing"

	kinesis "github.com/mer-oscar/conduit-connector-kinesis"
	"github.com/matryer/is"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := kinesis.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}
