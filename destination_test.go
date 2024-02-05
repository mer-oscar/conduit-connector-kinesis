package kinesis_test

import (
	"context"
	"testing"

	"github.com/matryer/is"
	kinesis "github.com/mer-oscar/conduit-connector-kinesis"
)

func TestTeardown_NoOpen(t *testing.T) {
	is := is.New(t)
	con := kinesis.NewDestination()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestTeardown_Open(t *testing.T) {
	is := is.New(t)
	con := kinesis.NewDestination()
	err := con.Open(ctx)
	is.NoErr(err)
	err = con.Teardown(context.Background())
	is.NoErr(err)
}
