package kinesis

// Config contains shared config parameters, common to the source and
// destination. If you don't need shared parameters you can entirely remove this
// file.
type Config struct {
	// StreamARN is the Kinesis stream's Amazon Resource Name
	StreamARN string `json:"stream_arn" validate:"required"`
	// StreamName is the name of the Kinesis Data Stream
	StreamName string `json:"stream_name"`
	// AWSRegion is the region where the stream is hosted
	AWSRegion string `json:"aws_region" validate:"required"`
}
