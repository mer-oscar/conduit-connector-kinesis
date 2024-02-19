package common

// Config contains shared config parameters, common to the source and
// destination. If you don't need shared parameters you can entirely remove this
// file.
type Config struct {
	// amazon access key id
	AWSAccessKeyID string `json:"aws.accessKeyId" validate:"required"`
	// amazon secret access key
	AWSSecretAccessKey string `json:"aws.secretAccessKey" validate:"required"`
	// AWSRegion is the region where the stream is hosted
	AWSRegion string `json:"aws.region" validate:"required"`
	// StreamARN is the Kinesis stream's Amazon Resource Name
	StreamARN string `json:"streamARN" validate:"required"`
	// StreamName is the name of the Kinesis Data Stream
	StreamName string `json:"streamName"`

	// URL for endpoint override - testing/dry-run only
	AWSURL string `json:"aws.url"`
}
