# Conduit Connector for AWS Kinesis
[Conduit](https://conduit.io) for [AWS Kinesis](https://aws.amazon.com/kinesis/).

## How to build?
Run `make build` to build the connector.

## Testing
Run `make test-integration` to run the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the required resource (AWS Kinesis via Localstack) locally.

## Source
The Source connector for AWS Kinesis opens subscriptions to each of the available shards in the stream and pushes records into the buffer until
the subscription is up to date (all present source records read), at which point it switches to capturing the latest events in the stream. Every 5 minutes (the lifetime of the subscription), the subscription to the shard is refreshed.


### Configuration

| name                  | description                                      | required | default value |
|-----------------------|--------------------------------------------------|----------|---------------|
| `aws.accessKeyId`     | Access Key ID associated with your AWS resources | true     | ""            |
| `aws.secretAccessKey` | Secret Access Key associated with your AWS resources | true     | ""            |
| `aws.region`     | Region associated with your AWS resources | true     | ""            |
| `streamName`     | The AWS Kinesis stream name | false     | ""            |
| `streamARN`     | The AWS Kinesis stream ARN | true     | ""            |
| `aws.url`     | (LOCAL TESTING ONLY) the url override to test with localstack | false     | ""            |
| `startFromLatest`     | Set this value to true to ignore any records already in the stream  | false     | false           |


## Destination
The Destination connector for AWS Kinesis writes records to the stream either to a single shard or to multiple shards through the `useSingleShard` boolean configuration parameter. The size limit for a single record is 1MB, attempting to write a single record's data which is greater than 1MB will result in an error.

### Configuration

| name                       | description                                | required | default value |
|----------------------------|--------------------------------------------|----------|---------------|
| `aws.accessKeyId`     | Access Key ID associated with your AWS resources | true     | ""            |
| `aws.secretAccessKey` | Secret Access Key associated with your AWS resources | true     | ""            |
| `aws.region`     | Region associated with your AWS resources | true     | ""            |
| `streamName`     | The AWS Kinesis stream name | false     | ""            |
| `streamARN`     | The AWS Kinesis stream ARN | true     | ""            |
| `aws.url`     | (LOCAL TESTING ONLY) the url override to test with localstack | false     | ""            |

## Known Issues & Limitations


## Planned work

