#!/bin/sh
export VERSION=$(git describe --tags --dirty --always)

docker build --build-arg VERSION \
  -t conduit_with_kinesis:latest . \


cd test/destination && docker run -it -p 8080:8080 \
  -v ./pipeline.yaml:/app/pipelines/pipeline.yaml \
  -v ./example.in:/app/example.in \
  --add-host=host.docker.internal:host-gateway \
  -e CONDUIT_LOG_LEVEL=debug \
  conduit_with_kinesis:latest \
