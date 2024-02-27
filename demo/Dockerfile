FROM golang:1.22-alpine as build
ARG VERSION
COPY . .
RUN	go build -ldflags "-X 'github.com/mer-oscar/conduit-connector-kinesis.version=${VERSION}'" -o /bin/conduit-connector-kinesis cmd/connector/main.go

FROM ghcr.io/conduitio/conduit:latest

COPY --from=build /bin/conduit-connector-kinesis /app/connectors/conduit-connector-kinesis
