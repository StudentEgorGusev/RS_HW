#!/bin/sh

# Start docker daemon
dockerd-entrypoint.sh --storage-driver=overlay2 &> /var/log/dockerd-entrypoint.log &
sleep 3

# Build server image
docker build server -t hw3img

# Run tests
cd tests
export NO_COLOR=1
go test --docker -timeout 15m $@ || true