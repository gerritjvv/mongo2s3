#!/usr/bin/env bash

CMD="$1"
shift

set -euo pipefail
IFS=$'\n\t'


case $CMD in
 lint )
   go vet ./...
   ;;
 test )
   go test ./...
   ;;
 build )
  echo "build amd64 mongo2s3-amd64"
  GOOS=linux GOARCH=amd64 go build -o mongo2s3-amd64 ./cmd/
  echo "build arm mongo2s3-arm64"
  GOOS=linux GOARCH=arm64 go build -o mongo2s3-arm64 ./cmd/
  echo "build macos mongo2s3-darwin-arm64"
  GOOS=darwin GOARCH=arm64 go build -o mongo2s3-darwin-arm64 ./cmd
  ;;
 * )
   echo "Undefined command $CMD try (lint, test, build)"
esac