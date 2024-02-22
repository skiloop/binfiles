#!/usr/bin/env bash

# STEP 1: Determinate the required values

PACKAGE="github.com/skiloop/binfiles"
VERSION="$(git describe --tags --always --abbrev=0 --match='v[0-9]*.[0-9]*.[0-9]*' 2> /dev/null | sed 's/^.//')"
COMMIT_HASH="$(git rev-parse --short HEAD)"
BUILD_TIMESTAMP=$(date '+%Y-%m-%dT%H:%M:%S')

echo "VERSION         : ${VERSION}"
echo "COMMIT_HASH     : ${COMMIT_HASH}"
echo "BUILD_TIMESTAMP : ${BUILD_TIMESTAMP}"
# STEP 2: Build the ldflags

LDFLAGS=(
  "-X '${PACKAGE}/version.Version=${VERSION}'"
  "-X '${PACKAGE}/version.CommitHash=${COMMIT_HASH}'"
  "-X '${PACKAGE}/version.BuildTime=${BUILD_TIMESTAMP}'"
)

# STEP 3: Actual Go build process

go build -ldflags="${LDFLAGS[*]}" -o binutil