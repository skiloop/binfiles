#!/usr/bin/env bash

# STEP 1: Determinate the required values

PACKAGE="github.com/skiloop/binfiles"
BIN_NAME="binutil"
VERSION="$(git describe --tags --always --abbrev=0 --match='v[0-9]*.[0-9]*.[0-9]*' 2> /dev/null | sed 's/^.//')"
COMMIT_HASH="$(git rev-parse --short HEAD)"
BUILD_TIMESTAMP=$(date '+%Y-%m-%dT%H:%M:%S')
TARGET_OS=${1}
if [[ "${TARGET_OS}" == "" ]]; then
    TARGET_OS="$(uname | tr -d '\n'|tr 'A-Z' 'a-z')"
fi
echo "VERSION         : ${VERSION}"
echo "COMMIT_HASH     : ${COMMIT_HASH}"
echo "BUILD_TIMESTAMP : ${BUILD_TIMESTAMP}"
echo "TARGET_OS       : ${TARGET_OS}"
# STEP 2: Build the ldflags

LDFLAGS=(
  "-X '${PACKAGE}/version.Version=${VERSION}'"
  "-X '${PACKAGE}/version.CommitHash=${COMMIT_HASH}'"
  "-X '${PACKAGE}/version.BuildTime=${BUILD_TIMESTAMP}'"
)

# STEP 3: Actual Go build process
case "${TARGET_OS}" in
"linux")
  GOOS=linux
  GOARCH=amd64
  ;;
"windows")
  GOOS=windows
  GOARCH=amd64
  TARGET="${BIN_NAME}.exe"
  ;;
"*")
  GOOS=darwin
  GOARCH=amd64
  ;;
esac
GOOS=${GOOS} GOARCH=${GOARCH} go build -ldflags="${LDFLAGS[*]}" -o "${TARGET:-${BIN_NAME}}"