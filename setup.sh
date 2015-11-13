#!/bin/sh
#
# setup.sh: prepare a Docker build environment
#
# This copies content from the go-cordinate source directory to the current
# directory, so that you can run "docker build" to get a container out.
# Typical use is:
#
#   mkdir docker-build
#   cd docker-build
#   ../setup.sh
#   docker build -t $USER/coordinated:$(cat container-version) .
#
# If a command-line argument is given to this script, it is taken as a
# build number and appended to the git-derived container version.

# Break automatically on any command error
set -e

# Directory containing this script
D=$(cd $(dirname "$0") && pwd -P)

# Output directory
O=$(pwd -P)

# Build number suffix (if any)
B=${1:+-$1}

# Pre-build a static binary
docker run --rm -it \
       -v "$GOPATH":/gopath \
       -v "$D":/usr/src/go-coordinate \
       -v "$O":/usr/src/output \
       -e GOPATH=/gopath \
       -e CGO_ENABLED=0 \
       -w /usr/src/go-coordinate \
       golang:1.5.1 \
       go build -a --installsuffix cgo --ldflags=-s -o /usr/src/output/coordinated.bin ./cmd/coordinated

# Create the version stamp file
VERSION_HASH=$(cd "$D" && git rev-list -n1 --abbrev-commit HEAD)
VERSION_TIME_T=$(cd "$D" && git rev-list -n1 --pretty='format:%ct' HEAD | tail -1)
VERSION_DATE=$(date --date="$VERSION_TIME_T" +%Y%m%d 2>/dev/null ||
                      date -j -r "$VERSION_TIME_T" +%Y%m%d)
echo "$VERSION_DATE-$VERSION_HASH$B" > "$O/container-version"

# Copy in any additional files required
if [ "$D" != "$O" ]; then
    cp -a "$D/Dockerfile" "$O"
fi
