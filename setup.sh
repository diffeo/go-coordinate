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

GO_MODULE="github.com/diffeo/go-coordinate"
GO_SUBMODULE="cmd/coordinated"

# Break automatically on any command error
set -e

# Directory containing this script
D=$(cd "$(dirname "$0")" && pwd -P)

# Output directory
O=$(pwd -P)

# Build number suffix (if any)
B=${1}

# Create a Go environment (if needed)
if [ -z "$GOPATH" ]; then
    export GOPATH="$O/go"
    mkdir -p "$GOPATH/src/$GO_MODULE"
    (cd "$D" && (find . -name '*.go' -print0; find .git -type f -print0) | xargs -0 tar cf "$O/go.tar")
    (cd "$GOPATH/src/$GO_MODULE" && tar xf "$O/go.tar")
    (cd "$GOPATH/src/$GO_MODULE" && go get -u "./$GO_SUBMODULE")
fi

# Pre-build a static binary
(cd "$GOPATH/src/$GO_MODULE" && \
 CGO_ENABLED=0 GOOS=linux \
 go build -a --ldflags=-s -o "$O/coordinated.bin" "./$GO_SUBMODULE")

# Append labels to the Dockerfile
if [ "$D" = "$O" ]; then
    sed -i .bak -e '/^LABEL/,$d' "$O/Dockerfile"
else
    cp -a "$D/Dockerfile" "$O"
fi
V=$(cd "$D" && git describe HEAD)
echo "$V$B" > "$O/container-version"
NOW=$(TZ=UTC date +%Y-%m-%dT%H:%M:%SZ)
cat >>"$O/Dockerfile" <<EOF
LABEL name="coordinated" \\
      version="$V" \\
      release="$B" \\
      architecture="x86_64" \\
      build_date="$NOW" \\
      vendor="Diffeo, Inc." \\
      url="https://github.com/diffeo/go-coordinate" \\
      summary="Coordinate job queue daemon" \\
      description="Coordinate job queue daemon" \\
      vcs-type="git" \\
      vcs-url="https://github.com/diffeo/go-coordinate" \\
      vcs-ref="$V" \\
      distribution-scope="public"
EOF
