#!/bin/bash
export CGO_ENABLED=0
archs=(amd64)
platforms=(windows darwin linux)
for platform in "${platforms[@]}"; do
  for arch in "${archs[@]}"; do
    echo "Build release for $platform and $arch"
    export GOOS=${platform} GOARCH=${arch}
    go build -a -o "releases/$platform/"
  done
done
exit 0
