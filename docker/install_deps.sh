#!/bin/env bash

set -e

# Install arch specific build dependencies
echo env
if [ "$(uname -m)" = "x86_64" ]; then
  export PROTO_ARCH="x86_64";
  export MOLD_ARCH="x86_64";
  export PY_ARCH="x86_64";
elif [ "$(uname -m)" = "aarch64" ]; then
  export PROTO_ARCH="aarch_64";
  export MOLD_ARCH="aarch64";
  export PY_ARCH="aarch64";
else
  echo "Unsupported architecture: $(uname -m)"
  exit 1;
fi

# Install Python 3.12 (manually, as no bookworm package for 3.12)
curl -OL https://github.com/indygreg/python-build-standalone/releases/download/20240814/cpython-3.12.5+20240814-${PY_ARCH}-unknown-linux-gnu-install_only.tar.gz 
tar xvfz cpython*.tar.gz
cp -r python/bin/* /usr/local/bin/
cp -r python/include/* /usr/local/include/
cp -r python/lib/* /usr/local/lib/
cp -r python/share/* /usr/local/share/
ldconfig

# Install mold
curl -OL https://github.com/rui314/mold/releases/download/v1.11.0/mold-1.11.0-${MOLD_ARCH}-linux.tar.gz
tar xvfz mold*.tar.gz
mv mold*-linux/bin/* /usr/bin
mv mold*-linux/libexec/* /usr/libexec
rm -rf mold*

# Install protoc
curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v21.8/protoc-21.8-linux-${PROTO_ARCH}.zip
unzip protoc*.zip
mv bin/protoc /usr/local/bin
rm -rf protoc*.zip
