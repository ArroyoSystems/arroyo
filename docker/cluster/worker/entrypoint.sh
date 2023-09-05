#!/usr/bin/env bash

set -e

copy-artifacts "${WORKER_BIN}" "${WASM_BIN}" /

chmod +x /pipeline
/pipeline
