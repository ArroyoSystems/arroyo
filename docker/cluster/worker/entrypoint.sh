#!/usr/bin/env bash

copy_artifacts "${WORKER_BIN}" "${WASM_BIN}" /

chmod +x /pipeline
/pipeline
