#!/bin/bash

for proto_file in *.proto
do
    filename=$(basename -- "$proto_file")
    filename_noext="${filename%.*}"
    output_file="${filename_noext}.bin"
    
    echo "Compiling $filename to $output_file"
    protoc --descriptor_set_out="$output_file" "$proto_file"
done