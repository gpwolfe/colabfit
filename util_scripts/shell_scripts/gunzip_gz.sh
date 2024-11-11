#!/bin/bash

# Check for a source directory argument
if [ -z "$1" ]; then
  echo "Usage: $0 <source_directory>"
  exit 1
fi

source_dir="$1"

# Find all .gz files recursively and decompress them in place
find "$source_dir" -type f -name "*.gz" -exec gunzip '{}' \;
