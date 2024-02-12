#!/bin/bash

# Check if a source directory was provided
if [ -z "$1" ]; then
  echo "Usage: $0 <source_directory>"
  exit 1
fi

source_dir="$1"

# Recursively traverse the specified directory tree
find "$source_dir" -type f \( -name "*.tar" -o -name "*.tar.gz" -o -name "*.bz2" -o -name "*.tar.bz" \) -exec bash -c '
  for file do
    # Extract the file in its current directory
    tar xvf "$file" -C "$(dirname "$file")"
    # Delete the original tar file
    rm "$file"
  done
' bash {} +
