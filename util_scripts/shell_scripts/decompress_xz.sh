#!/bin/bash

# Check if a directory is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <directory>"
    exit 1
fi

# Get the directory from the argument
DIRECTORY=$1

# Check if the provided argument is a directory
if [ ! -d "$DIRECTORY" ]; then
    echo "Error: $DIRECTORY is not a directory."
    exit 1
fi

# Find all .xz files and decompress them
find "$DIRECTORY" -type f -name "*.xz" -print0 | while IFS= read -r -d '' file; do
    echo "Decompressing: $file"
    xz -d "$file"
done

echo "Decompression complete."
