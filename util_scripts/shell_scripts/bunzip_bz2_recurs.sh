#!/bin/bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 <directory>"
    exit 1
fi

if [ ! -d "$1" ]; then
    echo "Error: Directory '$1' does not exist"
    exit 1
fi

find "$1" -type f -name "*.bz2" -print0 | while IFS= read -r -d $'\0' file; do
    echo "Decompressing: $file"
    bunzip2 -k "$file" || echo "Failed to decompress: $file"
done

echo "Decompression complete"