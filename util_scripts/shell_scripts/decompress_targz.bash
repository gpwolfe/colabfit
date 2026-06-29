#!/bin/bash

# Check if the correct number of arguments is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <input_directory>"
    exit 1
fi

# Assign input directory to a variable
input_dir="$1"

# Check if directory exists
if [ ! -d "$input_dir" ]; then
    echo "Error: Input directory does not exist."
    exit 1
fi

# Find all tar.gz files in the input directory
files=$(find "$input_dir" -type f -name "*.tar.gz")

# Process each file
for file in $files; do
    # Extract the file
    tar -xvf "$file" -C "$(dirname "$file")"
    # Remove the original compressed file
    rm "$file"
done

echo "Decompression completed."