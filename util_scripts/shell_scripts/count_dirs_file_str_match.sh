#!/bin/bash

if [ $# -ne 2 ]; then
    echo "Usage: $0 <directory> <file-ending>"
    exit 1
fi

if [ ! -d "$1" ]; then
    echo "Error: '$1' is not a directory"
    exit 1
fi

base_dir="$1"
pattern="$2"

total_dirs=0
matching_dirs=0
echo "Directories without matching files:"
echo "--------------------------------"

while IFS= read -r dir; do
    ((total_dirs++))
    if find "$dir" -maxdepth 1 -type f -name "*$pattern" | grep -q .; then
        ((matching_dirs++))
    else
        echo "$dir"
    fi
done < <(find "$base_dir" -mindepth 1 -type d)

echo -e "\nSummary:"
echo "Total subdirectories: $total_dirs"
echo "Subdirectories containing files ending in '$pattern': $matching_dirs"
echo "Subdirectories without matching files: $((total_dirs - matching_dirs))"