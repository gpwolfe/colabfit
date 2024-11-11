#!/bin/csh

# Check if the correct number of arguments is provided
if ($#argv != 1) then
    echo "Usage: $0 <input_directory>"
    exit 1
endif

# Assign input directory to a variable
set input_dir = $1

if (! -d $input_dir) then
    echo "Error: Input directory does not exist."
    exit 1
endif

set files = `find $input_dir -type f -name "*.bz2"`

foreach file ($files)
    # Extract the file without the .gz extension
    # set output_subdir = `basename $file .gz`

    bzip2 -d $file 

    
end

echo "Decompression completed."