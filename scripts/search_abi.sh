#!/bin/bash

directory="$1"      # The directory to search in
filename="$2"       # The filename to search for

# Find the file in the directory
mapfile -t found_files < <(find "$directory" -type f -name "$filename")

# Check if any files were found
if [ ${#found_files[@]} -eq 0 ]; then
    echo "Error: No files named '$filename' found in directory '$directory'." >&2
    exit 1 
else
    printf "%s\n" "${found_files[@]}"
fi
