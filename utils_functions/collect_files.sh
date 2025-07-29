#!/bin/bash

# Output file
output_file="all_py_file_contents.txt"

# Clear or create the output file
> "$output_file"

# Save the tree output at the top
echo "===== Project Directory Structure =====" >> "$output_file"
tree >> "$output_file"

# Separator
echo -e "\n\n===== Python File Contents Below =====" >> "$output_file"

# Use a temp file to store .py file paths
temp_file_list=$(mktemp)

# Get all .py files from tree output
tree -fi | grep '\.py$' > "$temp_file_list"

# Loop through each .py file path
while IFS= read -r file; do
    if [[ -f "$file" ]]; then
        echo -e "\n\nContents of: $file :" >> "$output_file"
        echo "=====================================" >> "$output_file"

        if file "$file" | grep -q text; then
            cat "$file" >> "$output_file"
        else
            echo "[Skipped: Not a text file]" >> "$output_file"
        fi
    fi
done < "$temp_file_list"

# Clean up temp file
rm "$temp_file_list"
