#!/bin/bash

# Get the list of modified or new files
files=$(git diff --name-only --diff-filter=AM | grep -E '\.cc$|\.h$')

# Check if there are any files to process
if [ -z "$files" ]; then
  echo "No modified or new C++ files."
  exit 0
fi

# Run clang-tidy on the files
echo "Running clang-tidy on modified/new files..."
for file in $files; do
  clang-tidy --quiet  -p compile_commands.json "$file"
done

