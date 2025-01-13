#!/bin/bash

bazel run @hedron_compile_commands//:refresh_all

use_cache=false
if [[ "$1" == "--cache" ]]; then
  use_cache=true
fi

# Get the list of modified or new files
if $use_cache; then
  files=$(git diff --cached --name-only --diff-filter=AM | grep -E '\.cc$|\.h$')
else
  files=$(git diff --name-only --diff-filter=AM | grep -E '\.cc$|\.h$')
fi

# Check if there are any files to process
if [ -z "$files" ]; then
  echo "No modified or new C++ files."
  exit 0
fi

# Run clang-tidy on the files
echo "Running clang-tidy on modified/new files..."
for file in $files; do
  clang-tidy --quiet  -p compile_commands.json "$file"
  if ! clang-format -output-replacements-xml "$file" | grep -q "<replacement "; then
    continue
  fi
done

