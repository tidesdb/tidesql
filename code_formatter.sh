#!/bin/bash

# Before submitting a PR, run this script to format the source code.

EXCLUDE_DIRS="external\|cmake-build-debug\|.idea|build|cmake"

find . -type f -name "*.cpp" -o -name "*.hpp" | grep -v "$EXCLUDE_DIRS" | xargs clang-format -i