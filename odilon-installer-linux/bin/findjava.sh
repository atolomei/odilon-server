#!/bin/bash
# findjava.sh — print the canonical path to the java binary
# Works on Linux (readlink -f) and macOS (realpath / cd+pwd fallback)

echo
if command -v realpath &>/dev/null; then
    realpath "$(which java)"
elif command -v greadlink &>/dev/null; then
    greadlink -f "$(which java)"
else
    # Pure POSIX fallback: follow one level via cd + pwd
    _j="$(which java)"
    _d="$(cd "$(dirname "$_j")" 2>/dev/null && pwd -P)"
    echo "${_d}/$(basename "$_j")"
fi
echo