#!/usr/bin/env bash
#
# Compiles src/ and tests/ with javac (JDK 8 syntax) and runs the
# acceptance tests. Exits non-zero on any failure.
#
set -euo pipefail
cd "$(dirname "$0")"

# --- locate a JDK -----------------------------------------------------------
JAVAC="$(command -v javac || true)"
JAVA="$(command -v java || true)"
WIN=0

if [ -z "$JAVAC" ]; then
  # Fallback: use a Windows JDK (WSL interop or Git Bash / MSYS).
  for candidate in \
      /mnt/*/developSoftWare/Java/*/bin/javac.exe \
      /mnt/*/Java/*/bin/javac.exe \
      "/mnt/c/Program Files/Java/"*/bin/javac.exe \
      /mnt/*/*/jdk*/bin/javac.exe \
      /d/developSoftWare/Java/*/bin/javac.exe \
      "/c/Program Files/Java/"*/bin/javac.exe; do
    if [ -x "$candidate" ]; then
      JAVAC="$candidate"
      JAVA="$(dirname "$candidate")/java.exe"
      WIN=1
      break
    fi
  done
fi

if [ -z "$JAVAC" ]; then
  echo "ERROR: no javac found. Install JDK 8+ and put it on PATH." >&2
  exit 1
fi
if [ -z "$JAVA" ]; then
  JAVA="$(dirname "$JAVAC")/java"
fi

echo "Using javac: $JAVAC"

# --- compile ----------------------------------------------------------------
BUILD_DIR="build"
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

SOURCES="$(find src tests -name '*.java' | sort)"

if [ "$WIN" = "1" ] && command -v wslpath >/dev/null 2>&1; then
  # WSL: convert paths explicitly for the Windows JDK.
  NATIVE_SOURCES=()
  while IFS= read -r file; do
    NATIVE_SOURCES+=("$(wslpath -w "$file")")
  done <<< "$SOURCES"
  "$JAVAC" -encoding UTF-8 -source 8 -target 8 \
      -d "$(wslpath -w "$BUILD_DIR")" "${NATIVE_SOURCES[@]}"
  "$JAVA" -cp "$(wslpath -w "$BUILD_DIR")" com.gsb.eventstore.EventStoreTest
else
  # Linux/macOS javac, or Windows javac under Git Bash (MSYS converts paths).
  # shellcheck disable=SC2086
  "$JAVAC" -encoding UTF-8 -source 8 -target 8 -d "$BUILD_DIR" $SOURCES
  "$JAVA" -cp "$BUILD_DIR" com.gsb.eventstore.EventStoreTest
fi

echo
echo "run-tests.sh: ALL TESTS PASSED"
