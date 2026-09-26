#!/bin/sh
# Builds and runs the event store test suite. Requires JDK 8+ (javac/java on PATH).
set -e

cd "$(dirname "$0")"

BUILD_DIR=build
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

echo "== Compiling (JDK 8 source/target) =="
# shellcheck disable=SC2046
javac -encoding UTF-8 -source 1.8 -target 1.8 -Xlint:all -d "$BUILD_DIR" \
    $(find src test -name '*.java' | sort)

echo "== Running tests =="
java -cp "$BUILD_DIR" com.gsb.eventstore.EventStoreTest
