package com.gsb.eventstore;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Self-contained acceptance tests. No test framework: every check throws an
 * AssertionError on failure and main() exits non-zero, so run-tests.sh stays
 * green only if everything passes.
 */
public class EventStoreTest {

    private static int passed = 0;

    public static void main(String[] args) throws Exception {
        testUpgradeChainToHighestVersion();
        testReadWithOlderUpcasterGraph();
        testMissingUpcasterThrows();
        testUnknownFieldsSurviveUpgradeAndWriteBack();
        testDeepEqualityRoundTrip();
        testSequentialVersionsEnforced();
        testPayloadTypeValidation();
        System.out.println();
        System.out.println("ALL " + passed + " TESTS PASSED");
    }

    /** v1 events must read back as v3 once v1->v2 and v2->v3 are registered. */
    private static void testUpgradeChainToHighestVersion() throws Exception {
        File dir = newTempDir();
        try {
            EventStore store = new EventStore(dir.getAbsolutePath());
            store.registerUpcaster(1, 2, upcasterV1toV2());
            store.registerUpcaster(2, 3, upcasterV2toV3());

            store.append("orders", 1, map("name", "alice", "age", 30L));
            store.append("orders", 2, map("fullName", "bob", "age", 41L));
            store.append("orders", 3,
                    map("fullName", "carol", "years", 25L, "country", "US", "nickname", "c"));

            List<Map<String, Object>> events = store.read("orders");
            checkEquals(3, events.size(), "event count");
            checkEquals(map("fullName", "alice", "years", 30L, "country", "CN"),
                    events.get(0), "v1 event upgraded v1->v2->v3");
            checkEquals(map("fullName", "bob", "years", 41L, "country", "CN"),
                    events.get(1), "v2 event upgraded v2->v3");
            checkEquals(map("fullName", "carol", "years", 25L, "country", "US", "nickname", "c"),
                    events.get(2), "v3 event returned as stored");
            ok("v1 events are upgraded step by step to v3 once v1->v2->v3 is registered");
        } finally {
            deleteRecursively(dir);
        }
    }

    /** A graph registered only up to v2 must read the same file without errors. */
    private static void testReadWithOlderUpcasterGraph() throws Exception {
        File dir = newTempDir();
        try {
            EventStore writer = new EventStore(dir.getAbsolutePath());
            writer.registerUpcaster(1, 2, upcasterV1toV2());
            writer.registerUpcaster(2, 3, upcasterV2toV3());
            writer.append("orders", 1, map("name", "alice", "age", 30L));
            writer.append("orders", 2, map("fullName", "bob", "age", 41L));
            writer.append("orders", 3,
                    map("fullName", "carol", "years", 25L, "country", "US", "nickname", "c"));

            // A second reader over the same files that only knows up to v2.
            EventStore reader = new EventStore(dir.getAbsolutePath());
            reader.registerUpcaster(1, 2, upcasterV1toV2());

            List<Map<String, Object>> events = reader.read("orders");
            checkEquals(3, events.size(), "event count");
            checkEquals(map("fullName", "alice", "age", 30L),
                    events.get(0), "v1 event upgraded to v2 only");
            checkEquals(map("fullName", "bob", "age", 41L),
                    events.get(1), "v2 event stays at v2");
            checkEquals(map("fullName", "carol", "years", 25L, "country", "US", "nickname", "c"),
                    events.get(2), "newer v3 event passes through untouched, new fields kept");
            ok("graph registered only to v2 reads the same file as v2 shape without errors");
        } finally {
            deleteRecursively(dir);
        }
    }

    /** A missing level must raise MissingUpcasterException naming the level. */
    private static void testMissingUpcasterThrows() throws Exception {
        File dir = newTempDir();
        try {
            EventStore store = new EventStore(dir.getAbsolutePath());
            store.append("orders", 1, map("name", "alice", "age", 30L));
            store.append("orders", 2, map("fullName", "bob", "age", 41L));

            // Only v2 -> v3 is registered; the v1 -> v2 level is missing.
            store.registerUpcaster(2, 3, upcasterV2toV3());

            try {
                store.read("orders");
                throw new AssertionError("expected MissingUpcasterException");
            } catch (MissingUpcasterException expected) {
                checkEquals(1, expected.getFromVersion(), "missing level fromVersion");
                checkEquals(2, expected.getToVersion(), "missing level toVersion");
                String message = String.valueOf(expected.getMessage());
                check(message.contains("1") && message.contains("2"),
                        "exception message must name the missing level, got: " + message);
            }
            ok("missing level throws MissingUpcasterException whose message names the versions");
        } finally {
            deleteRecursively(dir);
        }
    }

    /** Unknown fields must survive an upgrade plus a write-back round trip. */
    private static void testUnknownFieldsSurviveUpgradeAndWriteBack() throws Exception {
        File dir = newTempDir();
        try {
            Map<String, Object> unknown =
                    map("legacyCode", "X-9", "attrs", map("color", "red", "tags", list("a", 1L, true)));

            EventStore store = new EventStore(dir.getAbsolutePath());
            store.registerUpcaster(1, 2, upcasterV1toV2());
            store.registerUpcaster(2, 3, upcasterV2toV3());
            store.append("legacy", 1, map("name", "dave", "age", 50L, "unknownField", unknown));

            Map<String, Object> upgraded = store.read("legacy").get(0);
            checkEquals(unknown, upgraded.get("unknownField"),
                    "unknown field preserved through v1->v2->v3 upgrade");

            // Write the upgraded payload back as a new event, then read it with
            // a fresh store that has no upcasters at all.
            store.append("legacy-copy", 1, upgraded);
            EventStore fresh = new EventStore(dir.getAbsolutePath());
            Map<String, Object> reloaded = fresh.read("legacy-copy").get(0);
            checkEquals(unknown, reloaded.get("unknownField"),
                    "unknown field preserved after write-back round trip");
            checkEquals(upgraded, reloaded, "whole payload preserved after write-back round trip");
            ok("unknown fields survive a full upgrade plus write-back round trip");
        } finally {
            deleteRecursively(dir);
        }
    }

    /** A payload using all six value types must deep-equal the original. */
    private static void testDeepEqualityRoundTrip() throws Exception {
        File dir = newTempDir();
        try {
            Map<String, Object> payload = map(
                    "text", "line1\nline2:;{} \"quoted\" \u4e2d\u6587 \u2713",
                    "empty", "",
                    "longVal", Long.MAX_VALUE,
                    "negative", -42L,
                    "boolT", Boolean.TRUE,
                    "boolF", Boolean.FALSE,
                    "doubleVal", 3.141592653589793,
                    "negZero", -0.0,
                    "list", list(1L, "two", Boolean.FALSE, list(3.5, "x")),
                    "nested", map("a", list(map("b", "c")), "d", 2.5));

            EventStore store = new EventStore(dir.getAbsolutePath());
            store.append("all-types", 1, payload);

            List<Map<String, Object>> events = store.read("all-types");
            checkEquals(1, events.size(), "event count");
            checkEquals(payload, events.get(0), "payload deep-equals original after write + read");
            ok("payload with all six value types deep-equals the original after a round trip");
        } finally {
            deleteRecursively(dir);
        }
    }

    /** Versions per stream must start at 1 and increase without gaps. */
    private static void testSequentialVersionsEnforced() throws Exception {
        File dir = newTempDir();
        try {
            EventStore store = new EventStore(dir.getAbsolutePath());
            store.append("s", 1, map("a", 1L));
            expectIllegalState(store, "s", 3); // gap: 2 is missing
            expectIllegalState(store, "s", 1); // duplicate
            store.append("s", 2, map("a", 2L)); // the next sequential version is accepted
            checkEquals(2, store.read("s").size(), "two events stored");
            ok("versions must start at 1 and increase sequentially without gaps");
        } finally {
            deleteRecursively(dir);
        }
    }

    /** Values outside the six supported types must be rejected on append. */
    private static void testPayloadTypeValidation() throws Exception {
        File dir = newTempDir();
        try {
            EventStore store = new EventStore(dir.getAbsolutePath());
            try {
                store.append("bad", 1, map("n", Integer.valueOf(7)));
                throw new AssertionError("expected IllegalArgumentException for Integer payload");
            } catch (IllegalArgumentException expected) {
                // Integer is not one of the six supported types; use Long instead
            }
            try {
                store.append("bad", 1, map("x", new Object()));
                throw new AssertionError("expected IllegalArgumentException for Object payload");
            } catch (IllegalArgumentException expected) {
                // expected
            }
            ok("payloads with unsupported value types are rejected");
        } finally {
            deleteRecursively(dir);
        }
    }

    // ------------------------------------------------------------------
    // Upcasters used by the tests. They follow the copy-then-modify style:
    // known keys are transformed, everything unknown is carried over as-is.
    // ------------------------------------------------------------------

    /** v1 -> v2: rename "name" to "fullName". */
    private static Function<Map<String, Object>, Map<String, Object>> upcasterV1toV2() {
        return new Function<Map<String, Object>, Map<String, Object>>() {
            public Map<String, Object> apply(Map<String, Object> event) {
                Map<String, Object> out = new LinkedHashMap<String, Object>(event);
                if (out.containsKey("name")) {
                    out.put("fullName", out.remove("name"));
                }
                return out;
            }
        };
    }

    /** v2 -> v3: rename "age" to "years", default "country" to "CN". */
    private static Function<Map<String, Object>, Map<String, Object>> upcasterV2toV3() {
        return new Function<Map<String, Object>, Map<String, Object>>() {
            public Map<String, Object> apply(Map<String, Object> event) {
                Map<String, Object> out = new LinkedHashMap<String, Object>(event);
                if (out.containsKey("age")) {
                    out.put("years", out.remove("age"));
                }
                if (!out.containsKey("country")) {
                    out.put("country", "CN");
                }
                return out;
            }
        };
    }

    // ------------------------------------------------------------------
    // Small test helpers
    // ------------------------------------------------------------------

    private static Map<String, Object> map(Object... keysAndValues) {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return map;
    }

    private static List<Object> list(Object... values) {
        return new ArrayList<Object>(Arrays.asList(values));
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static void checkEquals(Object expected, Object actual, String message) {
        if (expected == null ? actual != null : !expected.equals(actual)) {
            throw new AssertionError(message + " -- expected: " + expected + ", actual: " + actual);
        }
    }

    private static void expectIllegalState(EventStore store, String streamId, int version) {
        try {
            store.append(streamId, version, map("a", 1L));
            throw new AssertionError("expected IllegalStateException for version " + version);
        } catch (IllegalStateException expected) {
            // expected
        }
    }

    private static void ok(String name) {
        passed++;
        System.out.println("ok " + passed + " - " + name);
    }

    private static File newTempDir() throws Exception {
        return Files.createTempDirectory("eventstore-test").toFile();
    }

    private static void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }
}
