package com.gsb.eventstore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Dependency-free test runner. Exits with status 0 and prints a summary when
 * every check passes; throws (non-zero exit) on the first failure.
 */
public final class EventStoreTest {

    private static int checks = 0;

    public static void main(String[] args) throws Exception {
        testStepwiseUpgradeV1ToV3();
        testPartialGraphReadsV2Shape();
        testNewerSchemaPassesThroughWithFieldsKept();
        testMissingUpcasterThrowsWithVersions();
        testUnknownFieldsSurviveUpgradeAndRewrite();
        testDeepEqualsRoundTrip();
        testContiguousVersionsEnforced();
        testPayloadTypeValidation();
        testReturnedPayloadsAreIndependentCopies();
        System.out.println();
        System.out.println("ALL TESTS PASSED (" + checks + " checks)");
    }

    /** v1 events, full graph v1->v2->v3 registered afterwards: read yields v3 shape. */
    static void testStepwiseUpgradeV1ToV3() throws Exception {
        Path dir = freshDir("stepwise");
        FileEventStore store = new FileEventStore(dir);
        Map<String, Object> v1 = map(
                "orderId", "A-1",
                "amount", Long.valueOf(100L));
        store.append("orders", 1, v1);

        registerV1ToV2(store);
        registerV2ToV3(store);

        List<Map<String, Object>> events = store.read("orders");
        check(events.size() == 1, "stepwise: one event");
        Map<String, Object> p = events.get(0);
        check("A-1".equals(p.get("orderId")), "stepwise: orderId carried");
        check("CNY".equals(p.get("currency")), "stepwise: currency added by v1->v2");
        check("vip".equals(p.get("channel")), "stepwise: channel added by v2->v3");
        check(Long.valueOf(10000L).equals(p.get("amountCents")),
                "stepwise: amountCents derived by v2->v3");
        check(!p.containsKey("amount"), "stepwise: amount renamed away by v2->v3");
    }

    /** Same file, graph registered only up to v2: read yields v2 shape, no error. */
    static void testPartialGraphReadsV2Shape() throws Exception {
        Path dir = freshDir("partial");
        FileEventStore writer = new FileEventStore(dir);
        writer.append("orders", 1, map("orderId", "A-1", "amount", Long.valueOf(100L)));

        FileEventStore reader = new FileEventStore(dir);
        registerV1ToV2(reader);

        List<Map<String, Object>> events = reader.read("orders");
        check(events.size() == 1, "partial: one event");
        Map<String, Object> p = events.get(0);
        check("CNY".equals(p.get("currency")), "partial: v1->v2 applied");
        check(!p.containsKey("channel") && !p.containsKey("amountCents"),
                "partial: v3 fields absent");
        check(Long.valueOf(100L).equals(p.get("amount")), "partial: amount untouched");
    }

    /** File written at schema v3, reader knows only up to v2: pass-through, fields kept. */
    static void testNewerSchemaPassesThroughWithFieldsKept() throws Exception {
        Path dir = freshDir("forward");
        FileEventStore writer = new FileEventStore(dir);
        registerV1ToV2(writer);
        registerV2ToV3(writer);
        Map<String, Object> v3 = map(
                "orderId", "B-9",
                "currency", "CNY",
                "channel", "vip",
                "amountCents", Long.valueOf(500L),
                "brandNewField", "from-the-future");
        writer.append("orders", 1, v3);

        FileEventStore reader = new FileEventStore(dir);
        registerV1ToV2(reader);

        List<Map<String, Object>> events = reader.read("orders");
        check(events.size() == 1, "forward: one event");
        check("from-the-future".equals(events.get(0).get("brandNewField")),
                "forward: newer field preserved");
        check("vip".equals(events.get(0).get("channel")), "forward: v3 field preserved");
    }

    /** Missing single step must throw MissingUpcasterException naming the versions. */
    static void testMissingUpcasterThrowsWithVersions() throws Exception {
        Path dir = freshDir("missing");
        FileEventStore writer = new FileEventStore(dir);
        writer.append("orders", 1, map("orderId", "A-1"));

        FileEventStore reader = new FileEventStore(dir);
        registerV2ToV3(reader); // v1 -> v2 deliberately missing

        try {
            reader.read("orders");
            check(false, "missing: should have thrown");
        } catch (MissingUpcasterException e) {
            check(e.getMessage().contains("v1") && e.getMessage().contains("v2"),
                    "missing: message names versions, got: " + e.getMessage());
            check(e.getFromVersion() == 1 && e.getToVersion() == 2,
                    "missing: exception carries from/to");
        }
    }

    /** Unknown fields survive a full upgrade chain and a subsequent re-write. */
    static void testUnknownFieldsSurviveUpgradeAndRewrite() throws Exception {
        Path dir = freshDir("unknown");
        FileEventStore store = new FileEventStore(dir);
        Map<String, Object> meta = map("note", "do-not-drop", "flags",
                (Object) Arrays.asList("a", "b"));
        Map<String, Object> v1 = map(
                "orderId", "A-1",
                "amount", Long.valueOf(7L),
                "legacyField", "keep-me",
                "meta", meta);
        store.append("orders", 1, v1);

        registerV1ToV2(store);
        registerV2ToV3(store);

        Map<String, Object> upgraded = store.read("orders").get(0);
        check("keep-me".equals(upgraded.get("legacyField")), "unknown: scalar kept");
        check(deepEquals(meta, upgraded.get("meta")), "unknown: nested kept");

        // Write the upgraded payload back as a new event, read again: still intact.
        store.append("orders", 2, upgraded);
        Map<String, Object> reread = store.read("orders").get(1);
        check("keep-me".equals(reread.get("legacyField")), "unknown: scalar kept after rewrite");
        check(deepEquals(meta, reread.get("meta")), "unknown: nested kept after rewrite");
        check(deepEquals(upgraded, reread), "unknown: full payload identical after rewrite");
    }

    /** Every allowed type (incl. nasty strings) round-trips deep-equal. */
    static void testDeepEqualsRoundTrip() throws Exception {
        Path dir = freshDir("roundtrip");
        FileEventStore store = new FileEventStore(dir);
        Map<String, Object> nested = map(
                "empty", "",
                "tricky", "semi;colon Z M A newline\ntab\there 中文 emoji ",
                "boolT", Boolean.TRUE,
                "boolF", Boolean.FALSE,
                "longMin", Long.valueOf(Long.MIN_VALUE),
                "longMax", Long.valueOf(Long.MAX_VALUE),
                "pi", Double.valueOf(3.141592653589793),
                "negZero", Double.valueOf(-0.0d),
                "nan", Double.valueOf(Double.NaN),
                "inf", Double.valueOf(Double.POSITIVE_INFINITY));
        Map<String, Object> payload = map(
                "name", "round-trip",
                "count", Long.valueOf(42L),
                "list", Arrays.asList(
                        "x",
                        Long.valueOf(-1L),
                        Arrays.asList(Boolean.TRUE, map("k", "v")),
                        new ArrayList<Object>()),
                "nested", nested,
                "emptyMap", new LinkedHashMap<String, Object>());
        store.append("s", 1, payload);
        store.append("s", 2, payload);

        List<Map<String, Object>> events = store.read("s");
        check(events.size() == 2, "roundtrip: two events");
        check(deepEquals(payload, events.get(0)), "roundtrip: event 1 deep-equal");
        check(deepEquals(payload, events.get(1)), "roundtrip: event 2 deep-equal");
    }

    /** Versions must start at 1 and be contiguous per stream. */
    static void testContiguousVersionsEnforced() throws Exception {
        Path dir = freshDir("versions");
        FileEventStore store = new FileEventStore(dir);
        expectThrows(IllegalStateException.class, new Runnable() {
            public void run() {
                store.append("s", 0, map("a", "b")); // version 0
            }
        }, "versions: 0 rejected");
        expectThrows(IllegalStateException.class, new Runnable() {
            public void run() {
                store.append("s", 2, map("a", "b")); // gap: first event must be 1
            }
        }, "versions: gap rejected");
        store.append("s", 1, map("a", "b"));
        expectThrows(IllegalStateException.class, new Runnable() {
            public void run() {
                store.append("s", 3, map("a", "b")); // hole at 2
            }
        }, "versions: hole rejected");
        expectThrows(IllegalStateException.class, new Runnable() {
            public void run() {
                store.append("s", 1, map("a", "b")); // duplicate
            }
        }, "versions: duplicate rejected");
        store.append("s", 2, map("a", "b"));
        check(store.read("s").size() == 2, "versions: two events stored");
    }

    /** Only the six allowed value types are accepted, recursively. */
    static void testPayloadTypeValidation() throws Exception {
        Path dir = freshDir("types");
        final FileEventStore store = new FileEventStore(dir);
        expectThrows(IllegalArgumentException.class, new Runnable() {
            public void run() {
                store.append("s", 1, map("bad", Integer.valueOf(1)));
            }
        }, "types: Integer rejected");
        expectThrows(IllegalArgumentException.class, new Runnable() {
            public void run() {
                store.append("s", 1, map("bad", Arrays.asList("ok", Integer.valueOf(2))));
            }
        }, "types: nested Integer rejected");
        expectThrows(IllegalArgumentException.class, new Runnable() {
            public void run() {
                Map<Object, Object> bad = new LinkedHashMap<Object, Object>();
                bad.put(Integer.valueOf(1), "x");
                @SuppressWarnings({"unchecked", "rawtypes"})
                Map<String, Object> cast = (Map) bad;
                store.append("s", 1, cast);
            }
        }, "types: non-String key rejected");
        expectThrows(IllegalArgumentException.class, new Runnable() {
            public void run() {
                Map<String, Object> m = map("a", "b");
                m.put("nul", null);
                store.append("s", 1, m);
            }
        }, "types: null rejected");
        store.append("s", 1, map("ok", "fine"));
        check(store.read("s").size() == 1, "types: valid payload accepted");
    }

    /** Mutating a returned payload must not affect what later reads return. */
    static void testReturnedPayloadsAreIndependentCopies() throws Exception {
        Path dir = freshDir("immutable");
        FileEventStore store = new FileEventStore(dir);
        Map<String, Object> payload = map("k", "v", "n", Long.valueOf(1L));
        store.append("s", 1, payload);

        Map<String, Object> first = store.read("s").get(0);
        first.put("k", "tampered");
        first.put("injected", "x");

        Map<String, Object> second = store.read("s").get(0);
        check(deepEquals(payload, second), "immutable: stored event unaffected by mutation");
    }

    // ------------------------------------------------------------------
    // Upcasters used by several tests. Both copy every existing entry first,
    // so unknown fields are carried through.
    // ------------------------------------------------------------------

    /** v1 -> v2: adds "currency". */
    static void registerV1ToV2(EventStore store) {
        store.registerUpcaster(1, 2, new Function<Map<String, Object>, Map<String, Object>>() {
            @Override
            public Map<String, Object> apply(Map<String, Object> in) {
                Map<String, Object> out = new LinkedHashMap<String, Object>(in);
                out.put("currency", "CNY");
                return out;
            }
        });
    }

    /** v2 -> v3: renames amount -> amountCents (x100), adds "channel". */
    static void registerV2ToV3(EventStore store) {
        store.registerUpcaster(2, 3, new Function<Map<String, Object>, Map<String, Object>>() {
            @Override
            public Map<String, Object> apply(Map<String, Object> in) {
                Map<String, Object> out = new LinkedHashMap<String, Object>(in);
                Object amount = out.remove("amount");
                if (amount instanceof Long) {
                    out.put("amountCents", Long.valueOf(((Long) amount).longValue() * 100L));
                }
                out.put("channel", "vip");
                return out;
            }
        });
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    static void check(boolean condition, String name) {
        checks++;
        if (!condition) {
            throw new AssertionError("FAILED: " + name);
        }
        System.out.println("ok " + checks + " - " + name);
    }

    static void expectThrows(Class<? extends Throwable> type, Runnable body, String name) {
        checks++;
        try {
            body.run();
        } catch (Throwable t) {
            if (type.isInstance(t)) {
                System.out.println("ok " + checks + " - " + name);
                return;
            }
            throw new AssertionError("FAILED: " + name + " - expected " + type.getSimpleName()
                    + " but got " + t, t);
        }
        throw new AssertionError("FAILED: " + name + " - expected " + type.getSimpleName()
                + " but nothing was thrown");
    }

    static boolean deepEquals(Object a, Object b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof Map && b instanceof Map) {
            Map<?, ?> ma = (Map<?, ?>) a;
            Map<?, ?> mb = (Map<?, ?>) b;
            if (!ma.keySet().equals(mb.keySet())) {
                return false;
            }
            for (Object key : ma.keySet()) {
                if (!deepEquals(ma.get(key), mb.get(key))) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof List && b instanceof List) {
            List<?> la = (List<?>) a;
            List<?> lb = (List<?>) b;
            if (la.size() != lb.size()) {
                return false;
            }
            for (int i = 0; i < la.size(); i++) {
                if (!deepEquals(la.get(i), lb.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof Double && b instanceof Double) {
            return Double.doubleToRawLongBits(((Double) a).doubleValue())
                    == Double.doubleToRawLongBits(((Double) b).doubleValue());
        }
        return a.equals(b);
    }

    static Map<String, Object> map(Object... keysAndValues) {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            map.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return map;
    }

    static Path freshDir(String name) throws IOException {
        Path dir = Paths.get(System.getProperty("java.io.tmpdir"),
                "eventstore-test-" + name + "-" + System.nanoTime());
        deleteRecursively(dir);
        Files.createDirectories(dir);
        return dir;
    }

    static void deleteRecursively(Path dir) throws IOException {
        if (!Files.exists(dir)) {
            return;
        }
        Files.walkFileTree(dir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                try {
                    Files.delete(file);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path d, IOException e) {
                try {
                    Files.delete(d);
                } catch (IOException ioe) {
                    throw new UncheckedIOException(ioe);
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
