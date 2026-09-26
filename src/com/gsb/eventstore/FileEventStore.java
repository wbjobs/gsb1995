package com.gsb.eventstore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * File-backed {@link EventStore}. One append-only log file per stream inside a
 * base directory; each line is one event:
 *
 * <pre>E&lt;eventVersion&gt;;&lt;schemaVersion&gt;;&lt;encodedPayload&gt;\n</pre>
 *
 * Written events are never modified or rewritten; upgrades happen in memory at
 * read time only.
 */
public final class FileEventStore implements EventStore {

    private static final Pattern STREAM_ID = Pattern.compile("[A-Za-z0-9._-]+");

    private final Path directory;
    private final Map<Integer, Function<Map<String, Object>, Map<String, Object>>> upcasters =
            new HashMap<Integer, Function<Map<String, Object>, Map<String, Object>>>();

    public FileEventStore(Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create event store directory " + directory, e);
        }
    }

    @Override
    public synchronized void registerUpcaster(int fromVersion, int toVersion,
                                              Function<Map<String, Object>, Map<String, Object>> upcaster) {
        if (fromVersion < 1) {
            throw new IllegalArgumentException("fromVersion must be >= 1, got " + fromVersion);
        }
        if (toVersion != fromVersion + 1) {
            throw new IllegalArgumentException("upcasters must be single-step: v" + fromVersion
                    + " -> v" + toVersion + " is not allowed");
        }
        if (upcaster == null) {
            throw new IllegalArgumentException("upcaster must not be null");
        }
        if (upcasters.containsKey(Integer.valueOf(fromVersion))) {
            throw new IllegalStateException("upcaster v" + fromVersion + " -> v" + toVersion
                    + " is already registered");
        }
        upcasters.put(Integer.valueOf(fromVersion), upcaster);
    }

    @Override
    public synchronized void append(String streamId, int version, Map<String, Object> payload) {
        checkStreamId(streamId);
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        validateValue(payload, "payload");
        Path file = streamFile(streamId);
        int nextVersion = countEvents(file) + 1;
        if (version != nextVersion) {
            throw new IllegalStateException("non-contiguous event version for stream '" + streamId
                    + "': expected " + nextVersion + " but got " + version);
        }
        String line = "E" + version + ";" + currentSchemaVersion() + ";"
                + PayloadCodec.encodeMap(payload) + "\n";
        try {
            Files.write(file, line.getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot append to " + file, e);
        }
    }

    @Override
    public synchronized List<Map<String, Object>> read(String streamId) {
        checkStreamId(streamId);
        Path file = streamFile(streamId);
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        if (!Files.exists(file)) {
            return result;
        }
        List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + file, e);
        }
        int targetVersion = highestRegisteredVersion();
        int expectedVersion = 1;
        for (String line : lines) {
            if (line.isEmpty()) {
                continue;
            }
            StoredEvent event = parseLine(line);
            if (event.eventVersion != expectedVersion) {
                throw new IllegalStateException("gap in stream '" + streamId + "': expected event #"
                        + expectedVersion + " but found #" + event.eventVersion);
            }
            expectedVersion++;
            Map<String, Object> payload = event.payload;
            int schemaVersion = event.schemaVersion;
            while (schemaVersion < targetVersion) {
                Function<Map<String, Object>, Map<String, Object>> upcaster =
                        upcasters.get(Integer.valueOf(schemaVersion));
                if (upcaster == null) {
                    throw new MissingUpcasterException(streamId, event.eventVersion,
                            schemaVersion, schemaVersion + 1);
                }
                payload = upcaster.apply(payload);
                if (payload == null) {
                    throw new IllegalStateException("upcaster v" + schemaVersion + " -> v"
                            + (schemaVersion + 1) + " returned null (stream '" + streamId
                            + "', event #" + event.eventVersion + ")");
                }
                schemaVersion++;
            }
            // schemaVersion > targetVersion: stored by a newer writer; pass through
            // unchanged so newer fields survive a reader with an older upcaster graph.
            result.add(payload);
        }
        return result;
    }

    /**
     * Schema version stamped onto newly appended events: the highest version
     * reachable from v1 through an unbroken registered chain. With a broken
     * chain (e.g. only v2 -> v3 registered) new events stay at v1, and the
     * missing step surfaces as MissingUpcasterException on read.
     */
    private int currentSchemaVersion() {
        int version = 1;
        while (upcasters.containsKey(Integer.valueOf(version))) {
            version++;
        }
        return version;
    }

    /**
     * Target schema version for reads: the highest registered to-version.
     * Reads upgrade every stored event step by step up to this version; a gap
     * in the chain therefore fails loudly instead of silently returning a
     * half-upgraded payload.
     */
    private int highestRegisteredVersion() {
        int version = 1;
        for (Integer from : upcasters.keySet()) {
            if (from.intValue() + 1 > version) {
                version = from.intValue() + 1;
            }
        }
        return version;
    }

    private int countEvents(Path file) {
        if (!Files.exists(file)) {
            return 0;
        }
        try {
            int count = 0;
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (!line.isEmpty()) {
                    count++;
                }
            }
            return count;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read " + file, e);
        }
    }

    private Path streamFile(String streamId) {
        return directory.resolve(streamId + ".events");
    }

    private static void checkStreamId(String streamId) {
        if (streamId == null || !STREAM_ID.matcher(streamId).matches()
                || streamId.equals(".") || streamId.equals("..")) {
            throw new IllegalArgumentException("illegal stream id: " + streamId);
        }
    }

    private static StoredEvent parseLine(String line) {
        if (!line.startsWith("E")) {
            throw new IllegalStateException("corrupt event line: " + abbreviate(line));
        }
        int first = line.indexOf(';');
        int second = first < 0 ? -1 : line.indexOf(';', first + 1);
        if (first < 0 || second < 0) {
            throw new IllegalStateException("corrupt event line: " + abbreviate(line));
        }
        final int eventVersion;
        final int schemaVersion;
        try {
            eventVersion = Integer.parseInt(line.substring(1, first));
            schemaVersion = Integer.parseInt(line.substring(first + 1, second));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("corrupt event line: " + abbreviate(line));
        }
        return new StoredEvent(eventVersion, schemaVersion,
                PayloadCodec.decodeMap(line.substring(second + 1)));
    }

    private static String abbreviate(String line) {
        return line.length() <= 40 ? line : line.substring(0, 40) + "...";
    }

    private static void validateValue(Object value, String path) {
        if (value == null) {
            throw new IllegalArgumentException("null is not an allowed payload value at " + path);
        }
        if (value instanceof String || value instanceof Long
                || value instanceof Boolean || value instanceof Double) {
            return;
        }
        if (value instanceof List) {
            int index = 0;
            for (Object item : (List<?>) value) {
                validateValue(item, path + "[" + index + "]");
                index++;
            }
            return;
        }
        if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw new IllegalArgumentException("map keys must be String at " + path
                            + ", got " + entry.getKey().getClass().getName());
                }
                validateValue(entry.getValue(), path + "." + entry.getKey());
            }
            return;
        }
        throw new IllegalArgumentException("unsupported payload value type "
                + value.getClass().getName() + " at " + path
                + " (allowed: String, Long, Boolean, Double, List, Map)");
    }

    private static final class StoredEvent {
        final int eventVersion;
        final int schemaVersion;
        final Map<String, Object> payload;

        StoredEvent(int eventVersion, int schemaVersion, Map<String, Object> payload) {
            this.eventVersion = eventVersion;
            this.schemaVersion = schemaVersion;
            this.payload = payload;
        }
    }
}
