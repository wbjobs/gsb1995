package com.gsb.eventstore;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Append-only, file-backed event store with schema evolution support.
 *
 * Storage layout: one file per stream named {@code <streamId>.events} inside
 * the directory given to the constructor. Each line is one event:
 * {@code <version> <encoded payload>}. Lines are only ever appended, never
 * modified or deleted, so stored events are immutable.
 *
 * Version model: within one stream, versions start at 1 and increase by one
 * per event with no gaps; {@link #append} enforces this. An event's stored
 * version is also its schema version.
 *
 * Evolution: upcasters are registered per level (fromVersion -> fromVersion+1).
 * On {@link #read}, every event whose stored version is below the highest
 * registered version is upgraded in memory, one level at a time; a missing
 * level raises {@link MissingUpcasterException}. Events stored at or above the
 * target version are returned exactly as stored, so newer fields are never
 * lost when an older upcaster graph reads the file. The file on disk is never
 * rewritten by upgrades.
 */
public class EventStore {

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private final File directory;
    /** Keyed by fromVersion; every registered upcaster upgrades exactly one level. */
    private final Map<Integer, Function<Map<String, Object>, Map<String, Object>>> upcasters =
            new HashMap<Integer, Function<Map<String, Object>, Map<String, Object>>>();

    public EventStore(String directory) {
        if (directory == null) {
            throw new NullPointerException("directory");
        }
        this.directory = new File(directory);
        if (!this.directory.isDirectory() && !this.directory.mkdirs()) {
            throw new IllegalStateException("Cannot create event store directory: " + directory);
        }
    }

    /**
     * Registers an upcaster that upgrades a payload exactly one level, from
     * {@code fromVersion} to {@code toVersion}. {@code toVersion} must equal
     * {@code fromVersion + 1}: upgrades always happen level by level.
     */
    public synchronized void registerUpcaster(int fromVersion, int toVersion,
            Function<Map<String, Object>, Map<String, Object>> upcaster) {
        if (upcaster == null) {
            throw new NullPointerException("upcaster");
        }
        if (fromVersion < 1) {
            throw new IllegalArgumentException("fromVersion must be >= 1, got " + fromVersion);
        }
        if (toVersion != fromVersion + 1) {
            throw new IllegalArgumentException(
                    "Upcasters must upgrade exactly one level (toVersion = fromVersion + 1), got "
                            + fromVersion + " -> " + toVersion);
        }
        upcasters.put(Integer.valueOf(fromVersion), upcaster);
    }

    /**
     * Appends an event to a stream. {@code version} must be the next
     * sequential version of the stream (1 for the first event, then 2, 3, ...
     * with no gaps). The payload may only contain String, Long, Boolean,
     * Double, List and Map values (recursively).
     */
    public synchronized void append(String streamId, int version, Map<String, Object> payload) {
        validateStreamId(streamId);
        validateValue(payload, "payload");
        File file = fileFor(streamId);
        int expected = lastVersion(file) + 1;
        if (version != expected) {
            throw new IllegalStateException("Non-sequential version for stream '" + streamId
                    + "': expected " + expected + " but got " + version);
        }
        appendLine(file, version + " " + Codec.encodeMap(payload));
    }

    /**
     * Reads all events of a stream, upgrading each one in memory from its
     * stored version to the highest registered version, one level at a time.
     * Events stored at or above the target version are returned unchanged.
     * The stored file is never modified.
     */
    public synchronized List<Map<String, Object>> read(String streamId) {
        validateStreamId(streamId);
        List<Map<String, Object>> events = new ArrayList<Map<String, Object>>();
        File file = fileFor(streamId);
        if (!file.exists()) {
            return events;
        }
        int targetVersion = highestRegisteredVersion();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.length() == 0) {
                    continue;
                }
                int space = line.indexOf(' ');
                if (space <= 0) {
                    throw corrupt(file, line);
                }
                int storedVersion;
                try {
                    storedVersion = Integer.parseInt(line.substring(0, space));
                } catch (NumberFormatException e) {
                    throw corrupt(file, line);
                }
                Map<String, Object> payload = Codec.decodeMap(line.substring(space + 1));
                events.add(upcast(payload, storedVersion, targetVersion));
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read stream '" + streamId + "'", e);
        } finally {
            closeQuietly(reader);
        }
        return events;
    }

    private Map<String, Object> upcast(Map<String, Object> payload, int storedVersion,
            int targetVersion) {
        Map<String, Object> current = payload;
        int version = storedVersion;
        while (version < targetVersion) {
            Function<Map<String, Object>, Map<String, Object>> upcaster =
                    upcasters.get(Integer.valueOf(version));
            if (upcaster == null) {
                throw new MissingUpcasterException(version, version + 1);
            }
            Map<String, Object> next = upcaster.apply(current);
            if (next == null) {
                throw new IllegalStateException(
                        "Upcaster " + version + " -> " + (version + 1) + " returned null");
            }
            current = next;
            version++;
        }
        return current;
    }

    /** Highest version reachable through the registered upcasters, 0 if none. */
    private int highestRegisteredVersion() {
        int max = 0;
        for (Integer fromVersion : upcasters.keySet()) {
            if (fromVersion.intValue() + 1 > max) {
                max = fromVersion.intValue() + 1;
            }
        }
        return max;
    }

    private File fileFor(String streamId) {
        return new File(directory, streamId + ".events");
    }

    private static void validateStreamId(String streamId) {
        if (streamId == null || streamId.isEmpty()) {
            throw new IllegalArgumentException("streamId must not be null or empty");
        }
        if (streamId.equals(".") || streamId.equals("..") || !streamId.matches("[A-Za-z0-9._-]+")) {
            throw new IllegalArgumentException("Illegal streamId '" + streamId
                    + "': only letters, digits, '.', '_' and '-' are allowed");
        }
    }

    private static void validateValue(Object value, String path) {
        if (value == null) {
            throw new IllegalArgumentException("Null value not allowed at " + path);
        }
        if (value instanceof String || value instanceof Long
                || value instanceof Boolean || value instanceof Double) {
            return;
        }
        if (value instanceof List) {
            int index = 0;
            for (Object element : (List<?>) value) {
                validateValue(element, path + "[" + index + "]");
                index++;
            }
            return;
        }
        if (value instanceof Map) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw new IllegalArgumentException("Map key must be a String at " + path
                            + ", got " + entry.getKey());
                }
                validateValue(entry.getValue(), path + "." + entry.getKey());
            }
            return;
        }
        throw new IllegalArgumentException("Unsupported value type at " + path + ": "
                + value.getClass().getName()
                + " (only String, Long, Boolean, Double, List and Map are allowed)");
    }

    private static int lastVersion(File file) {
        if (!file.exists()) {
            return 0;
        }
        int count = 0;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), UTF_8));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.length() > 0) {
                    count++;
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + file, e);
        } finally {
            closeQuietly(reader);
        }
        return count;
    }

    private static void appendLine(File file, String line) {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(file, true), UTF_8));
            writer.write(line);
            writer.write('\n');
        } catch (IOException e) {
            throw new IllegalStateException("Failed to append to " + file, e);
        } finally {
            closeQuietly(writer);
        }
    }

    private static IllegalStateException corrupt(File file, String line) {
        return new IllegalStateException("Corrupt event file " + file + ": bad line: " + line);
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException ignored) {
                // nothing sensible to do while closing
            }
        }
    }
}
