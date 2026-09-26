package com.gsb.eventstore;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * A long-lived, file-backed event store with schema evolution support.
 *
 * <p>Two independent version concepts exist:</p>
 * <ul>
 *   <li><b>Event version</b> – the per-stream sequence number, starting at 1 and
 *       strictly contiguous (no gaps, no duplicates).</li>
 *   <li><b>Schema version</b> – the version of the payload shape. New events are
 *       written at the highest schema version reachable through the registered
 *       upcaster chain. On read, stored events are upgraded step by step
 *       (v1 -&gt; v2 -&gt; v3, never skipping a level) up to the highest
 *       registered version.</li>
 * </ul>
 */
public interface EventStore {

    /**
     * Appends an event to a stream.
     *
     * @param streamId stream identifier; must match {@code [A-Za-z0-9._-]+}
     * @param version  expected event version; must equal (number of events already
     *                 in the stream) + 1, i.e. versions start at 1 and are contiguous
     * @param payload  event payload; values may only be String, Long, Boolean,
     *                 Double, List or Map (recursively), map keys must be String
     * @throws IllegalStateException    if {@code version} is not the next contiguous version
     * @throws IllegalArgumentException if the payload contains an unsupported value type
     */
    void append(String streamId, int version, Map<String, Object> payload);

    /**
     * Reads all events of a stream in order, upgrading each payload step by step
     * to the highest registered schema version. Events stored at a schema version
     * newer than anything registered are returned as-is (forward compatibility).
     *
     * @throws MissingUpcasterException if a required single-step upcaster is not registered
     */
    List<Map<String, Object>> read(String streamId);

    /**
     * Registers a single-step upcaster. {@code toVersion} must equal
     * {@code fromVersion + 1}; upgrades are always applied one level at a time.
     *
     * <p>Contract: the upcaster receives the full payload (including fields it
     * does not know) and must return a map that carries every unknown field
     * through unchanged. The store never strips fields itself.</p>
     */
    void registerUpcaster(int fromVersion, int toVersion,
                          Function<Map<String, Object>, Map<String, Object>> upcaster);
}
