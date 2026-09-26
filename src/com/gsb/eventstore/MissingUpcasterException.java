package com.gsb.eventstore;

/**
 * Thrown by {@link EventStore#read} when a stored event must be upgraded but the
 * upcaster for one required single step (e.g. v1 -&gt; v2) is not registered.
 * The exception message always names the missing step, including both version
 * numbers.
 */
public class MissingUpcasterException extends RuntimeException {

    private final int fromVersion;
    private final int toVersion;

    public MissingUpcasterException(String streamId, int eventVersion, int fromVersion, int toVersion) {
        super("Missing upcaster: v" + fromVersion + " -> v" + toVersion
                + " (stream '" + streamId + "', event #" + eventVersion + ")");
        this.fromVersion = fromVersion;
        this.toVersion = toVersion;
    }

    public int getFromVersion() {
        return fromVersion;
    }

    public int getToVersion() {
        return toVersion;
    }
}
