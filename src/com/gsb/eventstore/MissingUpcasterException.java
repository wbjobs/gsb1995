package com.gsb.eventstore;

/**
 * Thrown by {@link EventStore#read(String)} when a stored event must be
 * upgraded through a version level for which no upcaster is registered.
 * The exception message always names the missing level, e.g.
 * "Missing upcaster from version 1 to version 2".
 */
public class MissingUpcasterException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final int fromVersion;
    private final int toVersion;

    public MissingUpcasterException(int fromVersion, int toVersion) {
        super("Missing upcaster from version " + fromVersion + " to version " + toVersion);
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
