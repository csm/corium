package dev.corium.client;

import java.nio.file.Path;
import java.util.Objects;

/** A bounded local cache for immutable direct-storage segments. */
public final class SegmentCache {
    private final Path directory;
    private final long capacityBytes;
    private final long memoryCapacityBytes;

    /** Uses the engine default for the in-process tier. */
    public SegmentCache(Path directory, long capacityBytes) {
        this(directory, capacityBytes, 0);
    }

    /**
     * @param directory a directory owned by this process alone
     * @param capacityBytes maximum accounted bytes in the disk tier
     * @param memoryCapacityBytes maximum accounted bytes in the in-process
     *     tier, or {@code 0} for the engine default
     */
    public SegmentCache(Path directory, long capacityBytes, long memoryCapacityBytes) {
        this.directory = Objects.requireNonNull(directory, "directory");
        if (capacityBytes <= 0) {
            throw new IllegalArgumentException("capacityBytes must be positive");
        }
        if (memoryCapacityBytes < 0) {
            throw new IllegalArgumentException("memoryCapacityBytes must be non-negative");
        }
        if (memoryCapacityBytes > capacityBytes) {
            throw new IllegalArgumentException("memoryCapacityBytes must not exceed capacityBytes");
        }
        this.capacityBytes = capacityBytes;
        this.memoryCapacityBytes = memoryCapacityBytes;
    }

    public Path directory() { return directory; }
    public long capacityBytes() { return capacityBytes; }
    public long memoryCapacityBytes() { return memoryCapacityBytes; }
}
