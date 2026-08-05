package dev.corium.client;

import java.lang.ref.Cleaner;

/**
 * Frees native peer and database handles once their Java owners are
 * unreachable. Closing a peer stops its work deterministically; this only
 * releases the handle memory afterwards.
 */
final class NativeHandles {
    private static final Cleaner CLEANER = Cleaner.create();

    private NativeHandles() {}

    static void releasePeerWith(Object owner, long handle) {
        CLEANER.register(owner, new PeerRelease(handle));
    }

    static void releaseDbWith(Object owner, long handle) {
        CLEANER.register(owner, new DbRelease(handle));
    }

    /** Holds only the raw handle, never the owner, so the owner stays collectable. */
    private static final class PeerRelease implements Runnable {
        private final long handle;

        PeerRelease(long handle) {
            this.handle = handle;
        }

        @Override
        public void run() {
            Native.freePeer(handle);
        }
    }

    private static final class DbRelease implements Runnable {
        private final long handle;

        DbRelease(long handle) {
            this.handle = handle;
        }

        @Override
        public void run() {
            Native.freeDb(handle);
        }
    }
}
