package io.corium.client;

/** Coarse statistics for one immutable database view. */
public final class DbStats {
    private final long basisT;
    private final long datoms;
    private final long entities;
    private final long attributes;

    public DbStats(long basisT, long datoms, long entities, long attributes) {
        this.basisT = basisT;
        this.datoms = datoms;
        this.entities = entities;
        this.attributes = attributes;
    }

    public long basisT() { return basisT; }
    public long datoms() { return datoms; }
    public long entities() { return entities; }
    public long attributes() { return attributes; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof DbStats)) return false;
        DbStats that = (DbStats) other;
        return basisT == that.basisT && datoms == that.datoms
                && entities == that.entities && attributes == that.attributes;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(basisT, datoms, entities, attributes);
    }
}
