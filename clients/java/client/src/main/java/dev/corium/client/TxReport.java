package dev.corium.client;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** The result of a committed transaction. */
public final class TxReport {
    private final long basisBefore;
    private final long basisT;
    private final Instant transactionInstant;
    private final Map<String, Long> tempids;
    private final Db dbAfter;

    TxReport(long basisBefore, long basisT, Instant transactionInstant,
             Map<String, Long> tempids, Db dbAfter) {
        this.basisBefore = basisBefore;
        this.basisT = basisT;
        this.transactionInstant = transactionInstant;
        this.tempids = Collections.unmodifiableMap(new LinkedHashMap<>(tempids));
        this.dbAfter = dbAfter;
    }

    public long basisBefore() { return basisBefore; }
    public long basisT() { return basisT; }
    public Instant transactionInstant() { return transactionInstant; }
    public Map<String, Long> tempids() { return tempids; }
    public Db dbAfter() { return dbAfter; }
}
