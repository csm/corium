package io.corium.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Runs a few self-checking MusicBrainz queries through the PostgreSQL JDBC
 * driver. The queries use {@link Statement} because Corium's PostgreSQL wire
 * endpoint does not yet support bound parameters.
 */
public final class MusicBrainzJdbcExample {
    private static final String DEFAULT_URL =
            "jdbc:postgresql://127.0.0.1:55432/mbrainz";
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(15);

    private MusicBrainzJdbcExample() {
    }

    public static void main(String[] args) throws Exception {
        String url = getenv("CORIUM_JDBC_URL", DEFAULT_URL);
        String user = getenv("CORIUM_JDBC_USER", "corium");
        String password = getenv("CORIUM_JDBC_PASSWORD", "");

        try (Connection connection = connect(url, user, password)) {
            System.out.printf("Connected to %s%n", url);
            checkReleaseCount(connection);
            checkReleaseArtists(connection);
            checkOkComputerTracks(connection);
        }

        System.out.println("All JDBC query checks passed.");
    }

    private static Connection connect(String url, String user, String password)
            throws SQLException, InterruptedException {
        long deadline = System.nanoTime() + CONNECT_TIMEOUT.toNanos();
        SQLException lastError = null;

        while (System.nanoTime() < deadline) {
            try {
                return DriverManager.getConnection(url, user, password);
            } catch (SQLException error) {
                lastError = error;
                Thread.sleep(100);
            }
        }

        throw new SQLException(
                "Corium PostgreSQL server did not become ready within "
                        + CONNECT_TIMEOUT.toSeconds() + " seconds",
                lastError);
    }

    private static void checkReleaseCount(Connection connection)
            throws SQLException {
        String sql = "SELECT COUNT(*) AS release_count "
                + "FROM corium.release WHERE year = 1997";

        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(sql)) {
            require(rows.next(), "release count query returned no row");
            long count = rows.getLong("release_count");
            require(count == 20, "expected 20 releases, got " + count);
            require(!rows.next(), "release count query returned multiple rows");
            System.out.printf("1997 releases: %d%n", count);
        }
    }

    private static void checkReleaseArtists(Connection connection)
            throws SQLException {
        String sql = "SELECT r.name AS release_name, a.name AS artist_name "
                + "FROM corium.release r "
                + "JOIN corium.artist a ON array_has(r.artists, a.e) "
                + "ORDER BY r.name";
        Map<String, String> releases = new LinkedHashMap<>();

        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(sql)) {
            while (rows.next()) {
                releases.put(
                        rows.getString("release_name"),
                        rows.getString("artist_name"));
            }
        }

        require(releases.size() == 20,
                "expected 20 release/artist rows, got " + releases.size());
        require("Radiohead".equals(releases.get("OK Computer")),
                "OK Computer was not joined to Radiohead");
        require("Björk".equals(releases.get("Homogenic")),
                "Homogenic was not joined to Björk");

        System.out.println("First five releases by title:");
        releases.entrySet().stream().limit(5).forEach(entry ->
                System.out.printf("  %-48s %s%n",
                        entry.getKey(), entry.getValue()));
    }

    private static void checkOkComputerTracks(Connection connection)
            throws SQLException {
        String sql = "SELECT t.position AS track_number, t.name AS track_name "
                + "FROM corium.release r "
                + "JOIN corium.medium m ON array_has(r.media, m.e) "
                + "JOIN corium.track t ON array_has(m.tracks, t.e) "
                + "WHERE r.name = 'OK Computer' "
                + "ORDER BY t.position";
        List<String> tracks = new ArrayList<>();

        try (Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(sql)) {
            while (rows.next()) {
                int expectedPosition = tracks.size() + 1;
                require(rows.getInt("track_number") == expectedPosition,
                        "unexpected OK Computer track position");
                tracks.add(rows.getString("track_name"));
            }
        }

        require(tracks.size() == 12,
                "expected 12 OK Computer tracks, got " + tracks.size());
        require("Airbag".equals(tracks.get(0)),
                "expected Airbag to be the first OK Computer track");
        require("The Tourist".equals(tracks.get(tracks.size() - 1)),
                "expected The Tourist to be the last OK Computer track");
        System.out.printf("OK Computer tracks: %d (%s … %s)%n",
                tracks.size(), tracks.get(0), tracks.get(tracks.size() - 1));
    }

    private static String getenv(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
