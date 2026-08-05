package dev.corium.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;

/** Endpoint parsing and transport-security rules shared by both peers. */
final class Endpoints {
    private Endpoints() {}

    /** Parses one {@code http://} or {@code https://} origin. */
    static URI parse(String value) {
        try {
            URI uri = new URI(Objects.requireNonNull(value, "endpoint"));
            if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                    || uri.getHost() == null || uri.getUserInfo() != null || uri.getQuery() != null
                    || uri.getFragment() != null
                    || (uri.getPath() != null && !uri.getPath().isEmpty() && !"/".equals(uri.getPath()))) {
                throw new IllegalArgumentException("endpoint must be an http:// or https:// origin");
            }
            return uri;
        } catch (URISyntaxException error) {
            throw new IllegalArgumentException("invalid endpoint", error);
        }
    }

    /**
     * Returns whether {@code endpoints} use TLS, rejecting mixed schemes and
     * credentials or TLS options that plaintext cannot protect.
     */
    static boolean tls(List<String> endpoints, boolean hasToken, boolean allowInsecureToken,
                       byte[] tlsCa, String tlsDomain) {
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("at least one endpoint is required");
        }
        boolean tls = "https".equalsIgnoreCase(parse(endpoints.get(0)).getScheme());
        for (String endpoint : endpoints) {
            if (tls != "https".equalsIgnoreCase(parse(endpoint).getScheme())) {
                throw new IllegalArgumentException(
                        "endpoints must consistently use http:// or https://");
            }
        }
        if (hasToken && !tls && !allowInsecureToken) {
            throw new IllegalArgumentException("bearer tokens require an https:// endpoint");
        }
        if (!tls && (tlsCa != null || tlsDomain != null)) {
            throw new IllegalArgumentException("custom TLS options require an https:// endpoint");
        }
        return tls;
    }
}
