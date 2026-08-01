package dev.corium.client;

import java.io.ByteArrayInputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

final class TrustManagers {
    private TrustManagers() {}

    static X509TrustManager platformAndPem(byte[] pem) throws Exception {
        X509TrustManager platform = manager(null);
        KeyStore customStore = KeyStore.getInstance(KeyStore.getDefaultType());
        customStore.load(null);
        Collection<? extends Certificate> certificates = CertificateFactory.getInstance("X.509")
                .generateCertificates(new ByteArrayInputStream(pem));
        if (certificates.isEmpty()) throw new IllegalArgumentException("TLS CA contains no certificates");
        int index = 0;
        for (Certificate certificate : certificates) customStore.setCertificateEntry("corium-" + index++, certificate);
        return new CompositeTrustManager(platform, manager(customStore));
    }

    private static X509TrustManager manager(KeyStore store) throws Exception {
        TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        factory.init(store);
        for (TrustManager manager : factory.getTrustManagers()) {
            if (manager instanceof X509TrustManager) return (X509TrustManager) manager;
        }
        throw new IllegalStateException("the JVM has no X.509 trust manager");
    }

    private static final class CompositeTrustManager implements X509TrustManager {
        private final List<X509TrustManager> managers;

        private CompositeTrustManager(X509TrustManager... managers) {
            this.managers = List.of(managers);
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType)
                throws java.security.cert.CertificateException {
            check(manager -> manager.checkClientTrusted(chain, authType));
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType)
                throws java.security.cert.CertificateException {
            check(manager -> manager.checkServerTrusted(chain, authType));
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            List<X509Certificate> certificates = new ArrayList<>();
            managers.forEach(manager -> certificates.addAll(Arrays.asList(manager.getAcceptedIssuers())));
            return certificates.toArray(new X509Certificate[0]);
        }

        private void check(Check operation) throws java.security.cert.CertificateException {
            java.security.cert.CertificateException rejected = null;
            for (X509TrustManager manager : managers) {
                try {
                    operation.run(manager);
                    return;
                } catch (java.security.cert.CertificateException error) {
                    rejected = error;
                }
            }
            throw rejected == null ? new java.security.cert.CertificateException("certificate was rejected") : rejected;
        }

        private interface Check {
            void run(X509TrustManager manager) throws java.security.cert.CertificateException;
        }
    }
}
