package dev.corium.client;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;
import java.util.ServiceLoader;

/**
 * Locates the native libraries the in-process peer needs.
 *
 * <p>The engine and every storage plugin ship as one platform-classified jar
 * each, holding a library under {@code dev/corium/native/<classifier>/}. A
 * library found on the classpath is extracted to a content-addressed cache
 * directory, because a shared library cannot be loaded from inside a jar.
 *
 * <p>Development builds can skip the jars entirely by pointing the
 * {@code corium.native.directory} system property, or the
 * {@code CORIUM_NATIVE_DIRECTORY} environment variable, at a Cargo output
 * directory such as {@code target/release}.
 */
public final class NativeLibraries {
    private static final String ENGINE = "corium_jni";
    private static final String DIRECTORY_PROPERTY = "corium.native.directory";
    private static final String DIRECTORY_ENVIRONMENT = "CORIUM_NATIVE_DIRECTORY";
    private static final String CACHE_PROPERTY = "corium.native.cache";
    private static final String RESOURCE_PREFIX = "dev/corium/native/";

    private NativeLibraries() {}

    /** The platform classifier of the native jars this JVM needs. */
    public static String classifier() {
        return operatingSystem() + "-" + architecture();
    }

    /** The platform file name of one library, such as {@code libcorium_jni.so}. */
    public static String libraryFileName(String library) {
        switch (operatingSystem()) {
            case "windows": return library + ".dll";
            case "macos": return "lib" + library + ".dylib";
            default: return "lib" + library + ".so";
        }
    }

    /**
     * Returns the on-disk path of one native library, extracting it from
     * {@code owner}'s classpath when it ships inside a jar.
     *
     * @throws NativeExtensionException when no artifact provides the library
     */
    public static Path resolve(Class<?> owner, String library) {
        String fileName = libraryFileName(library);
        Path directory = overrideDirectory();
        if (directory != null) {
            Path candidate = directory.resolve(fileName);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }
        String resource = RESOURCE_PREFIX + classifier() + "/" + fileName;
        ClassLoader loader = owner.getClassLoader();
        URL located = loader == null ? null : loader.getResource(resource);
        if (located == null) {
            throw new NativeExtensionException(
                    "the native library " + fileName + " is missing; add the "
                            + classifier() + " artifact for this platform, or set the "
                            + DIRECTORY_PROPERTY + " system property");
        }
        try {
            return extract(located, fileName);
        } catch (IOException error) {
            throw new NativeExtensionException(
                    "could not unpack the native library " + fileName, error);
        }
    }

    /**
     * Loads the engine and every installed storage plugin, once per JVM.
     *
     * <p>Loading happens in {@code Native}'s class initializer, so the JVM
     * reports a failure as a {@link LinkageError} at the point of first use —
     * and as a bare {@code NoClassDefFoundError} on every later use. This
     * restores the original cause in both cases.
     */
    static void ensureEngineLoaded() {
        try {
            Native.ensureLoaded();
        } catch (LinkageError error) {
            Throwable cause = error.getCause();
            if (cause instanceof CoriumException) {
                throw (CoriumException) cause;
            }
            throw new NativeExtensionException(
                    "the Corium native engine could not be loaded", error);
        }
    }

    /** Loads the engine library into this JVM. */
    static void loadEngine() {
        System.load(resolve(NativeLibraries.class, ENGINE).toAbsolutePath().toString());
    }

    /** Registers every {@link StoragePlugin} the classpath provides. */
    static void registerStoragePlugins() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) {
            loader = NativeLibraries.class.getClassLoader();
        }
        for (StoragePlugin plugin : ServiceLoader.load(StoragePlugin.class, loader)) {
            try {
                Native.loadStoragePlugin(plugin.libraryPath().toAbsolutePath().toString());
            } catch (RuntimeException error) {
                throw new NativeExtensionException(
                        "the storage plugin " + plugin.backend() + " failed to load", error);
            }
        }
    }

    private static Path extract(URL resource, String fileName) throws IOException {
        byte[] library = read(resource);
        Path directory = cacheDirectory().resolve(digest(library));
        Path target = directory.resolve(fileName);
        if (Files.isRegularFile(target) && Files.size(target) == library.length) {
            return target;
        }
        Files.createDirectories(directory);
        Path staged = Files.createTempFile(directory, fileName, ".partial");
        try {
            Files.write(staged, library);
            try {
                Files.move(staged, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (FileAlreadyExistsException concurrent) {
                // Another JVM extracted the same content first; its copy wins.
                Files.deleteIfExists(staged);
            }
        } catch (IOException error) {
            Files.deleteIfExists(staged);
            throw error;
        }
        return target;
    }

    private static byte[] read(URL resource) throws IOException {
        try (InputStream stream = resource.openStream()) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[64 * 1024];
            int read;
            while ((read = stream.read(buffer)) >= 0) {
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        }
    }

    private static String digest(byte[] library) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(library);
            StringBuilder text = new StringBuilder(32);
            for (int index = 0; index < 16; index++) {
                text.append(String.format("%02x", hash[index]));
            }
            return text.toString();
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }

    private static Path overrideDirectory() {
        String directory = System.getProperty(DIRECTORY_PROPERTY);
        if (directory == null) {
            directory = System.getenv(DIRECTORY_ENVIRONMENT);
        }
        return directory == null || directory.isEmpty() ? null : Paths.get(directory);
    }

    private static Path cacheDirectory() {
        String configured = System.getProperty(CACHE_PROPERTY);
        if (configured != null && !configured.isEmpty()) {
            return Paths.get(configured);
        }
        String cacheHome = System.getenv(
                "windows".equals(operatingSystem()) ? "LOCALAPPDATA" : "XDG_CACHE_HOME");
        if (cacheHome != null && !cacheHome.isEmpty()) {
            return Paths.get(cacheHome).resolve("corium").resolve("native");
        }
        String home = System.getProperty("user.home");
        if (home != null && !home.isEmpty()) {
            return Paths.get(home).resolve(".cache").resolve("corium").resolve("native");
        }
        return Paths.get(System.getProperty("java.io.tmpdir")).resolve("corium-native");
    }

    private static String operatingSystem() {
        String name = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        if (name.contains("win")) return "windows";
        if (name.contains("mac") || name.contains("darwin")) return "macos";
        return "linux";
    }

    private static String architecture() {
        String architecture = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);
        if (architecture.equals("aarch64") || architecture.equals("arm64")) return "aarch64";
        if (architecture.equals("amd64") || architecture.equals("x86_64")) return "x86_64";
        return architecture;
    }
}
