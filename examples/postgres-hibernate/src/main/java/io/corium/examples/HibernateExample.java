package io.corium.examples;

import java.util.List;
import java.util.Map;

import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;

public final class HibernateExample {
    private static final String DEFAULT_URL =
            "jdbc:postgresql://127.0.0.1:55432/people";

    private HibernateExample() {
    }

    public static void main(String[] args) {
        String url = getenv("CORIUM_JDBC_URL", DEFAULT_URL);
        StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
                .applySettings(Map.of(
                        "hibernate.connection.url", url,
                        "hibernate.connection.username",
                        getenv("CORIUM_JDBC_USER", "corium"),
                        "hibernate.connection.password",
                        getenv("CORIUM_JDBC_PASSWORD", ""),
                        "hibernate.hbm2ddl.auto", "none",
                        "hibernate.show_sql", "true",
                        "hibernate.format_sql", "false"))
                .build();

        try (SessionFactory sessions = new MetadataSources(registry)
                .addAnnotatedClass(Person.class)
                .buildMetadata()
                .buildSessionFactory()) {
            Long id = sessions.fromTransaction(session -> {
                Person person = new Person("Ada Lovelace", 36);
                session.persist(person);
                session.flush();
                require(person.getId() != null, "Hibernate did not receive a generated id");
                return person.getId();
            });

            Person inserted = sessions.fromSession(session -> session.find(Person.class, id));
            require(inserted != null, "inserted entity was not found");
            require("Ada Lovelace".equals(inserted.getName()), "inserted name did not round-trip");
            require(inserted.getAge() == 36, "inserted age did not round-trip");

            sessions.inTransaction(session -> {
                Person person = session.find(Person.class, id);
                require(person != null, "entity disappeared before update");
                person.setName("Augusta Ada King");
            });

            List<Person> adults = sessions.fromSession(session -> session
                    .createQuery(
                            "from Person where age >= :minimum order by name",
                            Person.class)
                    .setParameter("minimum", 18L)
                    .getResultList());
            require(adults.stream().anyMatch(person -> id.equals(person.getId())
                            && "Augusta Ada King".equals(person.getName())),
                    "updated entity was not returned by HQL");

            sessions.inTransaction(session -> {
                Person person = session.find(Person.class, id);
                require(person != null, "entity disappeared before delete");
                session.remove(person);
            });
            require(sessions.fromSession(session -> session.find(Person.class, id)) == null,
                    "deleted entity is still visible");

            System.out.printf(
                    "Hibernate read/write entity check passed against %s (entity %d).%n",
                    url, id);
        } catch (RuntimeException error) {
            StandardServiceRegistryBuilder.destroy(registry);
            throw error;
        }
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
