package dev.corium.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** An immutable builder for common Corium Datalog queries. */
public final class Query implements EdnForm {
    /** A typed {@code :where} clause. */
    public interface Clause extends EdnForm {}

    private enum Shape { RELATION, COLLECTION, TUPLE, SCALAR }

    private final Shape shape;
    private final List<Object> find;
    private final List<Object> inputs;
    private final List<Clause> where;

    private Query(Shape shape, List<Object> find, List<Object> inputs, List<Clause> where) {
        if (find.isEmpty()) throw new IllegalArgumentException("find requires at least one element");
        this.shape = shape;
        this.find = List.copyOf(find);
        this.inputs = List.copyOf(inputs);
        this.where = List.copyOf(where);
    }

    public static Query find(String... variables) {
        return create(Shape.RELATION, variables);
    }

    public static Query findCollection(String variable) {
        return create(Shape.COLLECTION, variable);
    }

    public static Query findTuple(String... variables) {
        return create(Shape.TUPLE, variables);
    }

    public static Query findScalar(String variable) {
        return create(Shape.SCALAR, variable);
    }

    private static Query create(Shape shape, String... variables) {
        List<Object> find = new ArrayList<>();
        for (String variable : variables) find.add(variable(variable));
        return new Query(shape, find, List.of(), List.of());
    }

    public Query inScalar(String variable) {
        return withInput(variable(variable));
    }

    public Query inCollection(String variable) {
        return withInput(List.of(variable(variable), new Symbol("...")));
    }

    public Query inTuple(String... variables) {
        if (variables.length == 0) throw new IllegalArgumentException("tuple input requires variables");
        return withInput(symbols(variables));
    }

    public Query inRelation(String... variables) {
        if (variables.length == 0) throw new IllegalArgumentException("relation input requires variables");
        return withInput(List.of(symbols(variables)));
    }

    public Query inRules() {
        return withInput(new Symbol("%"));
    }

    public Query inDatabase(String name) {
        Objects.requireNonNull(name, "name");
        return withInput(new Symbol(name.startsWith("$") ? name : "$" + name));
    }

    private Query withInput(Object input) {
        List<Object> next = new ArrayList<>(inputs);
        next.add(input);
        return new Query(shape, find, next, where);
    }

    public Query where(Object entity, Object attribute, Object value) {
        return where(data(entity, attribute, value));
    }

    public Query where(Clause clause) {
        List<Clause> next = new ArrayList<>(where);
        next.add(Objects.requireNonNull(clause, "clause"));
        return new Query(shape, find, inputs, next);
    }

    public static Clause data(Object entity, Object attribute, Object value) {
        return () -> Arrays.asList(term(entity), term(attribute), term(value));
    }

    public static Clause predicate(String name, Object... arguments) {
        Objects.requireNonNull(name, "name");
        return () -> {
            List<Object> invocation = new ArrayList<>();
            invocation.add(new Symbol(name));
            Arrays.stream(arguments).map(Query::term).forEach(invocation::add);
            return List.of(new EdnList(invocation));
        };
    }

    public static Clause gt(Object left, Object right) { return predicate(">", left, right); }
    public static Clause gte(Object left, Object right) { return predicate(">=", left, right); }
    public static Clause lt(Object left, Object right) { return predicate("<", left, right); }
    public static Clause lte(Object left, Object right) { return predicate("<=", left, right); }
    public static Clause eq(Object left, Object right) { return predicate("=", left, right); }
    public static Clause notEqual(Object left, Object right) { return predicate("not=", left, right); }

    private static Symbol variable(String name) {
        Objects.requireNonNull(name, "name");
        String normalized = name.startsWith("?") ? name : "?" + name;
        if (normalized.length() == 1) throw new IllegalArgumentException("variable must not be empty");
        return new Symbol(normalized);
    }

    private static List<Object> symbols(String[] variables) {
        List<Object> result = new ArrayList<>();
        for (String variable : variables) result.add(variable(variable));
        return result;
    }

    private static Object term(Object value) {
        if (!(value instanceof String)) return value;
        String text = (String) value;
        if (text.startsWith("?")) return new Symbol(text);
        if (text.startsWith(":")) return new Keyword(text);
        if (text.equals("_")) return new Symbol("_");
        return text;
    }

    @Override
    public Object toEdn() {
        List<Object> findForm = new ArrayList<>(find);
        if (shape == Shape.COLLECTION) findForm = List.of(List.of(find.get(0), new Symbol("...")));
        if (shape == Shape.TUPLE) findForm = List.of(find);
        if (shape == Shape.SCALAR) findForm.add(new Symbol("."));

        Map<Object, Object> form = new LinkedHashMap<>();
        form.put(new Keyword("find"), findForm);
        if (!inputs.isEmpty()) {
            List<Object> inputForm = new ArrayList<>();
            inputForm.add(new Symbol("$"));
            inputForm.addAll(inputs);
            form.put(new Keyword("in"), inputForm);
        }
        List<Object> clauses = new ArrayList<>();
        where.stream().map(EdnForm::toEdn).forEach(clauses::add);
        form.put(new Keyword("where"), clauses);
        return Collections.unmodifiableMap(form);
    }
}
