package io.corium.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** An immutable builder for map-form and operation-form transactions. */
public final class TxBuilder implements EdnForm {
    private final List<Object> forms;

    public TxBuilder() {
        this.forms = List.of();
    }

    private TxBuilder(List<Object> forms) {
        this.forms = List.copyOf(forms);
    }

    public TxBuilder entity(EntityMap entity) { return form(entity); }
    public TxBuilder add(Object entity, String attribute, Object value) {
        return form(Arrays.asList(new Keyword("db/add"), entity, new Keyword(attribute), value));
    }
    public TxBuilder retract(Object entity, String attribute, Object value) {
        return form(Arrays.asList(new Keyword("db/retract"), entity, new Keyword(attribute), value));
    }
    public TxBuilder cas(Object entity, String attribute, Object oldValue, Object newValue) {
        return form(Arrays.asList(new Keyword("db/cas"), entity, new Keyword(attribute), oldValue, newValue));
    }
    public TxBuilder retractEntity(Object entity) {
        return form(List.of(new Keyword("db/retractEntity"), entity));
    }
    public TxBuilder form(Object form) {
        List<Object> next = new ArrayList<>(forms);
        next.add(form);
        return new TxBuilder(next);
    }
    @Override
    public Object toEdn() { return forms; }
}
