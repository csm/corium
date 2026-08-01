package dev.corium.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class BuildersTest {
    @Test
    void queryBuilderIsImmutableAndLowersTerms() {
        Query base = Query.find("?name");
        Query query = base.inScalar("?minimum")
                .where("?entity", ":person/name", "?name")
                .where(Query.gte("?age", "?minimum"));

        Map<?, ?> baseForm = (Map<?, ?>) base.toEdn();
        Map<?, ?> form = (Map<?, ?>) query.toEdn();
        assertEquals(List.of(), baseForm.get(new Keyword("where")));
        assertEquals(List.of(new Symbol("$"), new Symbol("?minimum")),
                form.get(new Keyword("in")));
        assertEquals(List.of(new Symbol("?entity"), new Keyword("person/name"), new Symbol("?name")),
                ((List<?>) form.get(new Keyword("where"))).get(0));
    }

    @Test
    void pullAndTransactionBuildersProduceBoundaryForms() {
        Pull pull = new Pull().dbId().attr("person/name")
                .nested("person/friends", new Pull().attr("person/name"));
        assertEquals(new Keyword("db/id"), ((List<?>) pull.toEdn()).get(0));

        TxBuilder tx = new TxBuilder().entity(EntityMap.withId("ada")
                .set("person/name", "Ada").set("person/age", 36));
        List<?> forms = (List<?>) tx.toEdn();
        Map<?, ?> entity = (Map<?, ?>) ((EdnForm) forms.get(0)).toEdn();
        assertEquals("ada", entity.get(new Keyword("db/id")));
        assertEquals("Ada", entity.get(new Keyword("person/name")));
    }
}
