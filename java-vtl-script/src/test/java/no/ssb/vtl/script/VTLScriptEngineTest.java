package no.ssb.vtl.script;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import no.ssb.vtl.connectors.Connector;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.parser.VTLLexer;
import no.ssb.vtl.script.support.VTLPrintStream;
import org.antlr.v4.runtime.Vocabulary;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.Role;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VTLScriptEngineTest {

    private Dataset dataset = mock(Dataset.class);
    private Connector connector = mock(Connector.class);
    private ScriptEngine engine = new VTLScriptEngine(connector);
    private Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);

    @Test
    public void testKeywords() {
        Map<String, Set<String>> keywords = ((VTLScriptEngine) engine).getVTLKeywords();
        Set<String> keywordsList = keywords.values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());

        Set<String> keywordsFromLexer = IntStream.range(0, VTLLexer.VOCABULARY.getMaxTokenType())
                .mapToObj(VTLLexer.VOCABULARY::getLiteralName)
                .filter(Objects::nonNull)
                .map(s -> s.replaceAll("'", ""))
                .collect(Collectors.toSet());

        Sets.SetView<String> symmetricDifference = Sets.symmetricDifference(keywordsList, keywordsFromLexer);

        System.out.println("Missing in keywords: " + Sets.difference(keywordsFromLexer, keywordsList));
        System.out.println("Missing in lexer: " + Sets.difference(keywordsList, keywordsFromLexer));

        // Will fail if a new keyword is added in the grammar or list of keywords without updating
        // the test.
        assertThat(symmetricDifference).containsExactlyInAnyOrder(
                "time_aggregate", "exists_in_all", "match_characters", 
                "timeshift", "join", "flow_to_stock", "identifier", 
                "string_from_date", "subscript", "transcode", 
                "setdiff", "current_date", "measure", 
                "extract", "eval", "concatenation", 
                "unique", "true", "exists_in", 
                "func_dep", "symdiff", "attribute",
                "fill_time_series", "intersect", "not_exists_in_all",
                "false", "any", 
                "lenght", "stock_to_flow", "not_exists_in",
                "aggregatefunctions", "alterdataset", "||",
                "<=", "<>", "measures",
                "(", ")", "*",
                "+", ",", "-",
                ".", "/", "condition",
                "not_valid", ":", "<", "=",
                ">", ">=", "implicit",
                ":=", "role", "errorlevel",
                "valid", "[", "]",
                "errorcode", "prod", "length",
                "{", "}"
        );

        Vocabulary vocabulary = VTLLexer.VOCABULARY;
        for (int i = 0; i < vocabulary.getMaxTokenType(); i++) {
            System.out.printf(
                    "[%s] litName: %s, symName: %s, dispName:%s\n",
                    i,
                    vocabulary.getLiteralName(i),
                    vocabulary.getSymbolicName(i),
                    vocabulary.getDisplayName(i)
            );
        }
    }
    @Test
    public void testVersion() {
        assertThat(new ComparableVersion("0.1.9")).isLessThan(new ComparableVersion("0.1.9-1"));
    }

    @Test
    public void testAssignment() throws Exception {

        bindings.put("ds1", dataset);
        engine.eval("ds2 := ds1");

        assertThat(bindings).containsKey("ds2");
        Object ds2 = bindings.get("ds2");
        assertThat(ds2).isInstanceOf(Dataset.class);
        assertThat(ds2).isSameAs(dataset);

    }

    @Test
    public void testEscapedAssignment() throws Exception {

        bindings.put("ds1", dataset);
        engine.eval("'1escapedDs2' := ds1");

        assertThat(bindings).containsKey("1escapedDs2");
        Object ds2 = bindings.get("1escapedDs2");
        assertThat(ds2).isInstanceOf(Dataset.class);
        assertThat(ds2).isSameAs(dataset);

    }

    @Test
    public void testEscapedExpression() throws Exception {

        bindings.put("123escaped-ds1", dataset);
        engine.eval("ds2 := '123escaped-ds1'");

        assertThat(bindings).containsKey("ds2");
        Object ds2 = bindings.get("ds2");
        assertThat(ds2).isInstanceOf(Dataset.class);
        assertThat(ds2).isSameAs(dataset);

    }

    @Test
    public void testJoinAssignment() throws ScriptException {
        Dataset simpleDataset = StaticDataset.create()
                .addComponent("id", Role.IDENTIFIER, String.class)
                .addComponent("me", Role.MEASURE, Long.class)
                .addPoints("id", 0L)
                .build();

        bindings.put("ds", simpleDataset);
        engine.eval("/* join assignment */" +
                "res := [ds] {" +
                "   assigned := me + 1" +
                "}");

        assertThat(bindings).containsKey("res");
        Object res = bindings.get("res");
        assertThat(res).isInstanceOf(Dataset.class);
        assertThat(((Dataset)res).getDataStructure()).containsKeys("assigned");
        assertThat(((Dataset) res).getData()).containsExactly(
                DataPoint.create("id", 0L, +1L)
        );
    }

    @Test
    public void testJoinEscapedAssignment() throws ScriptException {

        Dataset simpleDataset = StaticDataset.create()
                .addComponent("id", Role.IDENTIFIER, String.class)
                .addComponent("me", Role.MEASURE, Long.class)
                .addPoints("id", 0L)
                .build();

        bindings.put("ds", simpleDataset);
        engine.eval("/* join assignment */" +
                "res := [ds] {" +
                "   '123escaped-assigned' := me + 1" +
                "}");

        assertThat(bindings).containsKey("res");
        Object res = bindings.get("res");
        assertThat(res).isInstanceOf(Dataset.class);
        assertThat(((Dataset)res).getDataStructure()).containsKeys("123escaped-assigned");
        assertThat(((Dataset) res).getData()).containsExactly(
                DataPoint.create("id", 0L, 1L)
        );
    }

    @Test
    public void testJoinEscapedExpression() throws ScriptException {
        Dataset simpleDataset = StaticDataset.create()
                .addComponent("id", Role.IDENTIFIER, String.class)
                .addComponent("123escaped-me", Role.MEASURE, Long.class)
                .addPoints("id", 0L)
                .build();

        bindings.put("ds", simpleDataset);
        engine.eval("/* join assignment */" +
                "res := [ds] {" +
                "   assigned := '123escaped-me' + 1" +
                "}");

        assertThat(bindings).containsKey("res");
        Object res = bindings.get("res");
        assertThat(res).isInstanceOf(Dataset.class);
        assertThat(((Dataset)res).getDataStructure()).containsKeys("assigned");
        assertThat(((Dataset) res).getData()).containsExactly(
                DataPoint.create("id", 0L, 1L)
        );
    }

    @Test
    public void testAssignmentLiterals() throws Exception {

        StaticDataset dataset = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addPoints("1")
                .build();

        bindings.put("t1", dataset);
        engine.eval("/* test */\n" +
                "resultat := [t1] {\n" +
                "    testFloat := 1.0," +
                "    testInteger := 1," +
                "    testString := \"test string\",\n" +
                "    testString2 := \"test \"\"escaped\"\" string\",\n" +
                "    testBoolean := true" +
                "}");

        assertThat(bindings).containsKey("resultat");
        assertThat(bindings.get("resultat")).isInstanceOf(Dataset.class);

        Dataset resultat = (Dataset) bindings.get("resultat");
        assertThat(resultat.getDataStructure())
                .describedAs("data structure of resultat")
                .containsOnlyKeys(
                        "id1",
                        "testFloat",
                        "testInteger",
                        "testString",
                        "testString2",
                        "testBoolean"
                );

        assertThat(resultat.getData())
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        "1", 1.0d, 1L, "test string", "test \"escaped\" string", true
                );
    }

    @Test
    public void testGet() throws Exception {

        when(connector.canHandle(anyString())).thenReturn(true);
        when(connector.getDataset(anyString())).thenReturn(dataset);

        bindings.put("ds1", dataset);
        engine.eval("ds2 := get(\"todo\")");

        assertThat(bindings).contains(entry("ds2", dataset));

    }

    @Test
    public void testPut() throws Exception {

        when(connector.canHandle(anyString())).thenReturn(true);
        when(connector.putDataset(anyString(), any())).thenReturn(dataset);
        engine.eval("ds1 := put(\"todo\")");

        assertThat(bindings).contains(entry("ds1", dataset));

    }

    @Test
    public void testSimpleJoin() throws Exception {

        Dataset ds1 = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("id2", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.MEASURE, Long.class)
                .addComponent("m2", Role.MEASURE, Double.class)
                .addComponent("at1", Role.MEASURE, String.class)

                .addPoints("1", "1", -50L, 1.5D, "attr1-1")
                .addPoints("2", "2", 100L, 0.123456789, "attr1-2")
                .build();

        Dataset ds2 = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("id2", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.MEASURE, Long.class)
                .addComponent("m2", Role.MEASURE, Double.class)
                .addComponent("at2", Role.MEASURE, String.class)

                .addPoints("1", "1", 30L, -1.0D, "attr2-1")
                .addPoints("2", "2", -40L, 0.987654321, "attr2-2")
                .build();

        bindings.put("ds1", ds1);
        bindings.put("ds2", ds2);

        engine.eval("" +
                "ds3 := [ds1, ds2] {" +
                "  at := at1 || at2," +
                "  m1 := ds1.m1 + ds2.m1," +
                "  m2 := ds1.m2 + ds2.m2," +
                "  keep at, m1, m2" +
                "}" +
                "");

        assertThat(bindings).containsKey("ds3");
        assertThat(bindings.get("ds3")).isInstanceOf(Dataset.class);

        Dataset ds3 = (Dataset) bindings.get("ds3");
        assertThat(ds3.getDataStructure())
                .describedAs("data structure of d3")
                .containsOnlyKeys(
                        "id2", "m1", "m2", "id1", "at"
                );

        assertThat(ds3.getData())
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        "1", "1", "attr1-1" + "attr2-1", (-50L + 30), 0.5D,
                        "2", "2", "attr1-2" + "attr2-2", 60L, 1.11111111D
                );
    }

    @Test
    public void testJoin() throws Exception {

        Dataset ds1 = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("id2", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.MEASURE, Long.class)
                .addComponent("m2", Role.MEASURE, Double.class)
                .addComponent("at1", Role.MEASURE, String.class)

                .addPoints("1", "1", 0L, 0.0, "attr1-1")
                .addPoints("1", "2", 10L, 200.0, "attr1-2")

                .build();

        Dataset ds2 = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("id2", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.MEASURE, Long.class)
                .addComponent("m2", Role.MEASURE, Double.class)
                .addComponent("at2", Role.MEASURE, String.class)

                .addPoints("1", "1", 30L, 40.0, "attr2-1")
                .addPoints("1", "2", 0L, 0.0, "attr2-2")

                .build();

        bindings.put("ds1", ds1);
        bindings.put("ds2", ds2);

        engine.eval("" +
                "ds3 := [ds1, ds2]{" +                                      // id1, id2, ds1.m1, ds1.m2, d2.m1, d2.m2, at1, at2
                "  filter id1 = \"1\" and ds2.m1 = 30 or ds1.m1 = 10," +            //TODO: precedence
                "  ident := ds1.m1 + ds2.m2 - ds1.m2 - ds2.m1," +            // id1, id2, ds1.m1, ds1.m2, ds2.m1, ds2.m2, at1, at2, ident
                "  keep ident, ds1.m1, ds2.m1, ds2.m2," +                    // id1, id2, ds1.m1, ds2.m1, ds2.m2                  , ident
                "  boolTest := (ds1.m1 = 10)," +                             // id1, id2, ds1.m1, ds2.m1, ds2.m2                  , ident, boolTest
                "  drop ds2.m1," +                                           // id1, id2, ds1.m1,       , ds2.m2                  , ident, boolTest
                "  rename id1 to renamedId1, ds1.m1 to m1, ds2.m2 to m2" +   // renamedId1, id2, m1, m2, ident, boolTest
                "}" +
                "");

        assertThat(bindings).containsKey("ds3");
        assertThat(bindings.get("ds3")).isInstanceOf(Dataset.class);

        Dataset ds3 = (Dataset) bindings.get("ds3");
        assertThat(ds3.getDataStructure())
                .describedAs("data structure of d3")
                .containsOnlyKeys(
                        "renamedId1",
                        "id2",
                        "m2",
                        "m1",
                        "ident",
                        "boolTest"
                );

        assertThat(ds3.getData())
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        "1", "1", 0L, 40.0, 10.0, false,
                        "1", "2", 10L, 0.0, -190.0, true
                );
    }

    @Test
    public void testJoinFold() throws Exception {

        Dataset ds1 = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.MEASURE, Long.class)
                .addComponent("m2", Role.MEASURE, Long.class)
                .addComponent("123-m3", Role.MEASURE, Long.class)

                .addPoints("1", 101L, 102L, 103L)
                .addPoints("2", 201L, 202L, 203L)
                .addPoints("3", 301L, 302L, 303L)

                .build();

        bindings.put("ds1", ds1);
        engine.eval("ds2 := [ds1] {" +
                "  total := ds1.m1 + ds1.m2 + ds1.'123-m3'," +
                "  fold ds1.m1, m2, ds1.'123-m3', total to type, '123-value'" +
                "}"
        );

        assertThat(bindings).containsKey("ds2");
        Dataset ds2 = (Dataset) bindings.get("ds2");

        assertThat(ds2.getDataStructure().getRoles()).containsOnly(
                entry("id1", Role.IDENTIFIER),
                entry("type", Role.IDENTIFIER),
                entry("123-value", Role.MEASURE)
        );

        assertThat(ds2.getData()).flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        "1", "m1", 101L,
                        "1", "m2", 102L,
                        "1", "123-m3", 103L,
                        "1", "total", 101L + 102L + 103L,

                        "2", "m1", 201L,
                        "2", "m2", 202L,
                        "2", "123-m3", 203L,
                        "2", "total", 201L + 202L + 203L,

                        "3", "m1", 301L,
                        "3", "m2", 302L,
                        "3", "123-m3", 303L,
                        "3", "total", 301L + 302L + 303L
                );
    }

    @Test
    public void testBoolean() throws Exception {
        List<List<VTLObject>> data = Lists.newArrayList();
        data.add(
                Lists.newArrayList(VTLObject.of("1"), VTLObject.of(1L), VTLObject.of(2D))
        );

        Dataset ds1 = StaticDataset.create()
                .addComponent("id", Role.IDENTIFIER, String.class)
                .addComponent("integerMeasure", Role.MEASURE, Long.class)
                .addComponent("float", Role.MEASURE, Double.class)

                .addPoints("1", 1L, 2D)
                .build();

        // TODO: Add test that check that we can compare float and integer in VTL.

        bindings.put("ds1", ds1);
        engine.eval("ds2 := [ds1] {" +
                "  lessThan := ds1.integerMeasure < 2," +
                "  lessOrEqual := ds1.integerMeasure <= 2," +
                "  moreThan := ds1.integerMeasure > 2," +
                "  moreOrEqual := ds1.integerMeasure >= 2," +
                "  equal := ds1.integerMeasure = 2" +
                "}"
        );

        DataPoint point = ((Dataset) bindings.get("ds2")).getData().findFirst().get();
        assertThat(point)
                .extracting(VTLObject::get)
                .containsExactly("1", 1L, 2D, true, true, false, false, false);

    }

    @Test
    public void testJoinUnfold() throws Exception {
        Dataset ds1 = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("id2", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.MEASURE, Long.class)
                .addComponent("m2", Role.MEASURE, Double.class)
                .addComponent("at1", Role.MEASURE, String.class)

                .addPoints("1", "one", 101L, 1.1, "attr1")
                .addPoints("1", "two", 102L, 1.1, "attr2")
                .addPoints("2", "one", 201L, 1.1, "attr2")
                .addPoints("2", "two", 202L, 1.1, "attr2")

                .build();

        bindings.put("ds1", ds1);
        engine.eval("ds2 := [ds1] {" +
                "  unfold id2, ds1.m1 to \"one\", \"two\"," +
                "  onePlusTwo := one + two" +
                "}"
        );

        assertThat(bindings).containsKey("ds2");
        Dataset ds2 = (Dataset) bindings.get("ds2");

        assertThat(ds2.getDataStructure().getRoles()).containsOnly(
                entry("id1", Role.IDENTIFIER),
                entry("one", Role.MEASURE),
                entry("two", Role.MEASURE),
                entry("onePlusTwo", Role.MEASURE)
        );

        assertThat(ds2.getData()).flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        "1", 101L, 102L, 101L + 102L,
                        "2", 201L, 202L, 201L + 202L
                );
    }

    @Test
    public void testRename() throws Exception {

        dataset = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("me1", Role.MEASURE, String.class)
                .addComponent("at1", Role.ATTRIBUTE, String.class)
                .build();

        bindings.put("ds1", dataset);
        engine.eval("ds2 := ds1[rename id1 as id3]"
                + "            [rename id3 as id1]"
                + "                [rename id1 as id1m role MEASURE,"
                + "                        me1 as me1a role ATTRIBUTE,"
                + "                        at1 as at1i role IDENTIFIER]");

        assertThat(bindings).containsKey("ds2");
        Dataset result = (Dataset) bindings.get("ds2");

        assertThat(result.getDataStructure().getRoles()).contains(
                entry("id1m", Role.MEASURE),
                entry("me1a", Role.ATTRIBUTE),
                entry("at1i", Role.IDENTIFIER)
        );
    }

    @Test
    public void testCheckSingleRule() throws Exception {

        Dataset ds1 = StaticDataset.create()
                .addComponent("kommune_nr", Role.IDENTIFIER, String.class)
                .addComponent("periode", Role.IDENTIFIER, String.class)
                .addComponent("kostragruppe", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.MEASURE, Long.class)
                .addComponent("at1", Role.ATTRIBUTE, String.class)

                .addPoints("0101", "2015", "EKG14", 100L, "attr1")
                .addPoints("0101", "2015", "EKG15", 110L, "attr4")
                .addPoints("0111", "2014", "EKG14", 101L, "attr2")
                .addPoints("9000", "2014", "EKG14", 102L, "attr3")
                .build();

        Dataset dsCodeList2 = StaticDataset.create()
                .addComponent("code", Role.IDENTIFIER, String.class)
                .addComponent("name", Role.MEASURE, String.class)
                .addComponent("period", Role.IDENTIFIER, String.class)

                .addPoints("0101", "Halden 2010-2013", "2010")
                .addPoints("0101", "Halden 2010-2013", "2011")
                .addPoints("0101", "Halden 2010-2013", "2012")
                .addPoints("0101", "Halden", "2013")
                .addPoints("0101", "Halden", "2014")
                .addPoints("0101", "Halden", "2015")
                .addPoints("0101", "Halden", "2016")
                .addPoints("0101", "Halden", "2017")
                .addPoints("0111", "Hvaler", "2015")
                .addPoints("0111", "Hvaler", "2016")
                .addPoints("0111", "Hvaler", "2017")
                .addPoints("1001", "Kristiansand", "2013")
                .addPoints("1001", "Kristiansand", "2014")
                .addPoints("1001", "Kristiansand", "2015")
                .build();


        Dataset dsCodeList3 = StaticDataset.create()
                .addComponent("code", Role.IDENTIFIER, String.class)
                .addComponent("name", Role.MEASURE, String.class)
                .addComponent("period", Role.IDENTIFIER, String.class)

                .addPoints("EKG14", "Bergen, Trondheim og Stavanger", "2010")
                .addPoints("EKG14", "Bergen, Trondheim og Stavanger", "2011")
                .addPoints("EKG14", "Bergen, Trondheim og Stavanger", "2012")
                .addPoints("EKG14", "Bergen, Trondheim og Stavanger", "2013")
                .addPoints("EKG14", "Bergen, Trondheim og Stavanger", "2014")
                .addPoints("EKG14", "Bergen, Trondheim og Stavanger", "2015")
                .addPoints("EKG14", "Bergen, Trondheim og Stavanger", "2016")
                .addPoints("EKG14", "Bergen, Trondheim og Stavanger", "2017")
                .addPoints("EKG15", "Oslo kommune", "2016")
                .addPoints("EKG15", "Oslo kommune", "2017")
                .build();

        bindings.put("ds1", ds1);
        bindings.put("ds2", dsCodeList2);
        bindings.put("ds3", dsCodeList3);

        VTLPrintStream out = new VTLPrintStream(System.out);
        engine.eval("" +
                "ds2r := [ds2]{rename code to kommune_nr, period to periode}" +
                "dsBoolean0 := [outer ds1, ds2r]{" +
                "   ds2_CONDITION := name is not null," +
                "   rename name to ds2_name," +
                "   kommune_nr_RESULTAT := ds2_CONDITION" +
                "}" +
                "ds3r := [ds3]{rename code to kostragruppe, period to periode}" +
                "dsBoolean1 := [outer ds1, ds3r]{" +
                "   ds3_CONDITION := name is not null," +
                "   rename name to ds3_name," +
                "   kostragruppe_RESULTAT := ds3_CONDITION" +
                "}" +
                "dsBoolean3 := [dsBoolean0, dsBoolean1]{" +
                "   filter true" +
                "}" +
                "ds4invalid := check(dsBoolean3, not_valid, measures, errorcode(\"TEST_ERROR_CODE\"))" +
                "ds4valid   := check(dsBoolean3, valid, measures)"
        );

        out.println("dsBoolean0");
        out.println(bindings.get("dsBoolean0"));
        out.println("dsBoolean1");
        out.println(bindings.get("dsBoolean1"));
        out.println("dsBoolean3");
        out.println(bindings.get("dsBoolean3"));
        out.println("ds4invalid");
        out.println(bindings.get("ds4invalid"));

        assertThat(bindings).containsKey("ds4invalid");
        assertThat(bindings).containsKey("ds4valid");

        Dataset ds3invalid = (Dataset) bindings.get("ds4invalid");
        Dataset ds3valid = (Dataset) bindings.get("ds4valid");

        //not checking for other measures and attributes since they are
        //not necessary for validation
        assertThat(ds3invalid.getDataStructure().getRoles()).contains(
                entry("kommune_nr", Component.Role.IDENTIFIER),
                entry("periode", Component.Role.IDENTIFIER),
                entry("kostragruppe", Component.Role.IDENTIFIER),
                entry("errorcode", Component.Role.ATTRIBUTE),
                entry("kommune_nr_RESULTAT", Component.Role.MEASURE),
                entry("kostragruppe_RESULTAT", Component.Role.MEASURE)
        );

        assertThat(ds3valid.getDataStructure().getRoles()).contains(
                entry("kommune_nr", Component.Role.IDENTIFIER),
                entry("periode", Component.Role.IDENTIFIER),
                entry("kostragruppe", Component.Role.IDENTIFIER),
                entry("errorcode", Component.Role.ATTRIBUTE),
                entry("kommune_nr_RESULTAT", Component.Role.MEASURE),
                entry("kostragruppe_RESULTAT", Component.Role.MEASURE)
        );

        // Should only contain the "non valid" rows.
        DataStructure ds3InvalidDataStruct = ds3invalid.getDataStructure();
        List<DataPoint> ds3InvalidDataPoints = ds3invalid.getData().collect(Collectors.toList());

        assertThat(ds3InvalidDataPoints).hasSize(3);

        //using for loop as data points come in random order
        Map<Component, VTLObject> map;
        for (DataPoint dp : ds3InvalidDataPoints) {
            map = ds3InvalidDataStruct.asMap(dp);

            if (map.get(ds3InvalidDataStruct.get("kommune_nr")).get().equals("0101")) {
                assertThat(map.get(ds3InvalidDataStruct.get("kommune_nr")).get()).isEqualTo("0101");
                assertThat(map.get(ds3InvalidDataStruct.get("periode")).get()).isEqualTo("2015");
                assertThat(map.get(ds3InvalidDataStruct.get("kostragruppe")).get()).isEqualTo("EKG15");
                assertThat(map.get(ds3InvalidDataStruct.get("errorcode")).get()).isEqualTo("TEST_ERROR_CODE");
                assertThat(map.get(ds3InvalidDataStruct.get("kommune_nr_RESULTAT")).get()).isEqualTo(true);
                assertThat(map.get(ds3InvalidDataStruct.get("kostragruppe_RESULTAT")).get()).isEqualTo(false);
            } else if (map.get(ds3InvalidDataStruct.get("kommune_nr")).get().equals("9000")) {
                assertThat(map.get(ds3InvalidDataStruct.get("kommune_nr")).get()).isEqualTo("9000");
                assertThat(map.get(ds3InvalidDataStruct.get("periode")).get()).isEqualTo("2014");
                assertThat(map.get(ds3InvalidDataStruct.get("kostragruppe")).get()).isEqualTo("EKG14");
                assertThat(map.get(ds3InvalidDataStruct.get("errorcode")).get()).isEqualTo("TEST_ERROR_CODE");
                assertThat(map.get(ds3InvalidDataStruct.get("kommune_nr_RESULTAT")).get()).isEqualTo(false);
                assertThat(map.get(ds3InvalidDataStruct.get("kostragruppe_RESULTAT")).get()).isEqualTo(true);
            } else if (map.get(ds3InvalidDataStruct.get("kommune_nr")).get().equals("0111")) {
                assertThat(map.get(ds3InvalidDataStruct.get("kommune_nr")).get()).isEqualTo("0111");
                assertThat(map.get(ds3InvalidDataStruct.get("periode")).get()).isEqualTo("2014");
                assertThat(map.get(ds3InvalidDataStruct.get("kostragruppe")).get()).isEqualTo("EKG14");
                assertThat(map.get(ds3InvalidDataStruct.get("errorcode")).get()).isEqualTo("TEST_ERROR_CODE");
                assertThat(map.get(ds3InvalidDataStruct.get("kommune_nr_RESULTAT")).get()).isEqualTo(false);
                assertThat(map.get(ds3InvalidDataStruct.get("kostragruppe_RESULTAT")).get()).isEqualTo(true);
            }
        }

        // Should only contain the "valid" rows.
        DataStructure ds3ValidDataStruct = ds3valid.getDataStructure();
        List<DataPoint> ds3ValidDataPoints = ds3valid.getData().collect(Collectors.toList());

        assertThat(ds3ValidDataPoints).hasSize(1);

        map = ds3ValidDataStruct.asMap(ds3ValidDataPoints.get(0));
        assertThat(map.get(ds3ValidDataStruct.get("kommune_nr")).get()).isEqualTo("0101");
        assertThat(map.get(ds3ValidDataStruct.get("periode")).get()).isEqualTo("2015");
        assertThat(map.get(ds3ValidDataStruct.get("kostragruppe")).get()).isEqualTo("EKG14");
        assertThat(map.get(ds3ValidDataStruct.get("errorcode")).get()).isNull();
        assertThat(map.get(ds3ValidDataStruct.get("kommune_nr_RESULTAT")).get()).isEqualTo(true);
        assertThat(map.get(ds3ValidDataStruct.get("kostragruppe_RESULTAT")).get()).isEqualTo(true);
    }

    @Test
    public void testNvlAsClause() throws Exception {

        Dataset ds1 = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.MEASURE, Long.class)
                .addComponent("m2", Role.MEASURE, String.class)

                .addPoints("1", 1L, null)
                .addPoints("2", null, "str2")
                .addPoints("3", null, null)
                .build();

        bindings.put("ds1", ds1);
        engine.eval("ds2 := [ds1] {" +
                "   m11 := nvl(m1 , 0), " +
                "   m22 := nvl(ds1.m2, \"constant\"), " +
                "   drop m1, m2 " +
                "}"
        );

        assertThat(bindings).containsKey("ds2");
        Dataset ds2 = (Dataset) bindings.get("ds2");

        assertThat(ds2.getDataStructure().getRoles()).containsOnly(
                entry("id1", Role.IDENTIFIER),
                entry("m11", Role.MEASURE),
                entry("m22", Role.MEASURE)
        );

        assertThat(ds2.getDataStructure().getTypes()).containsOnly(
                entry("id1", String.class), // TODO: Should be VTLString.
                entry("m11", Long.class),
                entry("m22", String.class)
        );

        assertThat(ds2.getData(Order.createDefault(ds2.getDataStructure())).get())
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        "1", 1L, "constant",
                        "2", 0L, "str2",
                        "3", 0L, "constant"
                );
    }

    @Test(expected = ScriptException.class)
    public void testNvlAsClauseNotEqualTypes() throws Exception {

        Dataset ds1 = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.MEASURE, Long.class)

                .addPoints("1", null)
                .build();

        bindings.put("ds1", ds1);
        engine.eval("ds2 := [ds1] {" +
                "   m11 := nvl(m1 , \"constant\") " +
                "}"
        );
    }

    @Test
    public void testDateFromStringAsClause() throws Exception {

        Dataset ds1 = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.MEASURE, String.class)
                .addPoints("1", "2017")
                .addPoints("2", null)
                .build();

        bindings.put("ds1", ds1);
        engine.eval("ds2 := [ds1] {" +
                "   m11 := date_from_string(m1, \"YYYY\"), " +
                "   drop m1 " +
                "}"
        );

        assertThat(bindings).containsKey("ds2");
        Dataset ds2 = (Dataset) bindings.get("ds2");

        assertThat(ds2.getDataStructure().getRoles()).containsOnly(
                entry("id1", Role.IDENTIFIER),
                entry("m11", Role.MEASURE)
        );

        assertThat(ds2.getDataStructure().getTypes()).containsOnly(
                entry("id1", String.class),
                entry("m11", Instant.class)
        );

        assertThat(ds2.getData())
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        "1", ZonedDateTime.of(2017, 1, 1, 0, 0, 0, 0, ZoneId.systemDefault()).toInstant(),
                        "2", null
                );

    }

    @Test(expected = ScriptException.class)
    public void testDateFromStringAsClauseUnsupportedFormat() throws Exception {
        engine.eval("test := date_from_string(\"string\", \"YYYYSN\")");
    }

    @Test(expected = ScriptException.class)
    public void testDateFromStringAsClauseInputNotStringType() throws Exception {
        engine.eval("test := date_from_string(123, \"YYYY\")");
    }

    @Test
    public void testAggregationAvg() throws ScriptException {
        StaticDataset ds = StaticDataset.create()
                .addComponent("id", Role.IDENTIFIER, String.class)
                .addComponent("sign", Role.IDENTIFIER, String.class)
                .addComponent("withnulls", Role.IDENTIFIER, Boolean.class)
                .addComponent("integerMeasure", Role.MEASURE, Long.class)
                .addComponent("floatMeasure", Role.MEASURE, Double.class)

                .addPoints("id", "pos", false, 0L, 0D)
                .addPoints("id", "pos", false, 1L, 1D)
                .addPoints("id", "pos", false, 2L, 2D)
                .addPoints("id", "pos", false, 4L, 4D)

                .addPoints("id", "neg", false, -0L, -0D)
                .addPoints("id", "neg", false, -1L, -1D)
                .addPoints("id", "neg", false, -2L, -2D)
                .addPoints("id", "neg", false, -4L, -4D)

                .addPoints("id", "pos", true, null, null)
                .addPoints("id", "pos", true, 1L, 1D)
                .addPoints("id", "pos", true, 2L, 2D)
                .addPoints("id", "pos", true, 4L, 4D)

                .addPoints("id", "neg", true, null, null)
                .addPoints("id", "neg", true, -1L, -1D)
                .addPoints("id", "neg", true, -2L, -2D)
                .addPoints("id", "neg", true, -4L, -4D)
                .build();

        bindings.put("ds", ds);
        engine.eval("result := avg(ds) along id");

        assertThat(bindings).containsKeys("result");
        Dataset result = (Dataset) bindings.get("result");
        assertThat(result.getData()).containsExactlyInAnyOrder(
                DataPoint.create("pos", false, 1.75D, 1.75D),
                DataPoint.create("pos", true, 2.3333333333333335, 2.3333333333333335),
                DataPoint.create("neg", false, -1.75D, -1.75D),
                DataPoint.create("neg", true, -2.3333333333333335, -2.3333333333333335)
        );

        assertThat(result.getData(Order.create(result.getDataStructure()).put("withnulls", Order.Direction.ASC).build()).get()).containsExactlyInAnyOrder(
                DataPoint.create("pos", false, 1.75D, 1.75D),
                DataPoint.create("neg", false, -1.75D, -1.75D),
                DataPoint.create("pos", true, 2.3333333333333335, 2.3333333333333335),
                DataPoint.create("neg", true, -2.3333333333333335, -2.3333333333333335)
        );

    }

    @Test
    public void testAggregationSumGroupBy() throws Exception {
        Dataset ds1 = mock(Dataset.class);
        DataStructure structure = DataStructure.of(
                "id1", Role.IDENTIFIER, Long.class,
                "id2", Role.IDENTIFIER, String.class,
                "m1", Role.MEASURE, Long.class,
                "m2", Role.MEASURE, Double.class,
                "at1", Role.ATTRIBUTE, String.class
        );
        when(ds1.getDataStructure()).thenReturn(structure);

        when(ds1.getData(any(Order.class))).thenReturn(Optional.empty());
        when(ds1.getData()).then(invocation -> Stream.of(
                (Map) ImmutableMap.of(
                        "id1", 1L,
                        "id2", "one",
                        "m1", 101L,
                        "m2", 1.1,
                        "at1", "attr1"
                ),
                ImmutableMap.of(
                        "id1", 1L,
                        "id2", "two",
                        "m1", 102L,
                        "m2", 1.1,
                        "at1", "attr2"
                ),
                ImmutableMap.of(
                        "id1", 2L,
                        "id2", "one",
                        "m1", 201L,
                        "m2", 1.1,
                        "at1", "attr2"
                ), ImmutableMap.of(
                        "id1", 2L,
                        "id2", "two",
                        "m1", 202L,
                        "m2", 1.1,
                        "at1", "attr2"
                ), ImmutableMap.of(
                        "id1", 2L,
                        "id2", "two-null",
                        "m1", VTLObject.NULL,
                        "m2", VTLObject.NULL,
                        "at1", "attr2"
                )
        ).map(structure::wrap));

        bindings.put("ds1", ds1);
        engine.eval("ds2 := sum(ds1.m1) group by id1");

        assertThat(bindings).containsKey("ds2");
        Dataset ds2 = (Dataset) bindings.get("ds2");

        assertThat(ds2.getDataStructure().getRoles()).containsOnly(
                entry("id1", Role.IDENTIFIER),
                entry("m1", Role.MEASURE)
        );

        assertThat(ds2.getDataStructure().getTypes()).containsOnly(
                entry("id1", Long.class),
                entry("m1", Long.class)
        );

        assertThat(ds2.getData()).flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        1L, 101L + 102L,
                        2L, 201L + 202L
                );
    }

    @Test
    public void testIfThenElse() throws ScriptException {
        StaticDataset dataset = StaticDataset.create()
                .addComponent("id", Role.IDENTIFIER, Long.class)
                .addComponent("m1", Role.MEASURE, Boolean.class)
                .addComponent("m2", Role.MEASURE, Boolean.class)
                .addPoints(1L, false, false)
                .addPoints(2L, false, true)
                .addPoints(3L, true, false)
                .addPoints(4L, true, false)
                .build();

        bindings.put("ds", dataset);
        engine.eval("result := [ds] { " +
                "   test1 := if m1 then 1 elseif m2 then 2 else null," +
                "   test2 := if m1 then 1 elseif m2 then null else 2," +
                "   test3 := if m1 then null elseif m2 then 2 else 3," +
                "   keep test1, test2, test3" +
                "}");

        Dataset result = (Dataset) bindings.get("result");
        assertThat(result).isNotNull();

        assertThat(result.getDataStructure().getTypes().values())
                .containsOnly(Long.class);

        assertThat(result.getData()).extracting(dataPoint -> Lists.transform(dataPoint, VTLObject::get))
                .containsExactlyInAnyOrder(
                        Arrays.asList(1L, null, 2L, 3L),
                        Arrays.asList(2L, 2L, null, 2L),
                        Arrays.asList(3L, 1L, 1L, null),
                        Arrays.asList(4L, 1L, 1L, null)
                );

    }

    @Test
    public void testAggregationSumAlong() throws Exception {
        Dataset ds1 = mock(Dataset.class);
        DataStructure structure = DataStructure.of(
                "id1", Role.IDENTIFIER, Long.class,
                "id2", Role.IDENTIFIER, String.class,
                "m1", Role.MEASURE, Long.class,
                "at1", Role.ATTRIBUTE, String.class
        );
        when(ds1.getDataStructure()).thenReturn(structure);

        when(ds1.getData(any(Order.class))).thenReturn(Optional.empty());
        when(ds1.getData()).then(invocation -> Stream.of(
                (Map) ImmutableMap.of(
                        "id1", 1L,
                        "id2", "one",
                        "m1", 101L,
                        "at1", "attr1"
                ),
                ImmutableMap.of(
                        "id1", 1L,
                        "id2", "two",
                        "m1", 102L,
                        "at1", "attr2"
                ),
                ImmutableMap.of(
                        "id1", 2L,
                        "id2", "one",
                        "m1", 201L,
                        "at1", "attr2"
                ), ImmutableMap.of(
                        "id1", 2L,
                        "id2", "two",
                        "m1", 202L,
                        "at1", "attr2"
                ),
                ImmutableMap.of(
                        "id1", 2L,
                        "id2", "two-null",
                        "m1", VTLObject.NULL,
                        "at1", "attr2"
                )
        ).map(structure::wrap));

        bindings.put("ds1", ds1);
        engine.eval("ds2 := sum(ds1) along id2");

        assertThat(bindings).containsKey("ds2");
        Dataset ds2 = (Dataset) bindings.get("ds2");

        assertThat(ds2.getDataStructure().getRoles()).containsOnly(
                entry("id1", Role.IDENTIFIER),
                entry("m1", Role.MEASURE)
        );

        assertThat(ds2.getDataStructure().getTypes()).containsOnly(
                entry("id1", Long.class),
                entry("m1", Long.class)
        );

        assertThat(ds2.getData()).flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        1L, 101L + 102L,
                        2L, 201L + 202L
                );
    }

    @Test
    public void testAggregationMultiple() throws Exception {
        Dataset ds1 = mock(Dataset.class);
        DataStructure structure = DataStructure.of(
                "id1", Role.IDENTIFIER, Long.class,
                "id2", Role.IDENTIFIER, String.class,
                "m1", Role.MEASURE, Long.class,
                "m2", Role.MEASURE, Double.class,
                "at1", Role.ATTRIBUTE, String.class
        );
        when(ds1.getDataStructure()).thenReturn(structure);

        when(ds1.getData(any(Order.class))).thenReturn(Optional.empty());
        when(ds1.getData()).then(invocation -> Stream.of(
                (Map) ImmutableMap.of(
                        "id1", 1L,
                        "id2", "one",
                        "m1", 101L,
                        "m2", 1.1d,
                        "at1", "attr1"
                ),
                ImmutableMap.of(
                        "id1", 1L,
                        "id2", "two",
                        "m1", 102L,
                        "m2", 1.2d,
                        "at1", "attr2"
                ),
                ImmutableMap.of(
                        "id1", 2L,
                        "id2", "one",
                        "m1", 201L,
                        "m2", 2.1d,
                        "at1", "attr2"
                ), ImmutableMap.of(
                        "id1", 2L,
                        "id2", "two",
                        "m1", 202L,
                        "m2", 2.2d,
                        "at1", "attr2"
                ), ImmutableMap.of(
                        "id1", 2L,
                        "id2", "two-null",
                        "m1", VTLObject.NULL,
                        "m2", VTLObject.NULL,
                        "at1", "attr2"
                )
        ).map(structure::wrap));

        bindings.put("ds1", ds1);
        engine.eval("ds2 := sum(ds1) group by id1");

        assertThat(bindings).containsKey("ds2");
        Dataset ds2 = (Dataset) bindings.get("ds2");

        assertThat(ds2.getDataStructure().getRoles()).containsOnly(
                entry("id1", Role.IDENTIFIER),
                entry("m1", Role.MEASURE),
                entry("m2", Role.MEASURE)
        );

        assertThat(ds2.getDataStructure().getTypes()).containsOnly(
                entry("id1", Long.class),
                entry("m1", Long.class),
                entry("m2", Double.class)
        );

        assertThat(ds2.getData()).flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        1L, 203L, 1.1d + 1.2d,
                        2L, 403L, 2.1d + 2.2d
                );
    }

    @Test
    public void testUnion() throws Exception {

        Dataset ds1 = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.MEASURE, Long.class)
                .addComponent("m2", Role.MEASURE, Double.class)
                .addComponent("at1", Role.MEASURE, String.class)

                .addPoints("1", 10L, 20D, "attr1-1")
                .addPoints("2", 100L, 200D, "attr1-2")
                .build();

        Dataset ds2 = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.MEASURE, Long.class)
                .addComponent("m2", Role.MEASURE, Double.class)
                .addComponent("at1", Role.MEASURE, String.class)

                .addPoints("3", 30L, 40D, "attr2-1")
                .addPoints("4", 300L, 400D, "attr2-2")
                .build();


        bindings.put("ds1", ds1);
        bindings.put("ds2", ds2);

        engine.eval("" +
                "ds3 := union(ds1, ds2)");

        assertThat(bindings).containsKey("ds3");
        assertThat(bindings.get("ds3")).isInstanceOf(Dataset.class);

        Dataset ds3 = (Dataset) bindings.get("ds3");
        assertThat(ds3.getDataStructure())
                .describedAs("data structure of d3")
                .containsOnlyKeys(
                        "id1",
                        "m1",
                        "m2",
                        "at1"
                );

        assertThat(ds3.getData())
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        "1", 10L, 20D, "attr1-1",
                        "2", 100L, 200D, "attr1-2",
                        "3", 30L, 40D, "attr2-1",
                        "4", 300L, 400D, "attr2-2"
                );
    }
}
