package no.ssb.vtl.script.operations.join;

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

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import no.ssb.vtl.connectors.Connector;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.FilteringSpecification;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.model.VTLString;
import no.ssb.vtl.script.VTLScriptEngine;
import no.ssb.vtl.script.expressions.LiteralExpression;
import no.ssb.vtl.script.expressions.MembershipExpression;
import no.ssb.vtl.script.expressions.VariableExpression;
import no.ssb.vtl.script.expressions.equality.EqualExpression;
import no.ssb.vtl.script.expressions.logic.OrExpression;
import no.ssb.vtl.script.operations.filter.FilterOperation;
import no.ssb.vtl.script.support.VTLPrintStream;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.Role.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

public class OuterJoinOperationTest extends RandomizedTest {

    private Connector connector = mock(Connector.class);
    private ScriptEngine engine = new VTLScriptEngine(connector);
    private Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);

    @Test
    @Seed("8144D952C87F27F0")
    public void testDebug() throws Exception {
        // Regression
        testRandomDatasets();
    }

    @Test
    @Repeat(iterations = 10)
    public void testRandomDatasets() throws Exception {

        // Build random test data.

        Integer datasetAmount = randomIntBetween(1, 10);
        Integer rowAmount = randomIntBetween(0, 20);
        Integer identifierAmount = randomIntBetween(0, 5);
        Integer componentAmount = randomIntBetween(1, 5);

        Map<String, Dataset> datasets = Maps.newLinkedHashMap();
        Map<String, List<DataPoint>> data = Maps.newLinkedHashMap();
        Map<String, DataStructure> dataStructures = Maps.newLinkedHashMap();

        // Creates random values.
        ImmutableMap<Class<?>, Function<Long, Object>> types = ImmutableMap.of(
                String.class, rowId -> randomAsciiOfLengthBetween(5, 10) + "-" + rowId,
                Double.class, Long::doubleValue,
                Long.class, Long::longValue
        );

        DataStructure.Builder identifiers = DataStructure.builder();
        List<Class<?>> typeList = types.keySet().asList();
        for (int i = 0; i < identifierAmount; i++) {
            identifiers.put("i-" + i, IDENTIFIER, randomFrom(typeList));
        }

        for (int i = 0; i < datasetAmount; i++) {
            String datasetName = "ds" + i;

            DataStructure.Builder dataStructureBuilder = DataStructure.builder();
            dataStructureBuilder.put("rowNum", IDENTIFIER, String.class);

            dataStructureBuilder.putAll(identifiers.build());
            for (int j = identifierAmount; j < identifierAmount + componentAmount; j++) {
                if (rarely()) {
                    dataStructureBuilder.put(datasetName + "-a-" + j, ATTRIBUTE, randomFrom(typeList));
                } else {
                    dataStructureBuilder.put(datasetName + "-m-" + j, MEASURE, randomFrom(typeList));
                }
            }
            DataStructure currentStructure = dataStructureBuilder.build();

            List<DataPoint> currentData = Lists.newArrayList();
            for (int j = 0; j < rowAmount; j++) {
                List<VTLObject> points = Lists.newArrayList();
                for (Component component : currentStructure.values()) {
                    Object value;
                    if (component.equals(currentStructure.get("rowNum"))) {
                        value = datasetName + "-row-" + j;
                    } else {
                        value = types.get(component.getType()).apply(Long.valueOf(j));
                    }
                    points.add(VTLObject.of(value));
                }
                currentData.add(tuple(points));
            }

            StaticDataset.ValueBuilder datasetBuilder = StaticDataset.create(currentStructure);
            currentData.forEach(datasetBuilder::addPoints);
            Dataset dataset = datasetBuilder.build();

            datasets.put(datasetName, dataset);
            dataStructures.put(datasetName, currentStructure);
            data.put(datasetName, currentData);
        }

        OuterJoinOperation result = new OuterJoinOperation(datasets);
        VTLPrintStream out = new VTLPrintStream(System.out);
        out.println(result.getDataStructure());
        out.println(result);
    }

    @Test
    public void testDefault() throws Exception {


        Dataset ds1 = StaticDataset.create()
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("id2", IDENTIFIER, String.class)
                .addComponent("id3", IDENTIFIER, String.class)
                .addComponent("value", MEASURE, String.class)
                .addPoints("1", "a", "id", "left 1a")
                .addPoints("2", "b", "id", "left 2b")
                .addPoints("3", "c", "id", "left 3c")
                .build();

        Dataset ds2 = StaticDataset.create()
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("id2", IDENTIFIER, String.class)
                .addComponent("id3", IDENTIFIER, String.class)
                .addComponent("value", MEASURE, String.class)
                .addPoints("2", "b", "id", "right 2b")
                .addPoints("3", "c", "id", "right 3c")
                .addPoints("4", "d", "id", "right 4d")
                .build();

        AbstractJoinOperation result = new OuterJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2));

        VTLPrintStream vtlPrintStream = new VTLPrintStream(System.out);
        //vtlPrintStream.println(result.getDataStructure());
        vtlPrintStream.println(ds1);
        vtlPrintStream.println(ds2);
        //vtlPrintStream.println(result.getDataStructure());
        vtlPrintStream.println(result);
        vtlPrintStream.println(result);

        // Check that the structure is correct. We expect:
        // common identifiers () + left and right.
        assertThat(result.getDataStructure().keySet())
                .containsOnly(
                        "id1",
                        "id2",
                        "id3",
                        "ds1_value",
                        "ds2_value"
                );

        assertThat(result.getData())
                .containsExactly(
                        DataPoint.create("1", "a", "id", "left 1a", null),
                        DataPoint.create("2", "b", "id", "left 2b", "right 2b"),
                        DataPoint.create("3", "c", "id", "left 3c", "right 3c"),
                        DataPoint.create("4", "d", "id", null, "right 4d")
                );
    }

    @Test
    public void testOuterJoin() throws Exception {

        DataStructure.Builder structure = DataStructure.builder()
                .put("id1", IDENTIFIER, Long.class)
                .put("value", MEASURE, String.class);

        Dataset ds1 = Stream.of(1, 1, 1, 2, 3, 5, 7, 7, 8, 8, 9, 9, 9, 10)
                .map(id -> DataPoint.create(id, "ds1 " + id))
                .collect(
                        () -> StaticDataset.create(structure),
                        StaticDataset.ValueBuilder::addPoints,
                        (valueBuilder, valueBuilder2) -> {
                        }
                ).build();

        Dataset ds2 = Stream.of(1, 3, 3, 3, 3, 4, 5, 6, 8, 9, 9, 9, 10)
                .map(id -> DataPoint.create(id, "ds2 " + id))
                .collect(
                        () -> StaticDataset.create(structure),
                        StaticDataset.ValueBuilder::addPoints,
                        (valueBuilder, valueBuilder2) -> {
                        }
                ).build();

        AbstractJoinOperation result = new OuterJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2));

        assertThat(result.getData()).containsExactly(
                DataPoint.create(1L, "ds1 1", "ds2 1"),
                DataPoint.create(1L, "ds1 1", "ds2 1"),
                DataPoint.create(1L, "ds1 1", "ds2 1"),
                DataPoint.create(2L, "ds1 2", null),
                DataPoint.create(3L, "ds1 3", "ds2 3"),
                DataPoint.create(3L, "ds1 3", "ds2 3"),
                DataPoint.create(3L, "ds1 3", "ds2 3"),
                DataPoint.create(3L, "ds1 3", "ds2 3"),
                DataPoint.create(4L, null, "ds2 4"),
                DataPoint.create(5L, "ds1 5", "ds2 5"),
                DataPoint.create(6L, null, "ds2 6"),
                DataPoint.create(7L, "ds1 7", null),
                DataPoint.create(7L, "ds1 7", null),
                DataPoint.create(8L, "ds1 8", "ds2 8"),
                DataPoint.create(8L, "ds1 8", "ds2 8"),
                DataPoint.create(9L, "ds1 9", "ds2 9"),
                DataPoint.create(9L, "ds1 9", "ds2 9"),
                DataPoint.create(9L, "ds1 9", "ds2 9"),
                DataPoint.create(9L, "ds1 9", "ds2 9"),
                DataPoint.create(9L, "ds1 9", "ds2 9"),
                DataPoint.create(9L, "ds1 9", "ds2 9"),
                DataPoint.create(9L, "ds1 9", "ds2 9"),
                DataPoint.create(9L, "ds1 9", "ds2 9"),
                DataPoint.create(9L, "ds1 9", "ds2 9"),
                DataPoint.create(10L, "ds1 10", "ds2 10")
        );

        VTLPrintStream vtlPrintStream = new VTLPrintStream(System.out);
        //vtlPrintStream.println(result.getDataStructure());
        vtlPrintStream.println(ds1);
        vtlPrintStream.println(ds2);
        //vtlPrintStream.println(result.getDataStructure());
        vtlPrintStream.println(result);

    }

    @Test
    public void testOuterJoinWithUnequalIds() throws Exception {

        DataStructure structure = DataStructure.of(
                "id1", IDENTIFIER, String.class,
                "value", MEASURE, String.class
        );


        Dataset ds1 = StaticDataset.create()
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("value", MEASURE, String.class)
                .addPoints("1", "left 1")
                .addPoints("2", "left 2")
                .addPoints("3", "left 3")
                .build();

        Dataset ds2 = StaticDataset.create()
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("value", MEASURE, String.class)
                .addComponent("id2", IDENTIFIER, String.class)
                .addPoints("2", "right 2", "b")
                .addPoints("2", "right 2e", "e")
                .addPoints("3", "right 3", "c")
                .addPoints("4", "right 4", "d")
                .build();


        AbstractJoinOperation result = new OuterJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2));

        VTLPrintStream vtlPrintStream = new VTLPrintStream(System.out);
        //vtlPrintStream.println(result.getDataStructure());
        vtlPrintStream.println(ds1);
        vtlPrintStream.println(ds2);
        //vtlPrintStream.println(result.getDataStructure());
        vtlPrintStream.println(result);

        // Check that the structure is correct. We expect:
        // common identifiers () + left and right.
        assertThat(result.getDataStructure().keySet())
                .containsExactly(
                        "id1",
                        "ds1_value",
                        "ds2_value",
                        "id2"
                );

        assertThat(result.getData())
                .containsExactly(
                        DataPoint.create("1", "left 1", null, null),
                        DataPoint.create("2", "left 2", "right 2", "b"),
                        DataPoint.create("2", "left 2", "right 2e", "e"),
                        DataPoint.create("3", "left 3", "right 3", "c"),
                        DataPoint.create("4", null, "right 4", "d")
                );
    }

    @Test
    public void testFilterPropagation() throws ScriptException {
        Dataset ds1 = StaticDataset.create()
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("id2", IDENTIFIER, String.class)
                .addComponent("id3", IDENTIFIER, String.class)
                .addComponent("value", MEASURE, String.class)
                .addPoints("1", "a", "id", "left 1a")
                .addPoints("2", "b", "id", "left 2b")
                .addPoints("3", "c", "id", "left 3c")
                .build();

        Dataset ds2 = StaticDataset.create()
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("id2", IDENTIFIER, String.class)
                .addComponent("id3", IDENTIFIER, String.class)
                .addComponent("value", MEASURE, String.class)
                .addPoints("2", "b", "id", "right 2b")
                .addPoints("3", "c", "id", "right 3c")
                .addPoints("4", "d", "id", "right 4d")
                .build();

        // This shows the expression and expected result
        bindings.put("ds1", ds1);
        bindings.put("ds2", ds2);
        engine.eval("result := [outer ds1,ds2] {\n" +
                "  filter id2 = \"2\" or ds1.value = \"left 2b\"\n" +
                "}");
        Dataset result = (Dataset) bindings.get("result");
        assertThat(result.getData()).containsExactlyInAnyOrder(
                DataPoint.create("2", "b", "id", "left 2b", "right 2b")
        );

        // Now creating the same expression via nested operations
        OuterJoinOperation outerJoinOperation = new OuterJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2));

        FilterOperation filterOperation = new FilterOperation(outerJoinOperation, new OrExpression(
                new EqualExpression(
                    new VariableExpression(VTLString.class, "id2"),
                    new LiteralExpression(VTLString.of("2"))),
                new EqualExpression(
                        new MembershipExpression(VTLString.class, "ds1", "value"),
                        new LiteralExpression(VTLString.of("left 2b")))),
                outerJoinOperation.getJoinScope());

        FilteringSpecification renamedFilter = filterOperation.computeRequiredFiltering(Filtering.ALL);
        assertThat(renamedFilter.toString()).isEqualTo("(id2=2|ds1_value=left 2b)");

        // Check filter propagation for ds1
        Filtering ds1Filter = outerJoinOperation.renameFilterColumns(renamedFilter, "ds1");
        assertThat(ds1Filter.toString()).isEqualTo("(id2=2|value=left 2b)");
        assertThat(outerJoinOperation.computeDatasetFiltering(ds1, ds1Filter).toString())
                .isEqualTo("(id2=2|value=left 2b)");

        // Check filter propagation for ds2
        Filtering ds2Filter = outerJoinOperation.renameFilterColumns(renamedFilter, "ds2");
        assertThat(ds2Filter.toString()).isEqualTo("(id2=2|ds1_value=left 2b)");
        assertThat(outerJoinOperation.computeDatasetFiltering(ds2, ds2Filter).toString()).isEqualTo("TRUE");

        // Should be the same result as above
        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            Stream<DataPoint> stream = filterOperation.computeData(Ordering.ANY, Filtering.ALL,
                    filterOperation.getDataStructure().keySet());
            softly.assertThat(stream).containsExactly(
                    DataPoint.create("2", "b", "id", "left 2b", "right 2b")
           );
            stream.close();
        }
    }

    private DataPoint tuple(VTLObject... components) {
        return DataPoint.create(Arrays.asList(components));
    }

    private DataPoint tuple(Object... objects) {
        return tuple(Arrays.stream(objects).map(VTLObject::of).collect(Collectors.toList()));
    }

    private DataPoint tuple(List<VTLObject> components) {
        return DataPoint.create(components);
    }
}
