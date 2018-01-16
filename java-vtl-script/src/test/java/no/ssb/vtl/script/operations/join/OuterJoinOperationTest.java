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
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLNumber;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.script.support.VTLPrintStream;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static no.ssb.vtl.model.Component.Role.ATTRIBUTE;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OuterJoinOperationTest extends RandomizedTest {

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

            Dataset dataset = mock(Dataset.class, datasetName);
            datasets.put(datasetName, dataset);
            dataStructures.put(datasetName, currentStructure);
            when(dataset.getDataStructure()).thenReturn(currentStructure);
            data.put(datasetName, currentData);
            when(dataset.getData()).thenAnswer(o -> currentData.stream());
            when(dataset.getData(any(Order.class))).thenReturn(Optional.empty());
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
                .addPoints("4", "d", "id", "left 4d")
                .addPoints("5", "e", "id", "left 5e")
                .build();
        DataStructure structure1 = ds1.getDataStructure();

        Dataset ds2 = StaticDataset.create()
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("id2", IDENTIFIER, String.class)
                .addComponent("id3", IDENTIFIER, String.class)
                .addComponent("value", MEASURE, String.class)
                .addPoints("4", "d", "id", "right 4d")
                .addPoints("5", "e", "id", "right 5e")
                .addPoints("6", "f", "id", "right 6f")
                .addPoints("7", "g", "id", "right 7g")
                .addPoints("8", "h", "id", "right 8h")
                .build();
        DataStructure structure2 = ds2.getDataStructure();

        AbstractJoinOperation result = new OuterJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2));

        VTLPrintStream vtlPrintStream = new VTLPrintStream(System.out);
        //vtlPrintStream.println(result.getDataStructure());
        vtlPrintStream.println("ds1:");
        vtlPrintStream.println(ds1);
        vtlPrintStream.println("ds2:");
        vtlPrintStream.println(ds2);
        //vtlPrintStream.println(result.getDataStructure());
        vtlPrintStream.println("result:");
        vtlPrintStream.println(result);

        // Check that the structure is correct. We expect:
        // common identifiers () + left and right.
        assertThat(result.getDataStructure())
                .containsOnly(
                        entry("id1", structure1.get("id1")),
                        entry("id2", structure1.get("id2")),
                        entry("id3", structure1.get("id3")),
                        entry("ds1_value", structure1.get("value")),
                        entry("ds2_value", structure2.get("value"))
                );

        Order.Builder order = Order.create(result.getDataStructure())
                .put("id1", Order.Direction.ASC)
                .put("id2", Order.Direction.DESC);
        assertThat(result.getData(order.build()).get()).isSortedAccordingTo(order.build());

        assertThat(result.getData())
                .extracting(input -> input.stream().map(VTLObject::get).collect(Collectors.toList()))
                .containsExactly(
                        asList("1", "a", "id", "left 1a", null),
                        asList("2", "b", "id", "left 2b", null),
                        asList("3", "c", "id", "left 3c", null),
                        asList("4", "d", "id", "left 4d", "right 4d"),
                        asList("5", "e", "id", "left 5e", "right 5e"),
                        asList("6", "f", "id", null, "right 6f"),
                        asList("7", "g", "id", null, "right 7g"),
                        asList("8", "h", "id", null, "right 8h")
                );
    }

    @Test
    public void testOuterJoin() throws Exception {


        StaticDataset.StructureBuilder ds1Builder = StaticDataset.create()
                .addComponent("id1", IDENTIFIER, Long.class)
                .addComponent("value", MEASURE, String.class);
        Stream.of(1L, 1L, 1L, 2L, 3L, 5L, 7L, 7L, 8L, 8L, 9L, 9L, 9L, 10L)
                .map(id -> Lists.newArrayList(id, "ds1 " + id))
                .forEach(p -> ds1Builder.addPoints(p.toArray()));
        StaticDataset ds1 = ds1Builder.build();

        Dataset ds2 = mock(Dataset.class, "ds2");

        DataStructure structure2 = DataStructure.of(
                "id1", IDENTIFIER, Long.class,
                "value", MEASURE, String.class
        );

        given(ds2.getDataStructure()).willReturn(structure2);
        given(ds2.getData()).willAnswer(o -> Stream.of(1, 3, 3, 3, 3, 4, 5, 6, 8, 9, 9, 9, 10)
                .map(id -> Lists.newArrayList(
                        VTLNumber.of(id), VTLObject.of("ds2 " + id)
                ))
                .map(DataPoint::create));
        given(ds2.getData(any(Order.class))).willReturn(Optional.empty());

        AbstractJoinOperation result = new OuterJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2));

        VTLPrintStream vtlPrintStream = new VTLPrintStream(System.out);
        //vtlPrintStream.println(result.getDataStructure());
        vtlPrintStream.println(ds1);
        vtlPrintStream.println(ds2);
        //vtlPrintStream.println(result.getDataStructure());
        vtlPrintStream.println(result);

    }

    @Test
    public void testOuterJoinWithUnequalIds() throws Exception {


        Dataset ds1 = mock(Dataset.class, "ds1");
        Dataset ds2 = mock(Dataset.class, "ds2");

        DataStructure structure1 = DataStructure.of(
                "id1", IDENTIFIER, String.class,
                "value", MEASURE, String.class
        );

        DataStructure structure2 = DataStructure.of(
                "id1", IDENTIFIER, String.class,
                "value", MEASURE, String.class,
                "id2", IDENTIFIER, String.class
        );

        given(ds1.getDataStructure()).willReturn(structure1);
        given(ds2.getDataStructure()).willReturn(structure2);

        given(ds1.getData()).willAnswer(o -> Stream.of(
                tuple("1", "left 1"),
                tuple("2", "left 2"),
                tuple("3", "left 3")
        ));
        given(ds1.getData(any(Order.class))).willReturn(Optional.empty());

        given(ds2.getData()).willAnswer(o -> Stream.of(
                tuple("2", "right 2", "b"),
                tuple("2","right 2e", "e"),
                tuple("3", "right 3", "c"),
                tuple("4", "right 4", "d")
        ));
        given(ds2.getData(any(Order.class))).willReturn(Optional.empty());

        AbstractJoinOperation result = new OuterJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2));

        VTLPrintStream vtlPrintStream = new VTLPrintStream(System.out);
        //vtlPrintStream.println(result.getDataStructure());
        vtlPrintStream.println(ds1);
        vtlPrintStream.println(ds2);
        //vtlPrintStream.println(result.getDataStructure());
        vtlPrintStream.println(result);

        // Check that the structure is correct. We expect:
        // common identifiers () + left and right.
        assertThat(result.getDataStructure())
                .containsOnly(
                        entry("id1", structure1.get("id1")),
                        entry("ds1_value", structure1.get("value")),
                        entry("ds2_value", structure2.get("value")),
                        entry("id2", structure2.get("id2"))
                );

        assertThat(result.getData())
                .extracting(input -> input.stream().map(VTLObject::get).collect(Collectors.toList()))
                .containsExactly(
                        asList("1", "left 1", null, null),
                        asList("2", "left 2", "right 2", "b"),
                        asList("2", "left 2", "right 2e", "e"),
                        asList("3", "left 3", "right 3", "c"),
                        asList("4", null, "right 4", "d")
                );
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
