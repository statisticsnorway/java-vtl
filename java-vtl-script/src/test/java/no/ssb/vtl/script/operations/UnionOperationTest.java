package no.ssb.vtl.script.operations;

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.script.error.VTLRuntimeException;
import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.Role;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class UnionOperationTest {

    private DataStructure dataStructure;


    @Before
    public void setUp() throws Exception {
        dataStructure = DataStructure.of(
                "TIME", Role.IDENTIFIER, String.class,
                "GEO", Role.IDENTIFIER, String.class,
                "POP", Role.MEASURE, Long.class
        );
    }

    @Test
    public void testOneDatasetReturnedUnchanged() throws Exception {

        Dataset dataset = StaticDataset.create().build();
        Stream<DataPoint> stream = dataset.getData();

        Dataset spyDataset = spy(dataset);
        doReturn(stream).when(spyDataset).getData();
        doReturn(Optional.of(stream)).when(spyDataset).getData(any(), any(), any());

        UnionOperation operator = new UnionOperation(spyDataset);

        assertThat(operator.getData())
                .as("result of union operation")
                .isSameAs(stream);

        assertThat(operator.getData(Order.createDefault(dataset.getDataStructure())).get())
                .as("result of union operation")
                .isSameAs(stream);

    }


    @Test
    public void testSameIdentifierAndMeasures() throws Exception {

        SoftAssertions softly = new SoftAssertions();
        try {
            Dataset dataset1 = mock(Dataset.class);
            Dataset dataset2 = mock(Dataset.class);
            Dataset dataset3 = mock(Dataset.class);

            when(dataset1.getDataStructure()).thenReturn(dataStructure);
            when(dataset2.getDataStructure()).thenReturn(dataStructure);
            when(dataset3.getDataStructure()).thenReturn(dataStructure);

            UnionOperation unionOperation = new UnionOperation(dataset1, dataset2, dataset3);
            softly.assertThat(unionOperation).isNotNull();

            DataStructure wrongStructure = DataStructure.of(
                    "TIME2", Role.IDENTIFIER, String.class,
                    "GEO2", Role.IDENTIFIER, String.class,
                    "POP2", Role.MEASURE, Long.class
            );

            Dataset wrongDataset = mock(Dataset.class);
            when(wrongDataset.getDataStructure()).thenReturn(wrongStructure);
            Throwable expextedEx = null;
            try {
                UnionOperation operation = new UnionOperation(dataset1, wrongDataset, dataset2, dataset3);
                operation.computeDataStructure();
            } catch (Throwable t) {
                expextedEx = t;
            }
            softly.assertThat(expextedEx).isNotNull().hasMessageContaining("incompatible");

        } finally {
            softly.assertAll();
        }
    }

    @Test
    public void testUnionSorted() throws Exception {
        DataStructure structure = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, String.class)
                .put("id2", Role.IDENTIFIER, String.class)
                .put("me1", Role.MEASURE, String.class)
                .build();

        Dataset ds1 = StaticDataset.create(structure)
                .addPoints("1-1", "2-1", "1")
                .addPoints("1-2", "2-2", "1")
                .addPoints("1-4", "2-4", "1")
                .build();

        Dataset ds2 = StaticDataset.create(structure)
                .addPoints("1-6", "2-6", "1")
                .addPoints("1-8", "2-8", "1")
                .addPoints("1-9", "2-9", "1")
                .build();

        Dataset ds3 = StaticDataset.create(structure)
                .addPoints("1-3", "2-3", "1")
                .addPoints("1-5", "2-5", "1")
                .addPoints("1-7", "2-7", "1")
                .build();

        Dataset empty = StaticDataset.create(structure)
                .build();

        Order order = Order.createDefault(dataStructure);

        UnionOperation unionOperation = new UnionOperation(ds1, ds2, ds3, empty);

        Optional<Stream<DataPoint>> stream = unionOperation.getData(order);

        assertThat(Optional.of(stream)).isNotEmpty();

        ImmutableList<DataPoint> collect = stream.get().collect(ImmutableList.toImmutableList());

        assertThat(collect).containsExactlyElementsOf(
                ImmutableList.sortedCopyOf(order,
                        Stream.of(
                                ds1.getData(),
                                ds2.getData(),
                                ds3.getData(),
                                empty.getData()
                        ).flatMap(x -> x).collect(ImmutableList.toImmutableList())
                ));

    }

    @Test
    public void testUnion() throws Exception {

        // Example 1 of the operator specification

        Dataset totalPopulation1 = new TestableDataset(Lists.newArrayList(
                dataPoint("2012", "Belgium", 5L),
                dataPoint("2012", "Greece", 2L),
                dataPoint("2012", "France", 3L),
                dataPoint("2012", "Malta", 7L),
                dataPoint("2012", "Finland", 9L),
                dataPoint("2012", "Switzerland", 12L)
        ), dataStructure);

        Dataset totalPopulation2 = new TestableDataset(Lists.newArrayList(
                dataPoint("2012", "Netherlands", 23L),
                dataPoint("2012", "Spain", 5L),
                dataPoint("2012", "Iceland", 1L)
        ), dataStructure);

        Dataset resultDataset = new UnionOperation(totalPopulation1, totalPopulation2);
        assertThat(resultDataset).isNotNull();

        assertThat(resultDataset.getDataStructure()).isEqualTo(dataStructure);

        Stream<DataPoint> stream = resultDataset.getData();
        assertThat(stream).isNotNull();

        assertThat(stream)
                .containsExactlyInAnyOrder(
                        dataPoint("2012", "Belgium", 5L),
                        dataPoint("2012", "Greece", 2L),
                        dataPoint("2012", "France", 3L),
                        dataPoint("2012", "Malta", 7L),
                        dataPoint("2012", "Finland", 9L),
                        dataPoint("2012", "Switzerland", 12L),
                        dataPoint("2012", "Netherlands", 23L),
                        dataPoint("2012", "Spain", 5L),
                        dataPoint("2012", "Iceland", 1L)
                );
    }

    @Test
    public void testUnionWithOneDifferingComponent() throws Exception {

        // Example 2 of the operator specification.

        Dataset totalPopulation1 = StaticDataset.create()
                .addComponent("TIME", Role.IDENTIFIER, String.class)
                .addComponent("GEO", Role.IDENTIFIER, String.class)
                .addComponent("POP", Role.MEASURE, Long.class)
                .addPoints("2012", "Belgium", 1L)
                .addPoints("2012", "Greece", 2L)
                .addPoints("2012", "France", 3L)
                .addPoints("2012", "Malta", 4L)
                .addPoints("2012", "Finland", 5L)
                .addPoints("2012", "Switzerland", 6L)
                .build();

        Dataset totalPopulation2 = StaticDataset.create()
                .addComponent("TIME", Role.IDENTIFIER, String.class)
                .addComponent("GEO", Role.IDENTIFIER, String.class)
                .addComponent("POP", Role.MEASURE, Long.class)
                .addPoints("2011", "Belgium", 10L)
                .addPoints("2011", "Greece", 20L)
                .addPoints("2011", "France", 30L)
                .addPoints("2011", "Malta", 40L)
                .addPoints("2011", "Finland", 50L)
                .addPoints("2011", "Switzerland", 60L)
                .build();

        Dataset resultDataset = new UnionOperation(totalPopulation1, totalPopulation2);
        assertThat(resultDataset).isNotNull();

        Stream<DataPoint> stream = resultDataset.getData();
        assertThat(stream).isNotNull();

        assertThat(stream)
                .contains(
                        dataPoint("2012", "Belgium", 1L),
                        dataPoint("2012", "Greece", 2L),
                        dataPoint("2012", "France", 3L),
                        dataPoint("2012", "Malta", 4L),
                        dataPoint("2012", "Finland", 5L),
                        dataPoint("2012", "Switzerland", 6L),
                        dataPoint("2011", "Belgium", 10L),
                        dataPoint("2011", "Greece", 20L),
                        dataPoint("2011", "France", 30L),
                        dataPoint("2011", "Malta", 40L),
                        dataPoint("2011", "Finland", 50L),
                        dataPoint("2011", "Switzerland", 60L)
                );

    }

    @Test(expected = VTLRuntimeException.class)
    public void testUnionWithDuplicate() throws Exception {

        Dataset totalPopulation1 = new TestableDataset(Lists.newArrayList(
                dataPoint("2012", "Greece", 2L),
                dataPoint("2012", "France", 3L),
                dataPoint("2012", "Malta", 4L)
        ), dataStructure);

        Dataset totalPopulation2 = new TestableDataset(Lists.newArrayList(
                dataPoint("2012", "Belgium", 1L),
                dataPoint("2012", "Greece", 2L),
                dataPoint("2012", "France", 3L),
                dataPoint("2012", "Malta", 4L)
        ), dataStructure);

        Dataset resultDataset = new UnionOperation(totalPopulation1, totalPopulation2);
        assertThat(resultDataset).isNotNull();

        Stream<DataPoint> stream = resultDataset.getData();
        fail("UnionOperation with duplicates did not throw exception as expected but returned: " +
                stream.map(dataPoint -> "[" + dataPoint.toString() + "]").collect(Collectors.joining(", ")));
    }

    private DataPoint dataPoint(Object... objects) {
        List<VTLObject> vtlObjects = Stream.of(objects).map(VTLObject::of).collect(Collectors.toList());
        return DataPoint.create(vtlObjects);
    }

    private final class TestableDataset implements Dataset {

        private final List<DataPoint> data;
        private final DataStructure dataStructure;

        protected TestableDataset(List<DataPoint> data, DataStructure dataStructure) {
            this.data = data;
            this.dataStructure = dataStructure;
        }

        @Override
        public Stream<DataPoint> getData() {
            return data.stream();
        }

        @Override
        public Optional<Map<String, Integer>> getDistinctValuesCount() {
            return Optional.empty();
        }

        @Override
        public Optional<Long> getSize() {
            return Optional.of((long) data.size());
        }

        @Override
        public DataStructure getDataStructure() {
            return dataStructure;
        }

    }
}
