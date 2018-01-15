package no.ssb.vtl.model;

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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import no.ssb.vtl.model.Order.Direction;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.*;
import static no.ssb.vtl.model.Component.Role.*;

public class DatasetTest extends RandomizedTest {

    private final DataStructure structure = DataStructure.builder()
            .put("id1", IDENTIFIER, Long.class)
            .put("id2", IDENTIFIER, Long.class)
            .put("id3", IDENTIFIER, Long.class)
            .put("me1", MEASURE, String.class)
            .put("me2", MEASURE, String.class)
            .put("me3", MEASURE, String.class)
            .put("at1", MEASURE, String.class)
            .put("at2", MEASURE, String.class)
            .put("at3", MEASURE, String.class)
            .build();

    @Test
    @Repeat(iterations = 10)
    public void testDefaultSorting() throws Exception {
        // Checks that default implementation sorts the streams.

        DataStructure dataStructure = randomDataStructure();
        TestableDataset dataset = randomDataset(dataStructure, randomIntBetween(10, 100));
        Order.Builder order = randomOrder(dataStructure);

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {

            Optional<Stream<DataPoint>> sortedStream = dataset.getData(order.build());
            softly.assertThat(sortedStream).as("Sorted stream").isPresent();

            List<DataPoint> sorted = sortedStream.get().collect(toList());
            softly.assertThat(sorted).as("Sorted stream").isSortedAccordingTo(order.build());

            List<DataPoint> unsorted = dataset.getData().collect(toList());
            softly.assertThat(unsorted).as("Unsorted stream")
                    .containsAll(sorted);
        }
    }

    private Order.Builder randomOrder(DataStructure structure) {
        int sortedColumns = RandomizedTest.randomIntBetween(1, structure.size());
        ArrayList<Map.Entry<String, Component>> structureEntries = Lists.newArrayList(structure.entrySet());

        Order.Builder order = Order.create(structure);
        for (int i = 0; i < sortedColumns; i++) {
            Map.Entry<String, Component> entry = RandomizedTest.randomFrom(structureEntries);
            structureEntries.remove(entry);
            order.put(
                    entry.getKey(),
                    Direction.ASC
            );
        }
        return order;
    }

    private TestableDataset randomDataset(DataStructure structure, int size) {
        final Integer[] idSize = {size};
        Map<Component, Integer> indexesIds = structure.values().stream()
                .filter(Component::isIdentifier)
                .collect(Collectors.toMap(o -> o, o -> {
                    Integer copy = idSize[0];
                    idSize[0] = idSize[0] / 2;
                    return copy;
                }));

        ArrayList<DataPoint> data = Lists.newArrayListWithCapacity(size);
        for (int i = 0; i < size; i++) {
            ImmutableMap.Builder<String, Object> row = ImmutableMap.builder();
            for (Map.Entry<String, Component> column : structure.entrySet()) {
                if (indexesIds.containsKey(column.getValue())) {
                    row.put(column.getKey(), randomIntBetween(0, indexesIds.get(column.getValue())));
                } else {
                    row.put(column.getKey(), "Value " + i + " for " + column.getKey());
                }
            }
            data.add(structure.wrap(row.build()));
        }

        Collections.shuffle(data, new Random(RandomizedTest.randomLong()));

        return new TestableDataset(data, structure);
    }

    private DataStructure randomDataStructure() {
        List<Map.Entry<String, Component>> entries = Lists.newArrayList(structure.entrySet());
        Collections.shuffle(entries, new Random(RandomizedTest.randomLong()));
        return DataStructure.builder().putAll(entries).build();
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
        public Optional<Stream<DataPoint>> getData(Order orders, Filtering filtering, Set<String> components) {
            return Optional.of(getData().sorted(orders).filter(filtering).map(o -> {
                // TODO
                return o;
            }));
        }

        @Override
        public DataStructure getDataStructure() {
            return dataStructure;
        }

    }
}
