package no.ssb.vtl.script.operations.fold;

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
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VtlFiltering;
import no.ssb.vtl.model.VtlOrdering;
import no.ssb.vtl.script.operations.AbstractDatasetOperation;
import no.ssb.vtl.script.support.DatasetCloseWatcher;
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.PrintStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static no.ssb.vtl.model.Component.Role.ATTRIBUTE;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;
import static no.ssb.vtl.model.VtlFiltering.and;
import static no.ssb.vtl.model.VtlFiltering.eq;
import static no.ssb.vtl.model.VtlFiltering.ge;
import static no.ssb.vtl.model.VtlFiltering.lt;
import static org.assertj.core.api.Assertions.assertThat;

public class FoldOperationTest extends RandomizedTest {

    private static DataPoint tuple(DataStructure structure, Object... values) {
        checkArgument(values.length == structure.size());
        Map<String, Object> map = Maps.newLinkedHashMap();
        Iterator<Object> iterator = Lists.newArrayList(values).iterator();
        for (String name : structure.keySet()) {
            map.put(name, iterator.next());
        }
        return structure.wrap(map);
    }

    @Test
    public void testArguments() throws Exception {

        String validDimensionReference = "aDimension";
        String validMeasureReference = "aDimension";

        DataStructure structure = DataStructure.of(
                "element1", MEASURE, String.class,
                "element2", MEASURE, String.class
        );

        Set<String> validElements = Sets.newHashSet(structure.keySet());
        Dataset dataset = Mockito.mock(Dataset.class);

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {

            softly.assertThatThrownBy(() -> new FoldOperation(null, validDimensionReference, validMeasureReference, validElements))
                    .isInstanceOf(NullPointerException.class);

            softly.assertThatThrownBy(() -> new FoldOperation(dataset, null, validMeasureReference, validElements))
                    .isInstanceOf(NullPointerException.class);

            softly.assertThatThrownBy(() -> new FoldOperation(dataset, validDimensionReference, null, validElements))
                    .isInstanceOf(NullPointerException.class);

            softly.assertThatThrownBy(() -> new FoldOperation(dataset, validDimensionReference, validMeasureReference, null))
                    .isInstanceOf(NullPointerException.class);

            softly.assertThatThrownBy(() -> new FoldOperation(dataset, "", validMeasureReference, validElements))
                    .isInstanceOf(IllegalArgumentException.class);

            softly.assertThatThrownBy(() -> new FoldOperation(dataset, validDimensionReference, "", validElements))
                    .isInstanceOf(IllegalArgumentException.class);

            softly.assertThatThrownBy(() -> new FoldOperation(dataset, validDimensionReference, validMeasureReference, Collections.emptySet()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    public void testOrderingIncludesIdentifier() {

        // Fold supports
        Dataset dataset = StaticDataset.create()
                .addComponent("year", IDENTIFIER, String.class)
                .addComponent("country", IDENTIFIER, String.class)
                .addComponent("pop", MEASURE, Long.class)
                .addComponent("death", MEASURE, Long.class)
                .addComponent("birth", MEASURE, Long.class)
                .addPoints("2000", "norway", 1L, 2L, 4L)
                .addPoints("2000", "sweden", 8L, 16L, 32L)
                .addPoints("2001", "norway", 64L, 128L, 256L)
                .addPoints("2001", "sweden", 512L, 1024L, 2048L)
                .build();

        FoldOperation fold = new FoldOperation(
                new DebugDataset(dataset, System.err),
                "type",
                "value",
                ImmutableSet.of("pop", "death", "birth")
        );

        DebugDataset result = new DebugDataset(new OperationDataset(fold), System.err);

        VtlFiltering filtering = VtlFiltering.using(fold)
                .or(
                        and(eq("country", "sweden"), eq("year", "2000")),
                        and(ge("value", 256L), lt("value", 1024L))
                )
                .build();


        assertThat(result.getData(
                VtlOrdering.using(fold).desc("year", "country").build(),
                Filtering.ALL,
                fold.getDataStructure().keySet()
        ).get()).containsExactly(
                DataPoint.create("2001", "sweden", "pop", 512),
                DataPoint.create("2001", "sweden", "death", 1024),
                DataPoint.create("2001", "sweden", "birth", 2048),
                DataPoint.create("2001", "norway", "pop", 64),
                DataPoint.create("2001", "norway", "death", 128),
                DataPoint.create("2001", "norway", "birth", 256),
                DataPoint.create("2000", "sweden", "pop", 8),
                DataPoint.create("2000", "sweden", "death", 16),
                DataPoint.create("2000", "sweden", "birth", 32),
                DataPoint.create("2000", "norway", "pop", 1),
                DataPoint.create("2000", "norway", "death", 2),
                DataPoint.create("2000", "norway", "birth", 4)
        );

        assertThat(result.getData(
                VtlOrdering.using(fold).desc("year", "country").asc("type").build(),
                Filtering.ALL,
                fold.getDataStructure().keySet()
        ).get()).containsExactly(
                DataPoint.create("2001", "sweden", "birth", 2048),
                DataPoint.create("2001", "sweden", "death", 1024),
                DataPoint.create("2001", "sweden", "pop", 512),
                DataPoint.create("2001", "norway", "birth", 256),
                DataPoint.create("2001", "norway", "death", 128),
                DataPoint.create("2001", "norway", "pop", 64),
                DataPoint.create("2000", "sweden", "birth", 32),
                DataPoint.create("2000", "sweden", "death", 16),
                DataPoint.create("2000", "sweden", "pop", 8),
                DataPoint.create("2000", "norway", "birth", 4),
                DataPoint.create("2000", "norway", "death", 2),
                DataPoint.create("2000", "norway", "pop", 1)
        );

    }

    @Test
    public void testConstraint() throws Exception {

        Dataset dataset = Mockito.mock(Dataset.class);
        DataStructure structure = DataStructure.of(
                "id1", IDENTIFIER, String.class,
                "id2", IDENTIFIER, String.class,
                "m1", MEASURE, String.class,
                "m2", MEASURE, String.class,
                "m3", MEASURE, String.class
        );
        Dataset invalidDataset = Mockito.mock(Dataset.class);
        DataStructure wrongTypesDataset = DataStructure.of(
                "id1", IDENTIFIER, String.class,
                "id2", IDENTIFIER, String.class,
                "m1", MEASURE, Number.class,
                "m2", MEASURE, String.class,
                "m3", MEASURE, Instant.class
        );

        Set<String> validElements = Sets.newHashSet(
                "m1",
                "m2",
                "m3"
        );

        Set<String> invalidElements = Sets.newHashSet(
                "m1",
                "m2",
                "m3",
                "m4"
        );

        Set<String> wrongTypesElements = Sets.newHashSet(
                wrongTypesDataset.keySet()
        );

        Mockito.when(dataset.getDataStructure()).thenReturn(structure);
        Mockito.when(invalidDataset.getDataStructure()).thenReturn(wrongTypesDataset);

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThatThrownBy(() -> {
                FoldOperation clause = new FoldOperation(dataset, "newDimension", "newMeasure", invalidElements);
                clause.getDataStructure();
            })
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("m4")
                    .hasMessageContaining("not found");

            softly.assertThatThrownBy(() -> {
                FoldOperation clause = new FoldOperation(invalidDataset, "newDimension", "newMeasure", wrongTypesElements);
                clause.getDataStructure();
            })
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("same type");

            FoldOperation clause = new FoldOperation(dataset, "newDimension", "newMeasure", validElements);
            softly.assertThat(clause.getDataStructure()).isNotNull();
        }
    }

    @Test
    @Repeat(iterations = 10)
    public void testFold() {

        DatasetCloseWatcher dataset = DatasetCloseWatcher.wrap(StaticDataset.create()
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("id2", IDENTIFIER, String.class)
                .addComponent("measure1", MEASURE, String.class)
                .addComponent("measure2", MEASURE, String.class)
                .addComponent("measure3", MEASURE, String.class)
                .addComponent("attribute", ATTRIBUTE, String.class)

                .addPoints("id1-1", "id2-1", "measure1-1", "measure2-1", "measure3-1", "attribute1-1")
                .addPoints("id1-1", "id2-2", null, "measure2-2", "measure3-2", "attribute1-2")
                .addPoints("id1-2", "id2-1", "measure1-3", null, "measure3-3", "attribute1-3")
                .addPoints("id1-2", "id2-2", "measure1-4", "measure2-4", null, "attribute1-4")
                .addPoints("id1-3", "id2-1", "measure1-5", "measure2-5", "measure3-5", null)

                .build());

        // Collections.shuffle(data, new Random(randomLong()));

        // Randomly shuffle the measures
        ArrayList<String> elements = Lists.newArrayList(
                "measure2",
                "measure1",
                "measure3"
        );
        Collections.shuffle(elements, new Random(randomLong()));

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {

            FoldOperation clause = new FoldOperation(
                    new DebugDataset(dataset, System.err),
                    "newId",
                    "newMeasure",
                    ImmutableSet.copyOf(elements)
            );

            softly.assertThat(clause.getDataStructure()).containsOnlyKeys(
                    "id1", "id2", "newId", "newMeasure", "attribute"
            );

            // Need to sort back before assert.
            VtlOrdering order = VtlOrdering.using(clause).asc("id1", "id2", "newId").build();

            Stream<DataPoint> stream = clause.computeData(Ordering.ANY, Filtering.ALL, Collections.emptySet());
            softly.assertThat(stream.sorted(order))
                    .containsExactly(
                            DataPoint.create("id1-1", "id2-1", "attribute1-1", "measure1", "measure1-1"),
                            DataPoint.create("id1-1", "id2-1", "attribute1-1", "measure2", "measure2-1"),
                            DataPoint.create("id1-1", "id2-1", "attribute1-1", "measure3", "measure3-1"),

                            // DataPoint.create("id1-1", "id2-2", null, "measure1", "measure1-2"),
                            DataPoint.create("id1-1", "id2-2", "attribute1-2", "measure2", "measure2-2"),
                            DataPoint.create("id1-1", "id2-2", "attribute1-2", "measure3", "measure3-2"),

                            DataPoint.create("id1-2", "id2-1", "attribute1-3", "measure1", "measure1-3"),
                            // DataPoint.create("id1-2", "id2-1", null, "measure2", "measure2-3"),
                            DataPoint.create("id1-2", "id2-1", "attribute1-3", "measure3", "measure3-3"),

                            DataPoint.create("id1-2", "id2-2", "attribute1-4", "measure1", "measure1-4"),
                            DataPoint.create("id1-2", "id2-2", "attribute1-4", "measure2", "measure2-4"),
                            //DataPoint.create("id1-2", "id2-2", null, "measure3", "measure3-4"),

                            DataPoint.create("id1-3", "id2-1", null, "measure1", "measure1-5"),
                            DataPoint.create("id1-3", "id2-1", null, "measure2", "measure2-5"),
                            DataPoint.create("id1-3", "id2-1", null, "measure3", "measure3-5")
                    );
            stream.close();

            softly.assertThat(dataset.allStreamWereClosed()).isTrue();
        }
    }

    public class OperationDataset implements Dataset {
        AbstractDatasetOperation operation;

        public OperationDataset(FoldOperation fold) {
            this.operation = fold;
        }

        @Override
        public Stream<DataPoint> getData() {
            return getData(Ordering.ANY, Filtering.ALL, getDataStructure().keySet()).get();
        }

        @Override
        public Optional<Map<String, Integer>> getDistinctValuesCount() {
            return operation.getDistinctValuesCount();
        }

        @Override
        public Optional<Long> getSize() {
            return operation.getSize();
        }

        @Override
        public Optional<Stream<DataPoint>> getData(Ordering orders, Filtering filtering, Set<String> components) {
            return Optional.of(operation.computeData(orders, filtering, components));
        }

        @Override
        public DataStructure getDataStructure() {
            return operation.getDataStructure();
        }
    }

    public class DebugDataset implements Dataset {

        private final Dataset delegate;
        private final PrintStream output;

        public DebugDataset(Dataset delegate, PrintStream output) {
            this.delegate = delegate;
            this.output = output;
        }

        @Override
        public Stream<DataPoint> getData() {
            System.out.println("getData()");
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                return delegate.getData();
            } finally {
                stopwatch.stop();
                System.out.println("getData() : " + stopwatch.elapsed(TimeUnit.NANOSECONDS));
            }
        }

        @Override
        public Optional<Map<String, Integer>> getDistinctValuesCount() {
            System.out.println("getDistinctValuesCount()");
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                return delegate.getDistinctValuesCount();
            } finally {
                stopwatch.stop();
                System.out.println("getDistinctValuesCount() : " + stopwatch.elapsed(TimeUnit.NANOSECONDS));
            }
        }

        @Override
        public Optional<Long> getSize() {
            System.out.println("getSize()");
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                return delegate.getSize();
            } finally {
                stopwatch.stop();
                System.out.println("getSize() : " + stopwatch.elapsed(TimeUnit.NANOSECONDS));
            }
        }

        @Override
        public Optional<Stream<DataPoint>> getData(Ordering orders, Filtering filtering, Set<String> components) {
            System.out.printf("%h#getData(%s, %s, %s)\n", delegate.hashCode(), orders, filtering, components);
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                return delegate.getData(orders, filtering, components);
            } finally {
                stopwatch.stop();
                System.out.printf("%h#getData(%s, %s, %s) : %s\n", delegate.hashCode(), orders, filtering, components,
                        stopwatch.elapsed(TimeUnit.NANOSECONDS));
            }
        }

        @Override
        public Optional<Stream<DataPoint>> getData(Ordering order) {
            System.out.printf("%h#getData(%s)\n", delegate.hashCode(), order);
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                return delegate.getData(order);
            } finally {
                stopwatch.stop();
                System.out.printf("%h#getData(%s) : %s\n", delegate.hashCode(), order,
                        stopwatch.elapsed(TimeUnit.NANOSECONDS));
            }

        }

        @Override
        public Optional<Stream<DataPoint>> getData(Filtering filtering) {
            System.out.printf("%h#getData(%s)\n", delegate.hashCode(), filtering);
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                return delegate.getData(filtering);
            } finally {
                stopwatch.stop();
                System.out.printf("%h#getData(%s) : %s\n", delegate.hashCode(), filtering, stopwatch.elapsed(TimeUnit.NANOSECONDS));
            }
        }

        @Override
        public Optional<Stream<DataPoint>> getData(Set<String> components) {
            System.out.printf("%h#getData(%s)\n", delegate.hashCode(), components);
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                return delegate.getData(components);
            } finally {
                stopwatch.stop();
                System.out.printf("%h#getData(%s) : %s\n", delegate.hashCode(), components, stopwatch.elapsed(TimeUnit.NANOSECONDS));
            }
        }

        @Override
        public DataStructure getDataStructure() {
            System.out.printf("%h#getDataStructure()\n", delegate.hashCode());
            Stopwatch stopwatch = Stopwatch.createStarted();
            try {
                return delegate.getDataStructure();
            } finally {
                stopwatch.stop();
                System.out.printf("%h#getDataStructure() : %s\n", delegate.hashCode(), stopwatch.elapsed(TimeUnit.NANOSECONDS));
            }

        }
    }
}
