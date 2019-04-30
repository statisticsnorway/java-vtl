package no.ssb.vtl.script.operations.aggregation;

/*
 * -
 *  * ========================LICENSE_START=================================
 * * Java VTL
 *  *
 * %%
 * Copyright (C) 2017 Hadrien Kohl
 *  *
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
 *
 */

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VtlFiltering;
import no.ssb.vtl.model.VtlOrdering;
import no.ssb.vtl.script.functions.AggregationSumFunction;
import no.ssb.vtl.script.support.DatasetCloseWatcher;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static no.ssb.vtl.model.Component.Role;
import static no.ssb.vtl.model.Ordering.Direction.ASC;
import static no.ssb.vtl.model.Ordering.Direction.DESC;
import static org.assertj.core.api.Assertions.assertThat;

public class AggregationOperationTest {

    private DatasetCloseWatcher dataset;

    @Before
    public void setUp() {

        StaticDataset staticDataset = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("id2", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.IDENTIFIER, Long.class)

                .addPoints("a", "1", 1L)
                .addPoints("a", "2", 2L)
                .addPoints("a", "3", 4L)
                .addPoints("a", "4", 8L)

                .addPoints("b", "1", 1L)
                .addPoints("b", "2", 2L)
                .addPoints("b", "3", 4L)
                .addPoints("b", "4", 8L)

                .addPoints("c", "1", 1L)
                .addPoints("c", "2", 2L)
                .addPoints("c", "3", 4L)
                .addPoints("c", "4", 8L)

                .build();

        this.dataset = DatasetCloseWatcher.wrap(staticDataset);
    }

    @Test
    public void testClosesStreams() {

        DataStructure structure = this.dataset.getDataStructure();
        AggregationOperation aggregationOperation = new AggregationOperation(
                this.dataset,
                ImmutableList.of(structure.get("id1")),
                ImmutableList.of(structure.get("m1")),
                new AggregationSumFunction()
        );

        try (Stream<DataPoint> data = aggregationOperation.getData()) {
            assertThat(data).containsExactly(
                    DataPoint.create("a", 15),
                    DataPoint.create("b", 15),
                    DataPoint.create("c", 15)
            );
        } finally {
            assertThat(dataset.allStreamWereClosed()).isTrue();
        }

    }

    @Test
    public void testFilters() {

        DataStructure structure = this.dataset.getDataStructure();
        AggregationOperation op = new AggregationOperation(
                this.dataset,
                ImmutableList.of(structure.get("id1")),
                ImmutableList.of(structure.get("m1")),
                new AggregationSumFunction()
        );

        VtlFiltering filterOnId1 = VtlFiltering.using(op).with(
                VtlFiltering.eq("id1", "a")
        );
        assertThat(op.computeRequiredFiltering(filterOnId1).toString())
                .isEqualTo("id1=a");

        VtlFiltering filterOnId2 = VtlFiltering.using(op).with(
                VtlFiltering.eq("id2", "1")
        );
        assertThat(op.computeRequiredFiltering(filterOnId2).toString())
                .isEqualTo("TRUE");

        VtlFiltering filterOnId1AndId2 = VtlFiltering.and(
                filterOnId1, filterOnId2
        );
        assertThat(op.computeRequiredFiltering(filterOnId1AndId2).toString())
                .isEqualTo("(id1=a&TRUE)");

        VtlFiltering filterOnM1 = VtlFiltering.using(op).with(
                VtlFiltering.le("m1", 1L)
        );
        assertThat(op.computeRequiredFiltering(filterOnM1).toString())
                .isEqualTo("TRUE");

        VtlFiltering filterOnId1AndId2AndM1 = VtlFiltering.and(
                filterOnId1, filterOnId2, filterOnM1
        );
        assertThat(op.computeRequiredFiltering(filterOnId1AndId2AndM1).toString())
                .isEqualTo("(id1=a&TRUE&TRUE)");

        VtlFiltering childFilter = (VtlFiltering) op.computeRequiredFiltering(filterOnId1);
        assertThat(op.getData().filter(childFilter)).containsExactly(
                DataPoint.create("a", 15)
        );

    }

    @Test
    public void testOrders() {

        // Need a couple more columns to test ordering.
        StaticDataset dataset = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("id3", Role.IDENTIFIER, String.class)
                .addComponent("id2", Role.IDENTIFIER, String.class)
                .addComponent("id4", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.IDENTIFIER, Long.class)
                .build();

        DataStructure structure = dataset.getDataStructure();
        AggregationOperation op = new AggregationOperation(
                dataset,
                ImmutableList.of(
                        structure.get("id1"),
                        structure.get("id2"),
                        structure.get("id3")
                ),
                ImmutableList.of(structure.get("m1")),
                new AggregationSumFunction()
        );

        assertThat(op.computeRequiredOrdering(VtlOrdering.using(op).desc("id1").build()).toString())
                .isEqualTo("VtlOrdering{id1=DESC, id3=ASC, id2=ASC}");

        assertThat(op.computeRequiredOrdering(VtlOrdering.using(op).desc("id3").desc("id1").build()).toString())
                .isEqualTo("VtlOrdering{id3=DESC, id1=DESC, id2=ASC}");

    }

    @Test
    public void testAggregation() {
        StaticDataset staticDataset = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("id2", Role.IDENTIFIER, String.class)
                .addComponent("id3", Role.IDENTIFIER, String.class)
                .addComponent("id4", Role.IDENTIFIER, String.class)
                .addComponent("m1", Role.MEASURE, Long.class)
                .addComponent("m2", Role.MEASURE, Long.class)

                .addPoints("a", "1", "a", "l", 1L, 8L)
                .addPoints("a", "2", "b", "k", 2L, 4L)
                .addPoints("a", "3", "c", "j", 4L, 2L)
                .addPoints("a", "4", "d", "i", 8L, 1L)

                .addPoints("b", "1", "e", "h", 1L, 8L)
                .addPoints("b", "2", "f", "g", 2L, 4L)
                .addPoints("b", "3", "g", "f", 4L, 2L)
                .addPoints("b", "4", "h", "e", 8L, 1L)

                .addPoints("c", "1", "i", "d", 1L, 8L)
                .addPoints("c", "2", "j", "c", 2L, 4L)
                .addPoints("c", "3", "k", "b", 4L, 2L)
                .addPoints("c", "4", "l", "a", 8L, 1L)

                .build();

        DataStructure structure = staticDataset.getDataStructure();
        AggregationOperation groupById1 = new AggregationOperation(
                staticDataset,
                Arrays.asList(structure.get("id1")),
                Arrays.asList(structure.get("m1"), structure.get("m2")),
                new AggregationSumFunction()
        );

        for (VtlOrdering ordering : allCombinations(groupById1)) {
            List<DataPoint> data = groupById1.computeData(ordering, Filtering.ALL, groupById1.getDataStructure().keySet()).collect(toList());
            assertThat(data).as("data grouped by id1 with ordering %s", ordering)
                    .containsExactlyInAnyOrder(
                            DataPoint.create("a", 15L, 15L),
                            DataPoint.create("b", 15L, 15L),
                            DataPoint.create("c", 15L, 15L)
                    ).isSortedAccordingTo(ordering);
        }

        AggregationOperation groupById2 = new AggregationOperation(
                staticDataset,
                Arrays.asList(structure.get("id2")),
                Arrays.asList(structure.get("m1"), structure.get("m2")),
                new AggregationSumFunction()
        );

        for (VtlOrdering ordering : allCombinations(groupById2)) {
            List<DataPoint> data = groupById2.computeData(ordering, Filtering.ALL, groupById2.getDataStructure().keySet()).collect(toList());
            assertThat(data).as("data grouped by id2 with ordering %s", ordering)
                    .containsExactlyInAnyOrder(
                            DataPoint.create("1", 3L, 24L),
                            DataPoint.create("2", 6L, 12L),
                            DataPoint.create("3", 12L, 6L),
                            DataPoint.create("4", 24L, 3L)
                    ).isSortedAccordingTo(ordering);
        }

    }

    /**
     * Returns all the combination of ordering for a given dataset.
     */
    Iterable<VtlOrdering> allCombinations(Dataset dataset) {
        Set<String> columns = dataset.getDataStructure().keySet();
        List<ImmutableSet<Ordering.Direction>> directionCombinations = Collections.nCopies(
                columns.size(), ImmutableSet.of(ASC, DESC)
        );
        return () -> new AbstractIterator<VtlOrdering>() {

            PeekingIterator<List<String>> columnPermutations = Iterators.peekingIterator(
                    Collections2.permutations(columns).iterator()
            );

            Iterator<List<Ordering.Direction>> directions = Sets.cartesianProduct(directionCombinations).iterator();

            @Override
            protected VtlOrdering computeNext() {
                if (!directions.hasNext() && columnPermutations.hasNext()) {
                    directions = Sets.cartesianProduct(directionCombinations).iterator();
                    columnPermutations.next();
                }
                if (columnPermutations.hasNext()) {
                    VtlOrdering.Builder builder = VtlOrdering.using(dataset);
                    Iterator<String> columnIterator = columnPermutations.peek().iterator();
                    Iterator<Ordering.Direction> directionIterator = directions.next().iterator();
                    while (columnIterator.hasNext() && directionIterator.hasNext()) {
                        builder.then(directionIterator.next(), columnIterator.next());
                    }
                    return builder.build();
                } else {
                    return endOfData();
                }
            }
        };
    }
}
