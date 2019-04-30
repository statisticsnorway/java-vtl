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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.Role.ATTRIBUTE;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;

public class FoldOperationBenchmark {

    @State(Scope.Benchmark)
    public static class FoldState {

        private Dataset dataset;
        private FoldOperation clause;
        private Spliterator<DataPoint> spliterator;

        @Setup()
        public void setup() {

            DataStructure structure = DataStructure.builder()
                    .put("id1", IDENTIFIER, String.class)
                    .put("id2", IDENTIFIER, String.class)
                    .put("measure1", MEASURE, String.class)
                    .put("measure2", MEASURE, String.class)
                    .put("attribute", ATTRIBUTE, String.class)
                    .build();

            Iterator<DataPoint> source = Iterators.cycle(
                    Arrays.asList(DataPoint.create("id1-1", "id2-1", "measure1-1", "measure2-1", "attribute1-1"),
                    DataPoint.create("id1-1", "id2-2", null, "measure2-2", "attribute1-2"),
                    DataPoint.create("id1-2", "id2-1", "measure1-3", null, "attribute1-3"),
                    DataPoint.create("id1-2", "id2-2", "measure1-4", "measure2-4", null),
                    DataPoint.create("id1-3", "id2-1", null, null, null))
            );

            dataset = new Dataset() {

                @Override
                public Stream<DataPoint> getData() {
                    return Stream.generate(source::next);
                }

                @Override
                public Optional<Map<String, Integer>> getDistinctValuesCount() {
                    return Optional.empty();
                }

                @Override
                public Optional<Long> getSize() {
                    return Optional.empty();
                }

                @Override
                public DataStructure getDataStructure() {
                    return structure;
                }
            };

            clause = new FoldOperation(
                    dataset,
                    "newId",
                    "newMeasure",
                    ImmutableSet.of("measure1", "measure2")
            );

            spliterator = clause.getData().spliterator();
        }
    }

    @Benchmark
    @Fork(value = 4, warmups = 1)
    @Warmup(iterations = 5)
    @BenchmarkMode(Mode.Throughput)
    @Measurement(iterations = 10)
    public void foldBenchmark(FoldState state, Blackhole blackhole) {
        state.spliterator.tryAdvance(blackhole::consume);
    }
}
