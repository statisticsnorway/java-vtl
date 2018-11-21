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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.script.support.DatasetCloseWatcher;
import no.ssb.vtl.script.support.VTLPrintStream;
import org.junit.Test;

import java.time.Instant;
import java.time.Year;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.Role.ATTRIBUTE;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

public class InnerJoinOperationTest extends RandomizedTest {

    @Test
    public void testInvalidKeyExtractorBug() {
        // When the position of the first dataset's identifiers does not match those of
        // the subsequent datasets the key extractor can return the wrong objects.
        StaticDataset t1 = StaticDataset.create()
                .addComponent("ms1", MEASURE, Long.class)
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("id2", IDENTIFIER, String.class)
                .addPoints(4L, "1", "2")
                .build();

        StaticDataset t2 = StaticDataset.create()
                .addComponent("ms2", MEASURE, Long.class)
                .addComponent("ms3", MEASURE, Long.class)
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("id2", IDENTIFIER, String.class)
                .addPoints(8L, 16L, "1", "2")
                .build();

        StaticDataset t3 = StaticDataset.create()
                .addComponent("ms2", MEASURE, Long.class)
                .addComponent("ms4", MEASURE, Long.class)
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("id2", IDENTIFIER, String.class)
                .addPoints(32L, 64L, "1", "2")
                .build();

        InnerJoinOperation result = new InnerJoinOperation(ImmutableMap.of(
                "t1", t1,
                "t2", t2,
                "t3", t3
        ));

        assertThat(result.getData()).containsExactly(
                DataPoint.create(4, "1", "2", 8, 16, 32, 64)
        );

    }

    @Test
    @Seed("9DC9B02FF9A216E4")
    public void testRegression() throws Exception {
        //D0E9B354FC19C5A:9DC9B02FF9A216E4
        testRandomDatasets();
    }

    @Test
    public void testIncompatibleOrder() {
        StaticDataset ds1 = StaticDataset.create()
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("idx", IDENTIFIER, String.class)
                .addPoints("D", "ds1 match")
                .addPoints("B", "ds1 miss")
                .addPoints("C", "ds1 match")
                .addPoints("A", "ds1 miss")
                .build();

        StaticDataset ds2 = StaticDataset.create()
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("idy", IDENTIFIER, String.class)
                .addPoints("F", "ds2 miss")
                .addPoints("C", "ds2 match")
                .addPoints("E", "ds2 miss")
                .addPoints("D", "ds2 match")
                .build();

        InnerJoinOperation joinOperation = new InnerJoinOperation(
                ImmutableMap.of(
                        "ds1", ds1, "ds2", ds2
                ), Collections.emptySet()
        );

        // This order is not possible.
        Optional<Stream<DataPoint>> emptyData = joinOperation.getData(
                Order.create(joinOperation.getDataStructure())
                        .put("idx", Order.Direction.DESC)
                        .build()
        );
        assertThat(emptyData.isPresent()).isFalse();

        assertThat(joinOperation.getData()).containsExactlyInAnyOrder(
                DataPoint.create("D", "ds1 match", "ds2 match"),
                DataPoint.create("C", "ds1 match", "ds2 match")
        );
    }

    @Test
    public void testSourceOrderEmpty() {
        StaticDataset ds1 = StaticDataset.create()
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("idx", IDENTIFIER, String.class)
                .addPoints("D", "ds1 match")
                .addPoints("B", "ds1 miss")
                .addPoints("C", "ds1 match")
                .addPoints("A", "ds1 miss")
                .build();

        StaticDataset ds2 = StaticDataset.create()
                .addComponent("id1", IDENTIFIER, String.class)
                .addComponent("idy", IDENTIFIER, String.class)
                .addPoints("F", "ds2 miss")
                .addPoints("C", "ds2 match")
                .addPoints("E", "ds2 miss")
                .addPoints("D", "ds2 match")
                .build();

        Dataset orderCheckDataset = new Dataset() {

            @Override
            public Stream<DataPoint> getData() {
                return ds2.getData();
            }

            @Override
            public Optional<Map<String, Integer>> getDistinctValuesCount() {
                return ds2.getDistinctValuesCount();
            }

            @Override
            public Optional<Long> getSize() {
                return ds2.getSize();
            }

            @Override
            public Optional<Stream<DataPoint>> getData(Order orders, Filtering filtering, Set<String> components) {
                return Optional.empty();
            }

            @Override
            public DataStructure getDataStructure() {
                return ds2.getDataStructure();
            }
        };

        InnerJoinOperation joinOperation = new InnerJoinOperation(
                ImmutableMap.of(
                        "ds1", ds1, "ds2", orderCheckDataset
                ), Collections.emptySet()
        );

        Optional<Stream<DataPoint>> data = joinOperation.getData(Order.create(joinOperation.getDataStructure()).put("id1", Order.Direction.DESC).build());
        assertThat(data.isPresent()).isTrue();
        assertThat(data.get()).containsExactly(
                DataPoint.create("D", "ds1 match", "ds2 match"),
                DataPoint.create("C", "ds1 match", "ds2 match")
        );
    }

    @Test
    @Repeat(iterations = 10)
    public void testRandomDatasets() throws Exception {

        Integer datasetAmount = scaledRandomIntBetween(1, 10);
        Integer rowAmount = scaledRandomIntBetween(0, 100);
        Set<Component> allComponents = Sets.newHashSet();

        Map<String, DatasetCloseWatcher> datasets = Maps.newLinkedHashMap();
        for (int i = 0; i < datasetAmount; i++) {

            StaticDataset.ValueBuilder datasetBuilder = StaticDataset.create()
                    .withName("id1", "id2", "id3", "measure", "attribute")
                    .andRoles(IDENTIFIER, IDENTIFIER, IDENTIFIER, MEASURE, ATTRIBUTE)
                    .andTypes(Instant.class, String.class, Instant.class, Long.class, String.class);

            int j = i;
            for (int rowNum = 0; rowNum < rowAmount; rowNum++) {
                datasetBuilder.addPoints(Year.of(2000).atDay(1).atStartOfDay().atOffset(ZoneOffset.UTC).toInstant(),
                        "id" + rowNum,
                        Instant.ofEpochMilli(60 * 60 * 24 * 100),
                        (long) (j + rowNum),
                        "attribute-" + j + "-" + rowNum
                );
            }

            StaticDataset dataset = datasetBuilder.build();
            datasets.put("ds" + i, DatasetCloseWatcher.wrap(dataset));
            allComponents.addAll(dataset.getDataStructure().values());

            new VTLPrintStream(System.out).println(datasetBuilder);
        }

        InnerJoinOperation result = new InnerJoinOperation(Maps.transformValues(datasets, ds -> ds));

        new VTLPrintStream(System.out).println(result);

        assertThat(result.getDataStructure())
                .describedAs("data structure of the inner join")
                .hasSize(datasetAmount * 2 + 3);

        assertThat(allComponents)
                .describedAs("all the components in the datasets")
                .hasSize(datasetAmount * 5);

        List<Object> data = datasets.values().stream()
                .flatMap(Dataset::getData)
                .flatMap(Collection::stream)
                .map(VTLObject::get)
                .collect(Collectors.toList());

        try (Stream<DataPoint> stream = result.getData()) {
            assertThat(stream.flatMap(Collection::stream))
                    .describedAs("the data")
                    .extracting(VTLObject::get)
                    .containsOnlyElementsOf(
                            data
                    );
        } finally {
            assertThat(datasets.values()).allMatch(DatasetCloseWatcher::allStreamWereClosed);
        }

    }

    @Test
    public void testJoinWithOneDatasetForwardsDistinctCount() {

        Dataset ds1 = StaticDataset.create()
                .addComponent("id", IDENTIFIER, String.class)
                .addPoints("a")
                .addPoints("b")
                .addPoints("c")
                .addPoints("d")
                .build();

        Optional<Long> originalSize = ds1.getSize();
        Optional<Map<String, Integer>> originalDistinctValues = ds1.getDistinctValuesCount();

        Map<String, Dataset> map = Maps.newLinkedHashMap();
        map.put("ds1", ds1);
        InnerJoinOperation innerJoinOperation = new InnerJoinOperation(map);

        assertThat(innerJoinOperation.getSize()).isEqualTo(originalSize);
        assertThat(innerJoinOperation.getDistinctValuesCount()).isEqualTo(originalDistinctValues);

    }

    @Test
    public void testInnerJoinWithCodeListDataset() throws Exception {

        Dataset ds1 = StaticDataset.create()

                .withName("kommune_nr", "periode", "m1", "at1")
                .andRoles(IDENTIFIER, IDENTIFIER, MEASURE, ATTRIBUTE)
                .andTypes(String.class, Instant.class, Long.class, String.class)
                .addPoints("0101", Instant.parse("2015-01-01T00:00:00.00Z"), 100L, "attr1")
                .addPoints("0111", Instant.parse("2014-01-01T00:00:00.00Z"), 101L, "attr2")
                .addPoints("9000", Instant.parse("2014-01-01T00:00:00.00Z"), 102L, "attr3")
                .build();

        Dataset dsCodeList2 = StaticDataset.create()
                .withName("kommune_nr", "name", "validFrom", "validTo")
                .andRoles(IDENTIFIER, MEASURE, IDENTIFIER, IDENTIFIER)
                .andTypes(String.class, /*code*/ String.class, Instant.class, Instant.class)
                .addPoints("0101", "Halden", Instant.parse("2013-01-01T00:00:00.00Z"), null)
                .addPoints("0111", "Hvaler", Instant.parse("2015-01-01T00:00:00.00Z"), null)
                .addPoints("1001", "Kristiansand", Instant.parse("2013-01-01T00:00:00.00Z"), Instant.parse("2015-01-01T00:00:00.00Z"))
                .build();

        AbstractJoinOperation ds3 = new InnerJoinOperation(ImmutableMap.of("ds1", ds1, "dsCodeList2", dsCodeList2));

        new VTLPrintStream(System.out).println(ds1);
        new VTLPrintStream(System.out).println(dsCodeList2);
        new VTLPrintStream(System.out).println(ds3);

        assertThat(ds3.getDataStructure().getRoles()).contains(
                entry("kommune_nr", IDENTIFIER),
                entry("periode", IDENTIFIER),
                entry("m1", MEASURE),
                entry("at1", ATTRIBUTE),
                entry("name", MEASURE),
                entry("validFrom", IDENTIFIER),
                entry("validTo", IDENTIFIER)
        );

        assertThat(ds3.getData()).flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        "0101", Instant.parse("2015-01-01T00:00:00.00Z"), 100L, "attr1", "Halden", Instant.parse("2013-01-01T00:00:00.00Z"), null,
                        "0111", Instant.parse("2014-01-01T00:00:00.00Z"), 101L, "attr2", "Hvaler", Instant.parse("2015-01-01T00:00:00.00Z"), null
                );
    }
}
