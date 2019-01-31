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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Component.Role;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.FilteringSpecification;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.OrderingSpecification;
import no.ssb.vtl.model.StaticDataset;
import org.junit.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.guava.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractJoinOperationTest {


    @Test
    public void testComponentMapping() {
        StaticDataset ds = datasetWith(
                ImmutableSet.of("id1", "id2", "id3"),
                ImmutableSet.of("me1", "me2", "me3"),
                ImmutableSet.of("at1", "at2", "at3")
        );

        Table<String, String, String> columnMapping = AbstractJoinOperation.getColumnMapping(ImmutableMap.of("ds", ds));

        assertThat(columnMapping).hasColumnCount(1).hasRowCount(9)
                .containsCell("id1", "ds", "id1")
                .containsCell("id2", "ds", "id2")
                .containsCell("id3", "ds", "id3")
                .containsCell("me1", "ds", "me1")
                .containsCell("me2", "ds", "me2")
                .containsCell("me3", "ds", "me3")
                .containsCell("at1", "ds", "at1")
                .containsCell("at2", "ds", "at2")
                .containsCell("at3", "ds", "at3");

        Table<String, String, String> columnMappingTwo = AbstractJoinOperation.getColumnMapping(
                ImmutableMap.of("ds1", ds, "ds2", ds)
        );

        assertThat(columnMappingTwo).hasColumnCount(2).hasRowCount(15)
                .containsCell("id1", "ds1", "id1")
                .containsCell("id2", "ds1", "id2")
                .containsCell("id3", "ds1", "id3")

                .containsCell("id1", "ds2", "id1")
                .containsCell("id2", "ds2", "id2")
                .containsCell("id3", "ds2", "id3")

                .containsCell("ds1_me1", "ds1", "me1")
                .containsCell("ds1_me2", "ds1", "me2")
                .containsCell("ds1_me3", "ds1", "me3")
                .containsCell("ds1_at1", "ds1", "at1")
                .containsCell("ds1_at2", "ds1", "at2")
                .containsCell("ds1_at3", "ds1", "at3")

                .containsCell("ds2_me1", "ds2", "me1")
                .containsCell("ds2_me2", "ds2", "me2")
                .containsCell("ds2_me3", "ds2", "me3")
                .containsCell("ds2_at1", "ds2", "at1")
                .containsCell("ds2_at2", "ds2", "at2")
                .containsCell("ds2_at3", "ds2", "at3");

        StaticDataset ds1 = datasetWith(
                ImmutableSet.of("id1", "id2", "idA"),
                ImmutableSet.of("me1", "me2"),
                ImmutableSet.of("at1", "at2")
        );

        StaticDataset ds2 = datasetWith(
                ImmutableSet.of("id1", "id2", "idB"),
                ImmutableSet.of("me1", "me3"),
                ImmutableSet.of("at1", "at3")
        );

        StaticDataset ds3 = datasetWith(
                ImmutableSet.of("id1", "id2", "idC"),
                ImmutableSet.of("me1"),
                ImmutableSet.of("at2", "at3")
        );


        Table<String, String, String> columnMappingThree = AbstractJoinOperation.getColumnMapping(
                ImmutableMap.of("ds1", ds1, "ds2", ds2, "ds3", ds3)
        );

        assertThat(columnMappingThree).hasColumnCount(3).hasRowCount(16)
                .containsCell("id1", "ds1", "id1")
                .containsCell("id1", "ds2", "id1")
                .containsCell("id1", "ds3", "id1")

                .containsCell("id2", "ds1", "id2")
                .containsCell("id2", "ds2", "id2")
                .containsCell("id2", "ds3", "id2")

                .containsCell("idA", "ds1", "idA")
                .containsCell("ds1_me1", "ds1", "me1")
                .containsCell("me2", "ds1", "me2")
                .containsCell("ds1_at1", "ds1", "at1")
                .containsCell("ds1_at2", "ds1", "at2")

                .containsCell("idB", "ds2", "idB")
                .containsCell("ds2_me1", "ds2", "me1")
                .containsCell("me3", "ds2", "me3")
                .containsCell("ds2_at1", "ds2", "at1")
                .containsCell("ds2_at3", "ds2", "at3")

                .containsCell("idC", "ds3", "idC")
                .containsCell("ds3_me1", "ds3", "me1")
                .containsCell("ds3_at2", "ds3", "at2")
                .containsCell("ds3_at3", "ds3", "at3");
    }

    @Test
    public void testComponentMappingWithJoinOnIdentifiers() {
        StaticDataset ds = datasetWith(
                ImmutableSet.of("id1", "id2", "id3"),
                ImmutableSet.of("me1", "me2", "me3"),
                ImmutableSet.of("at1", "at2", "at3")
        );

        Table<String, String, String> columnMappingTwo = AbstractJoinOperation.getColumnMapping(
                ImmutableMap.of("ds1", ds, "ds2", ds), ImmutableSet.of("id1")
        );

        assertThat(columnMappingTwo).hasColumnCount(2).hasRowCount(17)
                .containsCell("id1", "ds1", "id1")
                .containsCell("id1", "ds2", "id1")

                .containsCell("ds1_id2", "ds1", "id2")
                .containsCell("ds1_id3", "ds1", "id3")

                .containsCell("ds2_id2", "ds2", "id2")
                .containsCell("ds2_id3", "ds2", "id3")

                .containsCell("ds1_me1", "ds1", "me1")
                .containsCell("ds1_me2", "ds1", "me2")
                .containsCell("ds1_me3", "ds1", "me3")
                .containsCell("ds1_at1", "ds1", "at1")
                .containsCell("ds1_at2", "ds1", "at2")
                .containsCell("ds1_at3", "ds1", "at3")

                .containsCell("ds2_me1", "ds2", "me1")
                .containsCell("ds2_me2", "ds2", "me2")
                .containsCell("ds2_me3", "ds2", "me3")
                .containsCell("ds2_at1", "ds2", "at1")
                .containsCell("ds2_at2", "ds2", "at2")
                .containsCell("ds2_at3", "ds2", "at3");
    }

    private StaticDataset datasetWith(Set<String> identifiers, Set<String> measures, Set<String> attributes) {
        StaticDataset.StructureBuilder builder = StaticDataset.create();
        for (String identifier : identifiers) {
            builder.addComponent(identifier, Role.IDENTIFIER, String.class);
        }
        for (String measure : measures) {
            builder.addComponent(measure, Role.MEASURE, String.class);
        }
        for (String attribute : attributes) {
            builder.addComponent(attribute, Role.ATTRIBUTE, String.class);
        }
        return builder.build();
    }

    @Test
    public void testNoCommonComponent() {

        Dataset ds1 = mock(Dataset.class);
        DataStructure s1 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, Long.class)
                .put("id2", Role.IDENTIFIER, Long.class)
                .put("me1", Role.MEASURE, Long.class)
                .put("at1", Role.ATTRIBUTE, Long.class)
                .build();
        when(ds1.getDataStructure()).thenReturn(s1);

        Dataset ds2 = mock(Dataset.class);
        DataStructure s2 = DataStructure.builder()
                .put("id3", Role.IDENTIFIER, Long.class)
                .put("id4", Role.IDENTIFIER, Long.class)
                .put("me1", Role.MEASURE, Long.class)
                .put("at1", Role.ATTRIBUTE, Long.class)
                .build();
        when(ds2.getDataStructure()).thenReturn(s2);

        assertThatThrownBy(() -> {
            new TestAbstractJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2));
        }).isNotNull()
                .hasMessageContaining("common")
                .hasMessageContaining("identifiers");
    }

    @Test
    public void testCommonIdentifiersButWrongType() {

        Dataset ds1 = mock(Dataset.class);
        DataStructure s1 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, String.class)
                .put("id2", Role.IDENTIFIER, Long.class)
                .put("me1", Role.MEASURE, Long.class)
                .put("at1", Role.ATTRIBUTE, Long.class)
                .build();
        when(ds1.getDataStructure()).thenReturn(s1);

        Dataset ds2 = mock(Dataset.class);
        DataStructure s2 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, Long.class)
                .put("id2", Role.IDENTIFIER, String.class)
                .put("me1", Role.MEASURE, Long.class)
                .put("at1", Role.ATTRIBUTE, Long.class)
                .build();
        when(ds2.getDataStructure()).thenReturn(s2);

        assertThatThrownBy(() -> {
            new TestAbstractJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2));
        }).isNotNull().hasMessageContaining("types").hasMessageContaining("identifier");
    }

    @Test
    public void testWrongTypeWithIdentifierSelection() {

        Dataset ds1 = mock(Dataset.class);
        DataStructure s1 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, String.class)
                .put("id2", Role.IDENTIFIER, Long.class)
                .put("me1", Role.MEASURE, Long.class)
                .put("at1", Role.ATTRIBUTE, Long.class)
                .build();
        when(ds1.getDataStructure()).thenReturn(s1);

        Dataset ds2 = mock(Dataset.class);
        DataStructure s2 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, String.class)
                .put("id2", Role.IDENTIFIER, String.class)
                .put("me1", Role.MEASURE, Long.class)
                .put("at1", Role.ATTRIBUTE, Long.class)
                .build();
        when(ds2.getDataStructure()).thenReturn(s2);

        new TestAbstractJoinOperation(
                ImmutableMap.of("ds1", ds1, "ds2", ds2),
                ImmutableMap.of("id1", s1.get("id1"))
        );

        assertThatThrownBy(() -> {
            new TestAbstractJoinOperation(
                    ImmutableMap.of("ds1", ds1, "ds2", ds2),
                    ImmutableMap.of("id2", s1.get("id2"))
            );
        }).isNotNull().hasMessageContaining("types");
    }


    @Test
    public void testCreateIdentifierMapping() throws Exception {

        Dataset ds1 = mock(Dataset.class);
        DataStructure s1 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, Long.class)
                .put("id2", Role.IDENTIFIER, Long.class)
                .put("id4", Role.IDENTIFIER, Long.class)
                .put("me1", Role.MEASURE, Long.class)
                .put("at1", Role.ATTRIBUTE, Long.class)
                .build();
        when(ds1.getDataStructure()).thenReturn(s1);

        Dataset ds2 = mock(Dataset.class);
        DataStructure s2 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, Long.class)
                .put("id3", Role.IDENTIFIER, Long.class)
                .put("id4", Role.IDENTIFIER, Long.class)
                .put("me1", Role.MEASURE, Long.class)
                .put("at1", Role.ATTRIBUTE, Long.class)
                .build();
        when(ds2.getDataStructure()).thenReturn(s2);

        Dataset ds3 = mock(Dataset.class);
        DataStructure s3 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, Long.class)
                .put("id4", Role.IDENTIFIER, Long.class)
                .put("me1", Role.MEASURE, Long.class)
                .put("at1", Role.ATTRIBUTE, Long.class)
                .build();
        when(ds3.getDataStructure()).thenReturn(s3);

        Table<Component, Dataset, Component> componentTable = AbstractJoinOperation.createComponentMapping(
                Lists.newArrayList(ds1, ds3, ds2)
        );

        assertThat(componentTable).hasRowCount(10).hasColumnCount(3)

                .containsCell(s1.get("id1"), ds1, s1.get("id1"))
                .containsCell(s1.get("id1"), ds2, s2.get("id1"))
                .containsCell(s1.get("id1"), ds3, s3.get("id1"))

                .containsCell(s1.get("id2"), ds1, s1.get("id2"))

                .containsCell(s2.get("id3"), ds2, s2.get("id3"))

                .containsCell(s1.get("id4"), ds1, s1.get("id4"))
                .containsCell(s1.get("id4"), ds2, s2.get("id4"))
                .containsCell(s1.get("id4"), ds3, s3.get("id4"))

                .containsCell(s1.get("me1"), ds1, s1.get("me1"))
                .containsCell(s2.get("me1"), ds2, s2.get("me1"))
                .containsCell(s3.get("me1"), ds3, s3.get("me1"))

                .containsCell(s1.get("at1"), ds1, s1.get("at1"))
                .containsCell(s2.get("at1"), ds2, s2.get("at1"))
                .containsCell(s3.get("at1"), ds3, s3.get("at1"));

    }

    @Test
    public void testSameDatasetShouldNotFail() throws Exception {

        Dataset ds1 = mock(Dataset.class);
        Dataset ds2 = mock(Dataset.class);

        DataStructure structure = DataStructure.builder()
                .put("m", Role.IDENTIFIER, Long.class)
                .build();

        when(ds1.getDataStructure()).thenReturn(structure);
        when(ds2.getDataStructure()).thenReturn(structure);

        HashMap<String, Dataset> datasets = Maps.newHashMap();
        datasets.put("ds1", ds1);
        datasets.put("ds2", ds1);

        new TestAbstractJoinOperation(datasets);
    }

    @Test
    public void testEmptyFails() {
        Throwable ex = null;
        try {
            new TestAbstractJoinOperation(Collections.emptyMap());
        } catch (Throwable t) {
            ex = t;
        }
        assertThat(ex)
                .hasMessageContaining("join")
                .hasMessageContaining("empty")
                .hasMessageContaining("dataset");
    }

    @Test
    public void testOnlyOneDataset() {

        StaticDataset t1 = StaticDataset.create()
                .addComponent("id1", Role.IDENTIFIER, String.class)
                .addComponent("id2", Role.IDENTIFIER, Long.class)
                .addComponent("uni1", Role.IDENTIFIER, Double.class)
                .addComponent("m1", Role.MEASURE, Instant.class)
                .addComponent("a1", Role.MEASURE, Boolean.class)
                .build();

        TestAbstractJoinOperation operation = new TestAbstractJoinOperation(ImmutableMap.of("t1", t1));

        assertThat(operation.getSize()).isEqualTo(t1.getSize());
        assertThat(operation.getDistinctValuesCount()).isEqualTo(t1.getDistinctValuesCount());
    }


    @Test
    public void testComponentNameIsUnique() {

        Dataset ds1 = mock(Dataset.class);
        DataStructure s1 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, Long.class)
                .put("me1", Role.MEASURE, Long.class)
                .put("me2", Role.MEASURE, Long.class)
                .put("me3", Role.MEASURE, Long.class)
                .put("at1", Role.ATTRIBUTE, Long.class)
                .build();
        when(ds1.getDataStructure()).thenReturn(s1);

        Dataset ds2 = mock(Dataset.class);
        DataStructure s2 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, Long.class)
                .put("me1", Role.MEASURE, Long.class)
                .put("at1", Role.ATTRIBUTE, Long.class)
                .build();
        when(ds2.getDataStructure()).thenReturn(s2);

        TestAbstractJoinOperation joinOperation = new TestAbstractJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2));

        assertThat(joinOperation.componentNameIsUnique("ds1", "me1"))
                .isFalse();
        assertThat(joinOperation.componentNameIsUnique("ds1", "me2"))
                .isTrue();
        assertThat(joinOperation.componentNameIsUnique("ds1", "me3"))
                .isTrue();
        assertThat(joinOperation.componentNameIsUnique("ds1", "at1"))
                .isFalse();
    }

    @Test
    public void testCommonIdentifiers() {

        Dataset ds1 = mock(Dataset.class);
        DataStructure s1 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, Long.class)
                .put("id2", Role.IDENTIFIER, Long.class)
                .put("me1", Role.MEASURE, Long.class)
                .put("at1", Role.ATTRIBUTE, Long.class)
                .build();
        when(ds1.getDataStructure()).thenReturn(s1);

        Dataset ds2 = mock(Dataset.class);
        DataStructure s2 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, Long.class)
                .put("id2", Role.IDENTIFIER, Long.class)
                .put("me1", Role.MEASURE, Long.class)
                .put("at1", Role.ATTRIBUTE, Long.class)
                .build();
        when(ds2.getDataStructure()).thenReturn(s2);

        TestAbstractJoinOperation joinOperation = new TestAbstractJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2),
                Collections.emptyMap());
        assertThat(joinOperation.getCommonIdentifiers().values())
                .containsExactlyInAnyOrder(s1.get("id1"), s1.get("id2"));

        joinOperation = new TestAbstractJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2),
                ImmutableMap.of("id1", s1.get("id1")));
        assertThat(joinOperation.getCommonIdentifiers().values())
                .containsExactlyInAnyOrder(s1.get("id1"));
    }

    @Test
    public void testJoinOnIdentifierThatIsNotCommon() {

        Dataset ds1 = mock(Dataset.class);
        DataStructure s1 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, Long.class)
                .put("id2", Role.IDENTIFIER, Long.class)
                .put("me1", Role.MEASURE, Long.class)
                .build();
        when(ds1.getDataStructure()).thenReturn(s1);

        Dataset ds2 = mock(Dataset.class);
        DataStructure s2 = DataStructure.builder()
                .put("id1", Role.IDENTIFIER, Long.class)
                .put("me1", Role.MEASURE, Long.class)
                .build();
        when(ds2.getDataStructure()).thenReturn(s2);

        assertThatThrownBy(() ->
            new TestAbstractJoinOperation(ImmutableMap.of("ds1", ds1, "ds2", ds2),
                    ImmutableMap.of("id2", s1.get("id2")))
        ).isNotNull().hasMessageContaining("join operation").hasMessageContaining("not a common identifier");
    }

    private static class TestAbstractJoinOperation extends AbstractJoinOperation {

        List<List<DataPoint>> leftMiss = Lists.newArrayList();
        List<List<DataPoint>> rightMiss = Lists.newArrayList();
        List<List<DataPoint>> hits = Lists.newArrayList();


        public TestAbstractJoinOperation(Map<String, Dataset> namedDatasets) {
            super(namedDatasets, Collections.emptyMap());
        }

        public TestAbstractJoinOperation(Map<String, Dataset> namedDatasets, Map<String, Component> identifiers) {
            super(namedDatasets, identifiers);
        }

        @Override
        public Stream<DataPoint> computeData(Ordering orders, Filtering filtering, Set<String> components) {
            return null;
        }

        @Override
        public FilteringSpecification computeRequiredFiltering(FilteringSpecification filtering) {
            return null;
        }

        @Override
        public OrderingSpecification computeRequiredOrdering(OrderingSpecification filtering) {
            return null;
        }
    }
}
