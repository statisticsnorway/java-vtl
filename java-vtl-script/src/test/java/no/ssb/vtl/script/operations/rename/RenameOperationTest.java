package no.ssb.vtl.script.operations.rename;

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

/*-
 * #%L
 * java-vtl-script
 * %%
 * Copyright (C) 2016 Hadrien Kohl
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
 * #L%
 */

import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.FilteringSpecification;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.model.VtlFiltering;
import no.ssb.vtl.script.operations.DatasetOperationWrapper;
import no.ssb.vtl.script.support.DatasetCloseWatcher;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.Role;
import static org.assertj.core.api.Assertions.assertThat;

public class RenameOperationTest {

    @Test
    public void testStreamClosed() {

        DatasetCloseWatcher dataset = DatasetCloseWatcher.wrap(StaticDataset.create()
                .addComponent("id", Role.IDENTIFIER, String.class)
                .addComponent("measure", Role.MEASURE, String.class)
                .addComponent("attribute", Role.MEASURE, String.class)

                .addPoints("id1", "measure1", "attribute1")
                .build()
        );

        RenameOperation rename = new RenameOperation(
                dataset,
                ImmutableMap.of(
                        "id", "newId",
                        "measure", "newMeasure",
                        "attribute", "newAttribute"
                )
        );

        try (Stream<DataPoint> data = rename.computeData(
                Ordering.ANY, Filtering.ALL, Collections.emptySet()
        )) {
            assertThat(data).isNotNull();
        } finally {
            assertThat(dataset.allStreamWereClosed()).isTrue();
        }
    }

    @Test
    public void testRename() throws Exception {

        Dataset dataset = Mockito.mock(Dataset.class);

        DataStructure structure = DataStructure.of(
                "Ia", Role.IDENTIFIER, String.class,
                "Ma", Role.MEASURE, String.class,
                "Aa", Role.ATTRIBUTE, String.class
        );
        Mockito.when(dataset.getDataStructure()).thenReturn(structure);

        Map<String, String> newNames = ImmutableMap.of(
                "Ia", "Ib",
                "Ma", "Mb",
                "Aa", "Ab"
        );

        RenameOperation rename;
        rename = new RenameOperation(
                new DatasetOperationWrapper(dataset),
                newNames
        );

        assertThat(rename.getDataStructure().getRoles()).contains(
                Assertions.entry("Ib", Role.IDENTIFIER),
                Assertions.entry("Mb", Role.MEASURE),
                Assertions.entry("Ab", Role.ATTRIBUTE)
        );

    }

    @Test
    public void testComplexFilters() {
        Dataset dataset = StaticDataset.create()
                .addComponent("Identifier1", Role.IDENTIFIER, String.class)
                .addComponent("Identifier2", Role.IDENTIFIER, String.class)
                .addComponent("Measure1", Role.MEASURE, String.class)
                .addComponent("Measure2", Role.MEASURE, String.class)
                .addComponent("Attribute1", Role.ATTRIBUTE, String.class)
                .addComponent("Attribute2", Role.ATTRIBUTE, String.class)

                .addPoints("Identifier1", "Identifier2", "Measure1", "Measure2", "Attribute1", "Attribute2")
                .build();

        RenameOperation renameOperation = new RenameOperation(dataset, ImmutableMap.<String, String>builder()
                .put("Identifier1", "RenamedIdentifier1")
                .put("Identifier2", "RenamedIdentifier2")
                .put("Measure1", "RenamedMeasure1")
                .put("Measure2", "RenamedMeasure2")
                .put("Attribute1", "RenamedAttribute1")
                .put("Attribute2", "RenamedAttribute2")
                .build()
        );

        VtlFiltering complexFilter = VtlFiltering.using(renameOperation).and(
                VtlFiltering.eq("RenamedIdentifier1", "a"),
                VtlFiltering.or(
                        VtlFiltering.gt("RenamedIdentifier2", "b"),
                        VtlFiltering.lt("RenamedMeasure1", "b")
                ),
                VtlFiltering.not(VtlFiltering.and(
                        VtlFiltering.neq("RenamedMeasure2", "d"),
                        VtlFiltering.ge("RenamedAttribute1", "e")
                )),
                VtlFiltering.le("RenamedAttribute2", "f")
        ).build();

        FilteringSpecification renamedFilter = renameOperation.computeRequiredFiltering(complexFilter);
        assertThat(renamedFilter.toString()).isEqualTo(
                "(Identifier1=a&(Identifier2>b|Measure1<b)&~(Measure2!=d&Attribute1>=e)&Attribute2<=f)"
        );
    }

    @Test
    public void testToString() {
        Dataset dataset = StaticDataset.create()
                .addComponent("Identifier1", Role.IDENTIFIER, String.class)
                .addComponent("Identifier2", Role.IDENTIFIER, String.class)
                .addComponent("Measure1", Role.MEASURE, String.class)
                .addComponent("Measure2", Role.MEASURE, String.class)
                .addComponent("Attribute1", Role.ATTRIBUTE, String.class)
                .addComponent("Attribute2", Role.ATTRIBUTE, String.class)

                .addPoints("Identifier1", "Identifier2", "Measure1", "Measure2", "Attribute1", "Attribute2")
                .build();

        RenameOperation renameOperation = new RenameOperation(dataset, ImmutableMap.<String, String>builder()
                .put("Identifier1", "RenamedIdentifier1")
                .put("Identifier2", "RenamedIdentifier2")
                .put("Measure1", "RenamedMeasure1")
                .put("Measure2", "RenamedMeasure2")
                .put("Attribute1", "RenamedAttribute1")
                .put("Attribute2", "RenamedAttribute2")
                .build(),
                ImmutableMap.of(
                        "Measure1", Role.ATTRIBUTE
                )
        );

        assertThat(renameOperation.toString()).isEqualTo(
                "RenameOperation{" +
                        "Identifier1=RenamedIdentifier1, " +
                        "Identifier2=RenamedIdentifier2, " +
                        "Measure1=RenamedMeasure1(ATTRIBUTE), " +
                        "Measure2=RenamedMeasure2, " +
                        "Attribute1=RenamedAttribute1, " +
                        "Attribute2=RenamedAttribute2}"
        );
    }

    @Test
    public void testGetSizeAndDistinct() {
        Dataset dataset = StaticDataset.create()
                .addComponent("Identifier1", Role.IDENTIFIER, String.class)
                .addComponent("Identifier2", Role.IDENTIFIER, String.class)
                .addComponent("Measure1", Role.MEASURE, String.class)
                .addComponent("Measure2", Role.MEASURE, String.class)
                .addComponent("Attribute1", Role.ATTRIBUTE, String.class)
                .addComponent("Attribute2", Role.ATTRIBUTE, String.class)

                .addPoints("Identifier1", "Identifier2", "Measure1", "Measure2", "Attribute1", "Attribute2")
                .build();

        RenameOperation renameOperation = new RenameOperation(dataset, ImmutableMap.<String, String>builder()
                .put("Identifier1", "RenamedIdentifier1")
                .put("Identifier2", "RenamedIdentifier2")
                .put("Measure1", "RenamedMeasure1")
                .put("Measure2", "RenamedMeasure2")
                .put("Attribute1", "RenamedAttribute1")
                .put("Attribute2", "RenamedAttribute2")
                .build()
        );

        assertThat(renameOperation.getSize()).contains(1L);
        assertThat(renameOperation.getDistinctValuesCount()).contains(
                ImmutableMap.<String, Integer>builder()
                        .put("RenamedIdentifier1", 1)
                        .put("RenamedIdentifier2", 1)
                        .put("RenamedMeasure1", 1)
                        .put("RenamedMeasure2", 1)
                        .put("RenamedAttribute1", 1)
                        .put("RenamedAttribute2", 1)
                        .build()
        );

    }

    @Test
    public void testRenameAndCast() throws Exception {

        Dataset dataset = StaticDataset.create()
                .addComponent("Identifier1", Role.IDENTIFIER, String.class)
                .addComponent("Identifier2", Role.IDENTIFIER, String.class)
                .addComponent("Measure1", Role.MEASURE, String.class)
                .addComponent("Measure2", Role.MEASURE, String.class)
                .addComponent("Attribute1", Role.ATTRIBUTE, String.class)
                .addComponent("Attribute2", Role.ATTRIBUTE, String.class)

                .addPoints("Identifier1", "Identifier2", "Measure1", "Measure2", "Attribute1", "Attribute2")
                .build();

        ImmutableMap<String, String> newNames = new ImmutableMap.Builder<String, String>()
                .put("Identifier1", "Identifier1Measure")
                .put("Identifier2", "Identifier2Attribute")
                .put("Measure1", "Measure1Identifier")
                .put("Measure2", "Measure2Attribute")
                .put("Attribute1", "Attribute1Identifier")
                .put("Attribute2", "Attribute2Measure")
                .build();

        ImmutableMap<String, Role> newRoles = new ImmutableMap.Builder<String, Role>()
                .put("Identifier1", Role.MEASURE)
                .put("Identifier2", Role.ATTRIBUTE)
                .put("Measure1", Role.IDENTIFIER)
                .put("Measure2", Role.ATTRIBUTE)
                .put("Attribute1", Role.IDENTIFIER)
                .put("Attribute2", Role.MEASURE)
                .build();

        RenameOperation rename;
        rename = new RenameOperation(
                dataset,
                newNames,
                newRoles
        );

        assertThat(rename.getDataStructure().getRoles()).contains(
                Assertions.entry("Identifier1Measure", Role.MEASURE),
                Assertions.entry("Identifier2Attribute", Role.ATTRIBUTE),
                Assertions.entry("Measure1Identifier", Role.IDENTIFIER),
                Assertions.entry("Measure2Attribute", Role.ATTRIBUTE),
                Assertions.entry("Attribute1Identifier", Role.IDENTIFIER),
                Assertions.entry("Attribute2Measure", Role.MEASURE)
        );

        assertThat(rename.computeData(Ordering.ANY, Filtering.ALL, Collections.emptySet())).flatExtracting(input -> input).extracting(VTLObject::get)
                .containsOnlyElementsOf(
                        dataset.getDataStructure().keySet()
                );
    }
}
