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
import com.google.common.collect.Maps;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.script.operations.DatasetOperationWrapper;
import no.ssb.vtl.script.support.DatasetCloseWatcher;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.Role;

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
                new DatasetOperationWrapper(dataset),
                ImmutableMap.of(
                        "id", "newId",
                        "measure", "newMeasure",
                        "attribute", "newAttribute"
                )
        );

        try (Stream<DataPoint> data = rename.getData()) {
            Assertions.assertThat(data).isNotNull();
        } finally {
            Assertions.assertThat(dataset.allStreamWereClosed()).isTrue();
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

        Assertions.assertThat(rename.getDataStructure().getRoles()).contains(
                Assertions.entry("Ib", Role.IDENTIFIER),
                Assertions.entry("Mb", Role.MEASURE),
                Assertions.entry("Ab", Role.ATTRIBUTE)
        );

    }

    @Test
    public void testRenameAndCast() throws Exception {
        Dataset dataset = Mockito.mock(Dataset.class);

        DataStructure structure = DataStructure.of(
                "Identifier1", Role.IDENTIFIER, String.class,
                "Identifier2", Role.IDENTIFIER, String.class,
                "Measure1", Role.MEASURE, String.class,
                "Measure2", Role.MEASURE, String.class,
                "Attribute1", Role.ATTRIBUTE, String.class,
                "Attribute2", Role.ATTRIBUTE, String.class
        );
        Mockito.when(dataset.getDataStructure()).thenReturn(structure);
        Mockito.when(dataset.getData()).then(invocation -> {
            return Stream.of(
                    structure.wrap(Maps.asMap(structure.keySet(), (String input) -> (Object) input))
            );
        });

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
                new DatasetOperationWrapper(dataset),
                newNames,
                newRoles
        );

        Assertions.assertThat(rename.getDataStructure().getRoles()).contains(
                Assertions.entry("Identifier1Measure", Role.MEASURE),
                Assertions.entry("Identifier2Attribute", Role.ATTRIBUTE),
                Assertions.entry("Measure1Identifier", Role.IDENTIFIER),
                Assertions.entry("Measure2Attribute", Role.ATTRIBUTE),
                Assertions.entry("Attribute1Identifier", Role.IDENTIFIER),
                Assertions.entry("Attribute2Measure", Role.MEASURE)
        );

        Assertions.assertThat(rename.getData()).flatExtracting(input -> input).extracting(VTLObject::get)
                .containsOnlyElementsOf(
                        structure.keySet()
                );
    }
}
