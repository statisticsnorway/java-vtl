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
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.Role;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RenameOperationTest {

    Dataset notNullDataset = new Dataset() {
        @Override
        public Stream<DataPoint> getData() {
            return null;
        }

        @Override
        public Optional<Map<String, Integer>> getDistinctValuesCount() {
            return null;
        }

        @Override
        public Optional<Long> getSize() {
            return null;
        }

        @Override
        public DataStructure getDataStructure() {
            return null;
        }

    };

//    @Test(expected = IllegalArgumentException.class)
//    public void testNotDuplicates() throws Exception {
//        ImmutableMap<String, String> names = ImmutableMap.of("a1", "a2", "b1", "a2");
//        try {
//            new RenameOperation(notNullDataset, names, Collections.emptyMap());
//        } catch (Throwable t) {
//            assertThat(t).hasMessageContaining("a2");
//            throw t;
//        }
//    }

//    @Test(expected = IllegalArgumentException.class)
//    public void testConsistentArguments() throws Exception {
//        ImmutableMap<String, String> names = ImmutableMap.of("a1", "a2", "b1", "b2");
//        ImmutableMap<String, Component.Role> roles;
//        roles = ImmutableMap.of("nothere", Role.IDENTIFIER);
//        try {
//            new RenameOperation(notNullDataset, names, roles);
//        } catch (Throwable t) {
//            assertThat(t).hasMessageContaining("nothere");
//            throw t;
//        }
//    }

//    @Test()
//    public void testKeyNotFound() throws Exception {
//
//        Dataset dataset = mock(Dataset.class);
//        when(dataset.getDataStructure()).thenReturn(
//                DataStructure.of((s, o) -> null, "notfound", Role.IDENTIFIER, String.class)
//        );
//
//        Throwable ex = null;
//        try {
//            RenameOperation renameOperation = new RenameOperation(dataset, ImmutableMap.of("1a", "1b"), Collections.emptyMap());
//            renameOperation.getDataStructure();
//        } catch (Throwable t) {
//            ex = t;
//        }
//        assertThat(ex).hasMessageContaining("1a");
//
//    }

    @Test
    public void testRename() throws Exception {

        Dataset dataset = mock(Dataset.class);

        DataStructure structure = DataStructure.of(
                "Ia", Role.IDENTIFIER, String.class,
                "Ma", Role.MEASURE, String.class,
                "Aa", Role.ATTRIBUTE, String.class
        );
        when(dataset.getDataStructure()).thenReturn(structure);

        Map<Component, String> newNames = ImmutableMap.of(
                structure.get("Ia"), "Ib",
                structure.get("Ma"), "Mb",
                structure.get("Aa"), "Ab"
        );

        RenameOperation rename;
        rename = new RenameOperation(
                dataset,
                newNames
        );

        assertThat(rename.getDataStructure().getRoles()).contains(
                entry("Ib", Role.IDENTIFIER),
                entry("Mb", Role.MEASURE),
                entry("Ab", Role.ATTRIBUTE)
        );

    }

    @Test
    public void testRenameAndCast() throws Exception {
        Dataset dataset = mock(Dataset.class);

        DataStructure structure = DataStructure.of(
                "Identifier1", Role.IDENTIFIER, String.class,
                "Identifier2", Role.IDENTIFIER, String.class,
                "Measure1", Role.MEASURE, String.class,
                "Measure2", Role.MEASURE, String.class,
                "Attribute1", Role.ATTRIBUTE, String.class,
                "Attribute2", Role.ATTRIBUTE, String.class
        );
        when(dataset.getDataStructure()).thenReturn(structure);
        when(dataset.getData()).then(invocation -> {
            return Stream.of(
                    structure.wrap(Maps.asMap(structure.keySet(), (String input) -> (Object) input))
            );
        });

        ImmutableMap<Component, String> newNames = new ImmutableMap.Builder<Component, String>()
                .put(structure.get("Identifier1"), "Identifier1Measure")
                .put(structure.get("Identifier2"), "Identifier2Attribute")
                .put(structure.get("Measure1"), "Measure1Identifier")
                .put(structure.get("Measure2"), "Measure2Attribute")
                .put(structure.get("Attribute1"), "Attribute1Identifier")
                .put(structure.get("Attribute2"), "Attribute2Measure")
                .build();

        ImmutableMap<Component, Role> newRoles = new ImmutableMap.Builder<Component, Role>()
                .put(structure.get("Identifier1"), Role.MEASURE)
                .put(structure.get("Identifier2"), Role.ATTRIBUTE)
                .put(structure.get("Measure1"), Role.IDENTIFIER)
                .put(structure.get("Measure2"), Role.ATTRIBUTE)
                .put(structure.get("Attribute1"), Role.IDENTIFIER)
                .put(structure.get("Attribute2"), Role.MEASURE)
                .build();

        RenameOperation rename;
        rename = new RenameOperation(dataset, newNames, newRoles);

        assertThat(rename.getDataStructure().getRoles()).contains(
                entry("Identifier1Measure", Role.MEASURE),
                entry("Identifier2Attribute", Role.ATTRIBUTE),
                entry("Measure1Identifier", Role.IDENTIFIER),
                entry("Measure2Attribute", Role.ATTRIBUTE),
                entry("Attribute1Identifier", Role.IDENTIFIER),
                entry("Attribute2Measure", Role.MEASURE)
        );

        assertThat(rename.getData()).flatExtracting(input -> input).extracting(VTLObject::get)
                .containsOnlyElementsOf(
                        structure.keySet()
                );
    }
}
