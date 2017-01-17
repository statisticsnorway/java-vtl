package kohl.hadrien.vtl.script.operations;

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
import kohl.hadrien.vtl.model.Component;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.Stream;

import static kohl.hadrien.vtl.model.Component.Role;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RenameOperationTest {

    Dataset notNullDataset = new Dataset() {
        @Override
        public DataStructure getDataStructure() {
            return null;
        }

        @Override
        public Stream<Tuple> get() {
            return null;
        }
    };

    @Test(expected = IllegalArgumentException.class)
    public void testNotDuplicates() throws Exception {
        ImmutableMap<String, String> names = ImmutableMap.of("a1", "a2", "b1", "a2");
        try {
            new RenameOperation(notNullDataset, names, Collections.emptyMap());
        } catch (Throwable t) {
            assertThat(t).hasMessageContaining("a2");
            throw t;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConsistentArguments() throws Exception {
        ImmutableMap<String, String> names = ImmutableMap.of("a1", "a2", "b1", "b2");
        ImmutableMap<String, Component.Role> roles;
        roles = ImmutableMap.of("nothere", Role.IDENTIFIER);
        try {
            new RenameOperation(notNullDataset, names, roles);
        } catch (Throwable t) {
            assertThat(t).hasMessageContaining("nothere");
            throw t;
        }
    }

    @Test()
    public void testKeyNotFound() throws Exception {

        Dataset dataset = mock(Dataset.class);
        when(dataset.getDataStructure()).thenReturn(
                DataStructure.of((s, o) -> null, "notfound", Role.IDENTIFIER, String.class)
        );

        Throwable ex = null;
        try {
            RenameOperation renameOperation = new RenameOperation(dataset, ImmutableMap.of("1a", "1b"), Collections.emptyMap());
            renameOperation.getDataStructure();
        } catch (Throwable t) {
            ex = t;
        }
        assertThat(ex).hasMessageContaining("1a");

    }

    @Test
    public void testRename() throws Exception {

        Dataset dataset = mock(Dataset.class);

        when(dataset.getDataStructure()).thenReturn(
                DataStructure.of((s, o) -> null,
                        "Ia", Role.IDENTIFIER, String.class,
                        "Ma", Role.MEASURE, String.class,
                        "Aa", Role.ATTRIBUTE, String.class
                )
        );

        RenameOperation rename;
        rename = new RenameOperation(
                dataset,
                ImmutableMap.of(
                        "Ia", "Ib",
                        "Ma", "Mb",
                        "Aa", "Ab"
                ), Collections.emptyMap()
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

        when(dataset.getDataStructure()).thenReturn(DataStructure.of((o, aClass) -> o,
                "Identifier1", Role.IDENTIFIER, Object.class,
                "Identifier2", Role.IDENTIFIER, Object.class,
                "Measure1", Role.MEASURE, Object.class,
                "Measure2", Role.MEASURE, Object.class,
                "Attribute1", Role.ATTRIBUTE, Object.class,
                "Attribute2", Role.ATTRIBUTE, Object.class
        ));

        RenameOperation rename;
        rename = new RenameOperation(
                dataset,
                new ImmutableMap.Builder<String, String>()
                        .put("Identifier1", "Identifier1Measure")
                        .put("Identifier2", "Identifier2Attribute")
                        .put("Measure1", "Measure1Identifier")
                        .put("Measure2", "Measure2Attribute")
                        .put("Attribute1", "Attribute1Identifier")
                        .put("Attribute2", "Attribute2Measure")
                        .build()
                , new ImmutableMap.Builder<String, Role>()
                .put("Identifier1", Role.MEASURE)
                .put("Identifier2", Role.ATTRIBUTE)
                .put("Measure1", Role.IDENTIFIER)
                .put("Measure2", Role.ATTRIBUTE)
                .put("Attribute1", Role.IDENTIFIER)
                .put("Attribute2", Role.MEASURE).build()
        );

        assertThat(rename.getDataStructure().getRoles()).contains(
                entry("Identifier1Measure", Role.MEASURE),
                entry("Identifier2Attribute", Role.ATTRIBUTE),
                entry("Measure1Identifier", Role.IDENTIFIER),
                entry("Measure2Attribute", Role.ATTRIBUTE),
                entry("Attribute1Identifier", Role.IDENTIFIER),
                entry("Attribute2Measure", Role.MEASURE)
        );

    }
}
