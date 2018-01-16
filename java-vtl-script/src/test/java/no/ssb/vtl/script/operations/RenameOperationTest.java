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
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

public class RenameOperationTest {

    Dataset dataset;
    @Before

    public void setUp() {
        dataset = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("id2", Component.Role.IDENTIFIER, String.class)
                .addComponent("m1", Component.Role.MEASURE, String.class)
                .addComponent("m2", Component.Role.MEASURE, String.class)
                .addPoints("id1-1", "id2-1", "m1-1", "m2-1")
                .addPoints("id1-2", "id2-2", "m1-2", "m2-2")
                .addPoints("id1-3", "id2-3", "m1-3", "m2-3")
                .addPoints("id1-4", "id2-4", "m1-4", "m2-4")
                .build();
    }

    @Test
    public void testOrder() {

        Dataset dataset = spy(this.dataset);

        ArgumentCaptor<Order> orderCapture = ArgumentCaptor.forClass(Order.class);
        ArgumentCaptor<Dataset.Filtering> filterCapture = ArgumentCaptor.forClass(Dataset.Filtering.class);
        ArgumentCaptor<Set> componentsCapture = ArgumentCaptor.forClass(Set.class);


        doCallRealMethod().when(dataset).getData(
                orderCapture.capture(),
                filterCapture.capture(),
                componentsCapture.capture()
        );

        DataStructure structure = dataset.getDataStructure();
        RenameOperation renameOperation = new RenameOperation(
                dataset,
                ImmutableMap.of(structure.get("m1"), "renamed")
        );

        Order order = Order.createDefault(this.dataset.getDataStructure());
        Dataset.Filtering filter = Dataset.Filtering.ALL;
        Set<String> components = this.dataset.getDataStructure().keySet();

        renameOperation.getData(
                order,
                filter,
                components
        );

        // Checks that the order uses the old name.
        assertThat(orderCapture.getValue()).containsKeys(
                structure.get("m1")
        );
        assertThat(orderCapture.getValue()).doesNotContainKeys(
                renameOperation.getDataStructure().get("renamed")
        );

        assertThat(filterCapture.getValue()).isSameAs(filter);
        assertThat(componentsCapture.getValue()).isSameAs(components);
    }

    @Test
    public void testRename() {

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
