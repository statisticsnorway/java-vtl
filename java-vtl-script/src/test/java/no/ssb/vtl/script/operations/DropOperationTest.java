package no.ssb.vtl.script.operations;

/*
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import com.google.common.collect.Sets;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.StaticDataset;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.spy;

public class DropOperationTest {

    private Dataset dataset;

    @Before
    public void setUp() throws Exception {
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

        DropOperation resultBooleanNull = new DropOperation(
                dataset,
                Sets.newHashSet(this.dataset.getDataStructure().get("m1"))
        );

        Order order = Order.createDefault(this.dataset.getDataStructure());
        Dataset.Filtering filter = Dataset.Filtering.ALL;
        Set<String> components = this.dataset.getDataStructure().keySet();

        resultBooleanNull.getData(
                order,
                filter,
                components
        );

        assertThat(orderCapture.getValue()).isSameAs(order);
        assertThat(filterCapture.getValue()).isSameAs(filter);
        assertThat(componentsCapture.getValue()).isSameAs(components);


    }

}
