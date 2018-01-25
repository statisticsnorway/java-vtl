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

import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLBoolean;
import no.ssb.vtl.model.VTLExpression;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.script.operations.join.ComponentBindings;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.script.Bindings;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.spy;

public class FilterOperationTest {

    private Dataset dataset;
    private ComponentBindings componentBindings;

    private static VTLExpression NULL = new VTLExpression() {
        @Override
        public VTLObject resolve(Bindings bindings) {
            return VTLObject.of((VTLObject) null);
        }

        @Override
        public Class getVTLType() {
            return VTLBoolean.class;
        }
    };

    private static VTLExpression FALSE = new VTLExpression() {
        @Override
        public VTLObject resolve(Bindings bindings) {
            return VTLBoolean.of(false);
        }

        @Override
        public Class getVTLType() {
            return VTLBoolean.class;
        }
    };

    private static VTLExpression TRUE = new VTLExpression() {
        @Override
        public VTLObject resolve(Bindings bindings) {
            return VTLBoolean.of(true);
        }

        @Override
        public Class getVTLType() {
            return VTLBoolean.class;
        }
    };

    @Before
    public void setUp() {
        dataset = StaticDataset.create()
                .addComponent("id", Component.Role.IDENTIFIER, String.class)
                .addComponent("mc", Component.Role.MEASURE, String.class)
                .addPoints("idValue", null)
                .build();
        componentBindings = new ComponentBindings(dataset);
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

        FilterOperation resultBooleanNull = new FilterOperation(dataset, TRUE, componentBindings);

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

    @Test
    public void testPredicateReturnsNull() {

        FilterOperation resultBooleanNull = new FilterOperation(dataset, NULL, componentBindings);
        assertThat(resultBooleanNull.getData()).isEmpty();
    }

    @Test
    public void testPredicateReturnsFalse() {

        FilterOperation result = new FilterOperation(dataset, FALSE, componentBindings);
        assertThat(result.getData()).isEmpty();
    }


    @Test
    public void testPredicateReturnsTrue() {

        FilterOperation result = new FilterOperation(dataset, TRUE, componentBindings);
        assertThat(result.getData()).isNotEmpty();
    }
}

