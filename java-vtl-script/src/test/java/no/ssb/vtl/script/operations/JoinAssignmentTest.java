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

import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLBoolean;
import no.ssb.vtl.model.VTLDate;
import no.ssb.vtl.model.VTLExpression;
import no.ssb.vtl.model.VTLFloat;
import no.ssb.vtl.model.VTLInteger;
import no.ssb.vtl.model.VTLNumber;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.model.VTLString;
import no.ssb.vtl.script.operations.join.ComponentBindings;
import org.assertj.core.api.JUnitSoftAssertions;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.script.Bindings;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.spy;

public class JoinAssignmentTest {
    @Rule
    public final JUnitSoftAssertions softly = new JUnitSoftAssertions();
    private VTLExpression expression;
    private Dataset dataset;

    @Before
    public void setUp() {
        expression = new VTLExpression() {
            @Override
            public VTLObject resolve(Bindings bindings) {
                return VTLObject.of("changed");
            }

            @Override
            public Class<?> getVTLType() {
                return VTLString.class;
            }

        };

        DataStructure structure = DataStructure.builder()
                .put("id", Component.Role.IDENTIFIER, String.class)
                .put("measure", Component.Role.MEASURE, String.class)
                .put("attr", Component.Role.ATTRIBUTE, String.class).build();

        DataPoint dataPoint = DataPoint.create(3);
        dataPoint.set(0, VTLObject.of("idValue"));
        dataPoint.set(0, VTLObject.of("measureValue"));
        dataPoint.set(0, VTLObject.of("attrValue"));

        dataset = StaticDataset.create(structure)
                .addPoints(dataPoint)
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

        JoinAssignment resultBooleanNull = new JoinAssignment(
                dataset,
                expression,
                "test",
                Component.Role.MEASURE,
                false,
                new ComponentBindings(dataset)
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

    @Test
    public void testTypeConversion() throws Exception {
        ImmutableMap<Class<? extends VTLObject>, Class<?>> types = ImmutableMap.<Class<? extends VTLObject>, Class<?>>builder()
                .put(VTLNumber.class, Number.class)
                .put(VTLInteger.class, Long.class)
                .put(VTLFloat.class, Double.class)
                .put(VTLString.class, String.class)
                .put(VTLBoolean.class, Boolean.class)
                .put(VTLDate.class, Instant.class)
                .build();

        for (Map.Entry<Class<? extends VTLObject>, Class<?>> entry : types.entrySet()) {
            Class<? extends VTLObject> vtlType = entry.getKey();
            Class<?> expectedType = entry.getValue();
            assertThat(JoinAssignment.convertToComponentType(vtlType)).isEqualTo(expectedType);
        }
    }

    @Test
    public void testIdentifierFails() throws Exception {
        /*
         * Replacing identifier is not allowed.
         */
        List<JoinAssignment> operations = Lists.newArrayList(
                new JoinAssignment(dataset, expression, "id", Component.Role.IDENTIFIER, true),
                new JoinAssignment(dataset, expression, "id", Component.Role.ATTRIBUTE, true),
                new JoinAssignment(dataset, expression, "id", Component.Role.MEASURE, true),
                new JoinAssignment(dataset, expression, "id", Component.Role.IDENTIFIER, false),
                new JoinAssignment(dataset, expression, "id", Component.Role.ATTRIBUTE, false),
                new JoinAssignment(dataset, expression, "id", Component.Role.MEASURE, false)
        );

        for (JoinAssignment operation : operations) {
            softly.assertThatThrownBy(operation::getDataStructure)
                    .hasMessageContaining("id");
        }
    }

    // When using implicit the role must be the same if the component is already present.
    @Test
    public void testImplicitFails() throws Exception {

        List<JoinAssignment> operations = Lists.newArrayList(
                new JoinAssignment(dataset, expression, "measure", Component.Role.MEASURE, true),
                new JoinAssignment(dataset, expression, "attr", Component.Role.ATTRIBUTE, true),
                new JoinAssignment(dataset, expression, "newMeasure", Component.Role.MEASURE, true),
                new JoinAssignment(dataset, expression, "newAttr", Component.Role.ATTRIBUTE, true)
        );

        List<JoinAssignment> operationsFailing = Lists.newArrayList(
                new JoinAssignment(dataset, expression, "measure", Component.Role.ATTRIBUTE, true),
                new JoinAssignment(dataset, expression, "attr", Component.Role.MEASURE, true)
        );

        for (JoinAssignment operation : operations) {
            operation.getDataStructure();
        }

        for (JoinAssignment operation : operationsFailing) {
            softly.assertThatThrownBy(operation::getDataStructure)
                    .hasMessageStartingWith("the role of the component");
        }
    }

    // Measure and attribute roles can be changed.
    @Test
    public void testChangeRoles() throws Exception {

        JoinAssignment operation;

        operation = new JoinAssignment(dataset, expression, "measure", Component.Role.ATTRIBUTE, false);

        softly.assertThat(operation.getDataStructure()).containsKey("measure");
        softly.assertThat(operation.getDataStructure().get("measure").getRole()).isEqualTo(Component.Role.ATTRIBUTE);

        operation = new JoinAssignment(dataset, expression, "attr", Component.Role.MEASURE, false);

        softly.assertThat(operation.getDataStructure()).containsKey("attr");
        softly.assertThat(operation.getDataStructure().get("attr").getRole()).isEqualTo(Component.Role.MEASURE);

    }

    @Test
    public void testNewRoles() throws Exception {

        JoinAssignment operation;

        operation = new JoinAssignment(dataset, expression, "newMeasure", Component.Role.MEASURE, false);
        softly.assertThat(operation.getDataStructure()).containsKey("newMeasure");
        softly.assertThat(operation.getDataStructure().get("newMeasure").getRole()).isEqualTo(Component.Role.MEASURE);

        operation = new JoinAssignment(dataset, expression, "newAttr", Component.Role.ATTRIBUTE, false);
        softly.assertThat(operation.getDataStructure()).containsKey("newAttr");
        softly.assertThat(operation.getDataStructure().get("newAttr").getRole()).isEqualTo(Component.Role.ATTRIBUTE);

        operation = new JoinAssignment(dataset, expression, "newId", Component.Role.IDENTIFIER, false);
        softly.assertThat(operation.getDataStructure()).containsKey("newId");
        softly.assertThat(operation.getDataStructure().get("newId").getRole()).isEqualTo(Component.Role.IDENTIFIER);

    }

}
