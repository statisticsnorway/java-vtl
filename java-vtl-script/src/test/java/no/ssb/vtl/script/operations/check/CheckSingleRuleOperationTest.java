package no.ssb.vtl.script.operations.check;

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
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;
import static no.ssb.vtl.script.operations.check.CheckSingleRuleOperation.CONDITION_LABEL;
import static no.ssb.vtl.script.operations.check.CheckSingleRuleOperation.ERROR_CODE_LABEL;
import static no.ssb.vtl.script.operations.check.CheckSingleRuleOperation.ERROR_LEVEL_LABEL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CheckSingleRuleOperationTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testArgumentDataset() throws Exception {
        assertThatThrownBy(() -> new CheckSingleRuleOperation.Builder(null).build())
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("dataset was null");
    }

    /**
     * NOTE: This is not in the spec for check with single rule, but exists for check with datapoint rulesets.
     * <p>
     * VTL 1.1, line 4799.
     */
    @Test
    public void testArgumentAllAndMeasuresToReturn() throws Exception {
        assertThatThrownBy(() ->
            new CheckSingleRuleOperation.Builder(mock(Dataset.class))
                .rowsToReturn(CheckSingleRuleOperation.RowsToReturn.ALL)
                .componentsToReturn(CheckSingleRuleOperation.ComponentsToReturn.MEASURES)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot use 'all' with 'measures'");
    }

    /**
     * The input Dataset must have all Boolean Measure Components.
     * NOTE: The spec is not consistent with regard to parameters and constraints sections (line 4925-4926 and 4949).
     * We decided to allow more than one Boolean Measure Component.
     * <p>
     * VTL 1.1 line 4949.
     */
    @Test
    public void testArgumentDatasetComponentsTooManyBooleans() throws Exception {
        Dataset dataset = mock(Dataset.class);
        when(dataset.getDataStructure()).thenReturn(
                DataStructure.of(
                        "id1", IDENTIFIER, String.class,
                        "me1", Component.Role.MEASURE, Boolean.class,
                        "me2", Component.Role.MEASURE, Boolean.class,
                        "at1", Component.Role.ATTRIBUTE, String.class
                )
        );

        new CheckSingleRuleOperation.Builder(dataset)
                .rowsToReturn(CheckSingleRuleOperation.RowsToReturn.VALID)
                .componentsToReturn(CheckSingleRuleOperation.ComponentsToReturn.MEASURES)
                .build();
    }

    @Test
    public void testCheckReturnMeasuresNotValidRows() throws Exception {
        //This data structure is a result of a boolean operation, so it will either have one CONDITION component
        //or more with "_CONDITION" suffix for each component.

        Dataset ds = StaticDataset.create()
                .withName("kommune_nr", "code", "measure", "CONDITION")
                .andRoles(IDENTIFIER,IDENTIFIER, MEASURE, MEASURE)
                .andTypes(String.class, String.class, String.class, Boolean.class)
                .addPoints("0101", "0101", "measure 0101", true)
                .addPoints("9990", null /* not in the code list, so a null value */, "measure 9990", false)
                .addPoints("0104", "0104", "measure 0104", true)
                .build();


        CheckSingleRuleOperation checkOperation = new CheckSingleRuleOperation.Builder(ds)
                .rowsToReturn(CheckSingleRuleOperation.RowsToReturn.NOT_VALID)
                .componentsToReturn(CheckSingleRuleOperation.ComponentsToReturn.MEASURES)
                .build();

        assertThat(checkOperation.getDataStructure().getRoles()).containsExactly(
                entry("kommune_nr", IDENTIFIER),
                entry("code", IDENTIFIER),
                entry("measure", Component.Role.MEASURE),
                entry(ERROR_CODE_LABEL, Component.Role.ATTRIBUTE)
        );

        Stream<DataPoint> stream = checkOperation.getData();
        assertThat(stream).isNotNull();

        List<DataPoint> collect = stream.collect(toList());

        assertThat(collect).hasSize(1);

        DataStructure dStructure = checkOperation.getDataStructure();

        //row 1
        Map<Component, VTLObject> map = dStructure.asMap(collect.get(0));
        assertThat(map.get(dStructure.get("kommune_nr")).get()).isEqualTo("9990");
        assertThat(map.get(dStructure.get("code")).get()).isEqualTo(null);
        assertThat(map.get(dStructure.get("measure")).get()).isEqualTo("measure 9990");
        assertThat(map.get(dStructure.get(ERROR_CODE_LABEL)).get()).isEqualTo(null);

    }

    @Test
    public void testCheckReturnMeasuresValidRows() throws Exception {
        //This data structure is a result of a boolean operation, so it will either has one CONDITION component
        //or more with "_CONDITION" suffix for each component.

        Dataset ds = StaticDataset.create()
                .withName("kommune_nr", "code", "measure", "CONDITION")
                .andRoles(IDENTIFIER, IDENTIFIER, MEASURE, MEASURE)
                .andTypes(String.class, String.class, String.class, Boolean.class)

                .addPoints("0101", "0101", "measure 0101", true)
                .addPoints("9990", null, "measure 9990", false)
                .addPoints("0104", "0104", "measure 0104", true)
                .build();


        CheckSingleRuleOperation checkOperation = new CheckSingleRuleOperation.Builder(ds)
                .rowsToReturn(CheckSingleRuleOperation.RowsToReturn.VALID)
                .componentsToReturn(CheckSingleRuleOperation.ComponentsToReturn.MEASURES)
                .build();

        assertThat(checkOperation.getDataStructure().getRoles()).contains(
                entry("kommune_nr", IDENTIFIER),
                entry("code", IDENTIFIER),
                entry("measure", Component.Role.MEASURE),
                entry(ERROR_CODE_LABEL, Component.Role.ATTRIBUTE)
        );

        Stream<DataPoint> stream = checkOperation.getData();
        assertThat(stream).isNotNull();

        List<DataPoint> collect = stream.collect(toList());

        assertThat(collect).hasSize(2);

        DataStructure dStructure = checkOperation.getDataStructure();

        //row 1
        Map<Component, VTLObject> map = dStructure.asMap(collect.get(0));
        assertThat(map.get(dStructure.get("kommune_nr")).get()).isEqualTo("0101");
        assertThat(map.get(dStructure.get("code")).get()).isEqualTo("0101");
        assertThat(map.get(dStructure.get("measure")).get()).isEqualTo("measure 0101");
        assertThat(map.get(dStructure.get(ERROR_CODE_LABEL)).get()).isEqualTo(null);

        //row 2
        map = dStructure.asMap(collect.get(1));
        assertThat(map.get(dStructure.get("kommune_nr")).get()).isEqualTo("0104");
        assertThat(map.get(dStructure.get("code")).get()).isEqualTo("0104");
        assertThat(map.get(dStructure.get("measure")).get()).isEqualTo("measure 0104");
        assertThat(map.get(dStructure.get(ERROR_CODE_LABEL)).get()).isEqualTo(null);
    }

    @Test
    public void testCheckReturnConditionNotValidRows() throws Exception {
        //This data structure is a result of a boolean operation, so it will either has one CONDITION component
        //or more with "_CONDITION" suffix for each component.
        //No attribute components as the VTL 1.1 does not specify that.

        Dataset ds = StaticDataset.create()
                .withName("kommune_nr", "code", "CONDITION_CONDITION", "booleanMeasure_CONDITION", "stringMeasure")
                .andRoles(IDENTIFIER, IDENTIFIER, MEASURE, MEASURE, MEASURE)
                .andTypes(String.class, String.class, Boolean.class, Boolean.class, String.class)

                .addPoints("0101", "0101", true, true, "t1")
                .addPoints("9990", null, true, false, "t2")
                .addPoints("0104", null /* not in the code list, so a null value */, false, false, "t3")
                .build();

        CheckSingleRuleOperation checkOperation = new CheckSingleRuleOperation.Builder(ds)
                .rowsToReturn(CheckSingleRuleOperation.RowsToReturn.NOT_VALID)
                .componentsToReturn(CheckSingleRuleOperation.ComponentsToReturn.CONDITION)
                .build();

        assertThat(checkOperation.getDataStructure().getRoles()).contains(
                entry("kommune_nr", IDENTIFIER),
                entry("code", IDENTIFIER),
                entry(CONDITION_LABEL, Component.Role.MEASURE),   //new component, result of CONDITION_CONDITION && booleanMeasure_CONDITION
                entry(ERROR_CODE_LABEL, Component.Role.ATTRIBUTE)  //new component
        );

        Stream<DataPoint> stream = checkOperation.getData();
        assertThat(stream).isNotNull();

        List<DataPoint> collect = stream.collect(toList());

        assertThat(collect).hasSize(2);

        DataStructure dStructure = checkOperation.getDataStructure();

        //row 1
        Map<Component, VTLObject> map = dStructure.asMap(collect.get(0));
        assertThat(map.get(dStructure.get("kommune_nr")).get()).isEqualTo("9990");
        assertThat(map.get(dStructure.get("code")).get()).isEqualTo(null);
        assertThat(map.get(dStructure.get(CONDITION_LABEL)).get()).isEqualTo(false);
        assertThat(map.get(dStructure.get(ERROR_CODE_LABEL)).get()).isEqualTo(null);

        //row 2
        map = dStructure.asMap(collect.get(1));
        assertThat(map.get(dStructure.get("kommune_nr")).get()).isEqualTo("0104");
        assertThat(map.get(dStructure.get("code")).get()).isEqualTo(null);
        assertThat(map.get(dStructure.get(CONDITION_LABEL)).get()).isEqualTo(false);
        assertThat(map.get(dStructure.get(ERROR_CODE_LABEL)).get()).isEqualTo(null);

    }

    @Test
    public void testCheckReturnConditionValidRows() throws Exception {
        //This data structure is a result of a boolean operation, so it will either has one CONDITION component
        //or more with "_CONDITION" suffix for each component.
        //No attribute components as the VTL 1.1 does not specify that.

        Dataset ds = StaticDataset.create()
                .withName("kommune_nr", "code", "CONDITION_CONDITION", "booleanMeasure_CONDITION", "stringMeasure")
                .andRoles(IDENTIFIER, IDENTIFIER, MEASURE, MEASURE, MEASURE)
                .andTypes(String.class, String.class, Boolean.class, Boolean.class, String.class)
                .addPoints("0101", "0101", true, true, "t1")
                .addPoints("9990", null, true, false, "t2")
                .addPoints("0104", null /*not in the code list, so a null value*/, false, false, "t3")
                .build();

        CheckSingleRuleOperation checkOperation = new CheckSingleRuleOperation.Builder(ds)
                .rowsToReturn(CheckSingleRuleOperation.RowsToReturn.VALID)
                .componentsToReturn(CheckSingleRuleOperation.ComponentsToReturn.CONDITION)
                .errorCode("error001")
                .errorLevel(10L)
                .build();

        assertThat(checkOperation.getDataStructure().getRoles()).containsExactly(
                entry("kommune_nr", IDENTIFIER),
                entry("code", IDENTIFIER),
                entry(CONDITION_LABEL, Component.Role.MEASURE),    //new component, result of CONDITION_CONDITION && booleanMeasure_CONDITION
                entry(ERROR_CODE_LABEL, Component.Role.ATTRIBUTE), //new component
                entry(ERROR_LEVEL_LABEL, Component.Role.ATTRIBUTE) //new component
        );

        Stream<DataPoint> stream = checkOperation.getData();
        assertThat(stream).isNotNull();

        List<DataPoint> collect = stream.collect(toList());

        assertThat(collect).hasSize(1);

        DataStructure dStructure = checkOperation.getDataStructure();

        //row 1
        Map<Component, VTLObject> map = dStructure.asMap(collect.get(0));
        assertThat(map.get(dStructure.get("kommune_nr")).get()).isEqualTo("0101");
        assertThat(map.get(dStructure.get("code")).get()).isEqualTo("0101");
        assertThat(map.get(dStructure.get(CONDITION_LABEL)).get()).isEqualTo(true);
        assertThat(map.get(dStructure.get(ERROR_CODE_LABEL)).get()).isEqualTo("error001");
        assertThat(map.get(dStructure.get(ERROR_LEVEL_LABEL)).get()).isEqualTo(10L);

    }

}
