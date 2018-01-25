package no.ssb.vtl.tools.rest.controllers;

/*-
 * ========================LICENSE_START=================================
 * Java VTL Utility connectors
 * %%
 * Copyright (C) 2017 Statistics Norway and contributors
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

import no.ssb.vtl.connectors.PxApiConnector;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.script.VTLScriptEngine;
import no.ssb.vtl.tools.rest.representations.ExecutionRepresentation;
import no.ssb.vtl.tools.rest.representations.ResultRepresentation;
import org.assertj.core.api.SoftAssertions;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.*;

public class ExecutorControllerTest {
    
    private ExecutorController controller;
    
    @Before
    public void setUp() throws Exception {
        PxApiConnector connector = mock(PxApiConnector.class);
        when(connector.canHandle(any())).thenReturn(true);
        when(connector.getDataset("a")).thenReturn(StaticDataset.create()
                .addComponent("region", Component.Role.IDENTIFIER, String.class)
                .addComponent("dividend", Component.Role.MEASURE, Double.class)
                .addComponent("dividend2", Component.Role.MEASURE, Double.class)
                .addPoints("0101", 10.0, 90.0)
                .addPoints("0102", 20.0, 80.0)
                .addPoints("0218", 30.0, 70.0)
                .addPoints("0219", 40.0, 60.0)
                .build());
        when(connector.getDataset("b")).thenReturn(StaticDataset.create()
                .addComponent("region", Component.Role.IDENTIFIER, String.class)
                .addComponent("divisor", Component.Role.MEASURE, Double.class)
                .addPoints("0101", 4.0)
                .addPoints("0102", 3.0)
                .addPoints("0218", 2.0)
                .addPoints("0219", 1.0)
                .build());
        controller = new ExecutorController(new VTLScriptEngine(connector));
    }
    
    @Test
    public void customAggregateRatioExecution() throws Exception {
        
        ExecutionRepresentation params = new ExecutionRepresentation();
        String expression =
                "a := get(\"a\") \n" +
                "b := get(\"b\") \n" +
                "fa := [a]{filter region = \"0101\" or region = \"0219\"} \n" +
                "fb := [b]{filter region = \"0101\" or region = \"0219\"} \n" +
                "sa := sum(fa) along region \n" +
                "sb := sum(fb) along region \n" +
                "t1 := [sa]{identifier region := \"ekg\"} \n" +
                "t2 := [sb]{identifier region := \"ekg\"} \n" +
                "res := [t1, t2]{ var := t1.dividend / t2.divisor, var2 := t1.dividend2 / t2.divisor, keep var, var2}";
        params.setExpression(expression);

        ResultRepresentation result = (ResultRepresentation) controller.execute(params);
    
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(result.getResultDataset().getStructure())
                .extracting("name", "role", "type")
                .contains(
                        tuple("var", Component.Role.MEASURE, Double.class),
                        tuple("var2", Component.Role.MEASURE, Double.class),
                        tuple("region", Component.Role.IDENTIFIER, String.class)
                );

        //noinspection unchecked
        softly.assertThat(result.getResultDataset().getData())
                .contains(Arrays.asList("ekg", 10.0d, 30.0d));

        softly.assertAll();
    }

}