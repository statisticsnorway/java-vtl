package no.ssb.vtl.script.visitors.join;

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

import no.ssb.vtl.connectors.Connector;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.script.VTLScriptEngine;
import org.junit.Before;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class FilterVisitorTest {

    private Dataset dataset = mock(Dataset.class);
    private Connector connector = mock(Connector.class);
    private ScriptEngine engine = new VTLScriptEngine(connector);
    private Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
    private Dataset ds1;
    private Dataset ds2;

    @Before
    public void setUp() throws Exception {
        ds1 = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("m1", Component.Role.MEASURE, Long.class)
                .addPoints("1", 10L)
                .addPoints("2", 100L)
                .build();

        ds2 = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("m1", Component.Role.MEASURE, Long.class)
                .addComponent("m2", Component.Role.MEASURE, Long.class)
                .addComponent("a1", Component.Role.ATTRIBUTE, String.class)
                .addPoints("1", 10L, 10L, "test")
                .addPoints("2", 100L, 10L, "2")
                .build();
    }

    @Test
    public void testSimpleBooleanFilter() throws Exception {
        bindings.put("ds1", ds1);


        engine.eval("" +
                "ds3 := [ds1]{" +
                "  filter id1 = \"1\" and m1 > 9" +
                "}" +
                "");


        assertThat(bindings).containsKey("ds3");
        assertThat(bindings.get("ds3")).isInstanceOf(Dataset.class);
        Dataset ds3 = (Dataset) bindings.get("ds3");

        assertThat(ds3.getData())
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        "1", 10L
                );
    }


    @Test
    public void testBooleanComponents() throws Exception {
        bindings.put("ds2", ds2);


        engine.eval("" +
                "ds3 := [ds2]{" +
                "  filter id1 = a1 or m1 > m2" +
                "}" +
                "");


        assertThat(bindings).containsKey("ds3");
        assertThat(bindings.get("ds3")).isInstanceOf(Dataset.class);
        Dataset ds3 = (Dataset) bindings.get("ds3");

        assertThat(ds3.getData())
                .flatExtracting(input -> input)
                .extracting(VTLObject::get)
                .containsExactly(
                        "2", 100L, 10L, "2"
                );

    }
}
