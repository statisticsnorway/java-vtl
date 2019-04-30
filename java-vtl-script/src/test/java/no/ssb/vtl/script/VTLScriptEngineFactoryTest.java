package no.ssb.vtl.script;

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

import org.junit.Before;
import org.junit.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

import static org.assertj.core.api.Assertions.assertThat;

public class VTLScriptEngineFactoryTest {

    private ScriptEngineManager factory;

    @Before
    public void setUp() {
        // create a script engine manager
        factory = new ScriptEngineManager();
    }

    @Test
    public void testLoadByName() throws Exception {
        ScriptEngine vtl = factory.getEngineByName("VTLJava");
        assertThat(vtl).isInstanceOf(VTLScriptEngine.class);
    }

    @Test
    public void testLoadByExtension() throws Exception {
        ScriptEngine vtl = factory.getEngineByExtension("vtl");
        assertThat(vtl).isInstanceOf(VTLScriptEngine.class);
    }

    @Test
    public void testLoadByMimeType() throws Exception {
        ScriptEngine vtl = factory.getEngineByMimeType("text/x-vtl");
        assertThat(vtl).isInstanceOf(VTLScriptEngine.class);
    }

    @Test
    public void testFactoriesContainVTL() throws Exception {
        assertThat(factory.getEngineFactories()).extracting(ScriptEngineFactory::getEngineName).contains(
                "VTLJava"
        );
    }
}
