package no.ssb.vtl.script;

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

import no.ssb.vtl.script.error.VTLCompileException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class VTLErrorsTest {

    @Test
    public void testErrorRecovery() {

        // @formatter:off
        String script = "variable := nvl(bla\"error\") // normal syntax error\n" +
                        "variable := nvl(\"string\", \"\")   // type error\n" +
                        "variable := round(0)             // missing argument\n";
        // @formatter:on

        VTLScriptEngine engine = new VTLScriptEngine();
        assertThatThrownBy(() -> {
            engine.eval(script);
        })
                .as("engine exception")
                .isInstanceOf(VTLCompileException.class)
                .hasMessageContaining("undefined variable bla")
                .hasMessageContaining("missing ','")
                .hasMessageContaining("missing argument(s): decimals");
    }

    @Test
    public void testUndefinedFunction() {

        // @formatter:off
        String script = "variable := notReallyAFunction(1,2,3)";
        // @formatter:on

        VTLScriptEngine engine = new VTLScriptEngine();
        assertThatThrownBy(() -> {
            engine.eval(script);
        })
                .as("engine exception")
                .isInstanceOf(VTLCompileException.class)
                .hasMessageContaining("undefined function")
                .hasMessageContaining("notReallyAFunction");
    }
}
