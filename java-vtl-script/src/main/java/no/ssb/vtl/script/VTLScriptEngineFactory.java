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

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import java.util.Collections;
import java.util.List;

public class VTLScriptEngineFactory implements ScriptEngineFactory {

    @Override
    public String getEngineName() {
        return "VTLJava";
    }

    @Override
    public String getEngineVersion() {
        return "0.1";
    }

    @Override
    public List<String> getExtensions() {
        return Collections.singletonList("vtl");
    }

    @Override
    public List<String> getMimeTypes() {
        return Collections.singletonList("text/x-vtl");
    }

    @Override
    public List<String> getNames() {
        return Collections.singletonList("VTLJava");
    }

    @Override
    public String getLanguageName() {
        return "VTL";
    }

    @Override
    public String getLanguageVersion() {
        return "1";
    }

    @Override
    public Object getParameter(String key) {
        switch (key) {
            case ScriptEngine.ENGINE:
                return getEngineName();
            case ScriptEngine.ENGINE_VERSION:
                return getEngineVersion();
            case ScriptEngine.LANGUAGE:
                return getLanguageName();
            case ScriptEngine.LANGUAGE_VERSION:
                return getLanguageVersion();
            case ScriptEngine.NAME:
                return getNames().get(0);
            default:
                return null;
        }
    }

    @Override
    public String getMethodCallSyntax(String obj, String m, String... args) {
        return null;
    }

    @Override
    public String getOutputStatement(String toDisplay) {
        return null;
    }

    @Override
    public String getProgram(String... statements) {
        return null;
    }

    @Override
    public ScriptEngine getScriptEngine() {
        return new VTLScriptEngine();
    }
}
