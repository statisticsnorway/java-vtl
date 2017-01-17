package kohl.hadrien.vtl.script;

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
