package no.ssb.vtl.script.operations;

import no.ssb.vtl.script.VTLScriptEngine;
import org.junit.Before;
import org.junit.Test;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import static org.assertj.core.api.Assertions.assertThat;

public class VTLScriptEngineFactoryTest {

    private ScriptEngineManager factory;

    @Before
    public void setUp() throws Exception {
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
        assertThat(factory.getEngineFactories());
    }
}
