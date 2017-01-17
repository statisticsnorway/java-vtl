package kohl.hadrien.console.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import kohl.hadrien.vtl.script.VTLScriptEngine;
import no.ssb.vtl.connectors.SsbApiConnector;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.test.context.web.WebAppConfiguration;

import javax.script.Bindings;

/**
 * Spring application
 */
@SpringBootApplication
@WebAppConfiguration
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public VTLScriptEngine getVTLEngine(ObjectMapper mapper) {
        //ScriptEngineManager factory = new ScriptEngineManager();
        //VTLScriptEngine engine = (VTLScriptEngine) factory.getEngineByMimeType("text/x-vtl");

        SsbApiConnector ssbApiConnector = new SsbApiConnector(mapper);
        VTLScriptEngine engine = new VTLScriptEngine(ssbApiConnector);
        return engine;
    }

    @Bean(name = "vtlBindings")
    @Scope(value = "session", proxyMode = ScopedProxyMode.TARGET_CLASS)
    public Bindings getBindings(VTLScriptEngine vtlEngine) {
        return vtlEngine.createBindings();
    }

}
