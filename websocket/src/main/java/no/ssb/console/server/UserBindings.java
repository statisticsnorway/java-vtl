package kohl.hadrien.console.server;

import org.springframework.stereotype.Component;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by hadrien on 17/11/16.
 */
@Component
//@Scope(WebApplicationContext.SCOPE_SESSION)
public class UserBindings {

    private static final String ENGINE_NAME = "VTLJava";
    private final ScriptEngine engine;

    public UserBindings() {
        ScriptEngineManager manager = new ScriptEngineManager();
        engine = checkNotNull(
                manager.getEngineByName(ENGINE_NAME),
                "could not get the engine %s", ENGINE_NAME
        );
    }
}
