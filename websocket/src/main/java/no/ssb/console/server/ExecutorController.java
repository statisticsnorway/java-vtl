package kohl.hadrien.console.server;

import com.codepoetics.protonpack.Streamable;
import com.google.common.collect.Maps;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.Dataset;
import kohl.hadrien.vtl.script.VTLScriptEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.script.Bindings;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import java.util.Set;

/**
 * Simple execution service that allows executions of a VTL expression
 * and returns the context.
 */
@RequestMapping(
        produces = MediaType.APPLICATION_JSON_UTF8_VALUE,
        consumes = MediaType.ALL_VALUE
)
@RestController
public class ExecutorController {

    @Autowired
    public VTLScriptEngine vtlEngine;

    @Autowired
    @Qualifier("vtlBindings")
    public Bindings bindings;

    @RequestMapping(
            path = "/execute",
            method = RequestMethod.POST
    )
    public Set<String> execute(Reader script) throws IOException, ScriptException {
        vtlEngine.eval(script, bindings);
        return bindings.keySet();
    }

    @RequestMapping(
            path = "/dataset/{id}/structure",
            method = RequestMethod.GET
    )
    public Object getStructure(@PathVariable String id) {
        return bindings.get(id);
    }

    @RequestMapping(
            path = "/dataset/{id}",
            method = RequestMethod.DELETE
    )
    public ResponseEntity<Void> delete(@PathVariable String id) {
        if (bindings.containsKey(id)) {
            bindings.remove(id);
            return ResponseEntity.ok(null);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    @RequestMapping(
            path = "/dataset/{id}/data",
            method = RequestMethod.GET
    )
    public Iterable<Map<String, Object>> getData(@PathVariable String id) {
        Object dataset = bindings.get(id);
        Streamable<Map<String, Object>> streamable = ((Dataset) dataset).map(dataPoints -> {
            Map<String, Object> map = Maps.newHashMap();
            for (DataPoint dataPoint : dataPoints) {
                map.put(dataPoint.getName(), dataPoint.get());
            }
            return map;
        });
        return () -> streamable.get().iterator();
    }
}
