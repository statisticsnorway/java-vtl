package kohl.hadrien.vtl.script;

/*-
 * #%L
 * java-vtl-script
 * %%
 * Copyright (C) 2016 Hadrien Kohl
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
 * #L%
 */

import com.google.common.collect.ImmutableMap;
import kohl.hadrien.vtl.connector.Connector;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import org.assertj.core.api.ListAssert;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static kohl.hadrien.vtl.model.Component.Role;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VTLScriptEngineTest {

    private Dataset dataset = mock(Dataset.class);
    private Connector connector = mock(Connector.class);
    private ScriptEngine engine = new VTLScriptEngine(connector);
    private Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);

    @Test
    public void testAssignment() throws Exception {

        bindings.put("ds1", dataset);
        engine.eval("ds2 := ds1");

        assertThat(bindings).contains(entry("ds2", dataset));

    }

    @Test
    public void testGet() throws Exception {

        when(connector.canHandle(anyString())).thenReturn(true);
        when(connector.getDataset(anyString())).thenReturn(dataset);

        bindings.put("ds1", dataset);
        engine.eval("ds2 := get(\"todo\")");

        assertThat(bindings).contains(entry("ds2", dataset));

    }

    @Test
    public void testPut() throws Exception {

        when(connector.canHandle(anyString())).thenReturn(true);
        when(connector.putDataset(anyString(), any())).thenReturn(dataset);
        engine.eval("ds1 := put(todo)");

        assertThat(bindings).contains(entry("ds1", dataset));

    }

    @Test
    public void testJoin() throws Exception {

        Dataset ds1 = mock(Dataset.class);
        Dataset ds2 = mock(Dataset.class);

        DataStructure ds = DataStructure.of(
                (o, aClass) -> o,
                "id1", Role.IDENTIFIER, String.class,
                "id2", Role.IDENTIFIER, String.class,
                "m1", Role.MEASURE, Integer.class,
                "m2", Role.MEASURE, Double.class,
                "at1", Role.MEASURE, String.class
        );
        when(ds1.getDataStructure()).thenReturn(ds);
        when(ds2.getDataStructure()).thenReturn(ds);

        when(ds1.get()).then(invocation -> {
            return Stream.of(
                    ds.wrap(ImmutableMap.of(
                            "id1", "1",
                            "id2", "1",
                            "m1", 10,
                            "m2", 20,
                            "at1", "attr1"
                    ))
            );
        });

        when(ds2.get()).then(invocation -> {
            return Stream.of(
                    ds.wrap(ImmutableMap.of(
                            "id1", "1",
                            "id2", "1",
                            "m1", 30,
                            "m2", 40,
                            "at1", "attr1"
                    ))
            );
        });

        bindings.put("ds1", ds1);
        bindings.put("ds2", ds2);

        engine.eval("" +
                "ds3 := [ds1, ds2]{" +
                "  ident = ds1.m1 + ds2.m2 - ds1.m2 - ds2.m1," +
                "  keep ident, ds1.m1, ds2.m1, ds2.m2," +           // id1, id2, ident, ds1.m1, ds2.m1, ds2.m2
                "  drop ds2.m1," +                                  // id1, id2, ident, ds1.m1, ds2.m2
                "  rename id1 to renamedId1, ds1.m1 to m1" +        // renamedId1, id2, ident, m1, ds2.m2
                "}" +
                "");

        assertThat(bindings).containsKey("ds3");


        assertThat(bindings.get("ds3")).isInstanceOf(Dataset.class);
        Dataset ds3 = (Dataset) bindings.get("ds3");
        assertThat(ds3.getDataStructure()).containsOnlyKeys(
                "renamedId1", "id2", "ds2.m2", "m1", "ident"
        );
        ListAssert<DataPoint> datapoints = assertThat(ds3.get())
                .flatExtracting(input -> input);

        datapoints.extracting(DataPoint::getName).containsExactly(
                "renamedId1", "id2", "ds2.m2", "m1", "ident"
        );

        datapoints.extracting(DataPoint::get).containsExactly(
                "1", "1", 40, 10, 0
        );


    }

    @Test
    public void testRename() throws Exception {

        when(dataset.getDataStructure()).thenReturn(
                DataStructure.of((s, o) -> null,
                        "id1", Role.IDENTIFIER, String.class,
                        "me1", Role.MEASURE, String.class,
                        "at1", Role.ATTRIBUTE, String.class
                )
        );

        bindings.put("ds1", dataset);
        engine.eval("ds2 := ds1[rename id1 as id3]"
                + "            [rename id3 as id1]"
                + "                [rename id1 as id1m role = MEASURE,"
                + "                        me1 as me1a role = ATTRIBUTE,"
                + "                        at1 as at1i role = IDENTIFIER]");

        assertThat(bindings).containsKey("ds2");
        Dataset result = (Dataset) bindings.get("ds2");

        assertThat(result.getDataStructure().getRoles()).contains(
                entry("id1m", Role.MEASURE),
                entry("me1a", Role.ATTRIBUTE),
                entry("at1i", Role.IDENTIFIER)
        );
    }

    private Dataset.Tuple tuple(DataPoint... components) {
        return new Dataset.AbstractTuple() {
            @Override
            protected List<DataPoint> delegate() {
                return Arrays.asList(components);
            }
        };
    }
}
