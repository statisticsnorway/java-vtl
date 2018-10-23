package no.ssb.vtl.script.operations.foreach;

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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.script.VTLDataset;
import no.ssb.vtl.script.VTLScriptEngine;
import no.ssb.vtl.script.operations.hierarchy.HierarchyOperation;
import no.ssb.vtl.script.operations.join.InnerJoinOperation;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Component.Role.ATTRIBUTE;
import static no.ssb.vtl.model.Component.Role.IDENTIFIER;
import static no.ssb.vtl.model.Component.Role.MEASURE;
import static org.assertj.core.api.Assertions.assertThat;

public class ForeachOperationTest {

    @Test
    public void testEval() throws ScriptException {
        ScriptEngine engine = new VTLScriptEngine();
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);

        bindings.put("t1", createInnerJoin1());
        bindings.put("t2", createInnerJoin2());

        engine.eval("" +
                "res := foreach year in t1, t2 do" +
                "  test := [t1, t2] {" +
                "    filter true" +
                "  }" +
                "done" +
                "", bindings);

        assertThat(bindings).containsKeys("res");
        Object res = bindings.get("res");
        assertThat(res).isInstanceOf(Dataset.class);
        Dataset ds = (Dataset) res;
        assertThat(ds.getData()).containsExactlyInAnyOrder(
                createInnerJoinResult()
        );
    }

    @Test
    public void testInnerJoin() {
        Dataset data1 = createInnerJoin1();
        Dataset data2 = createInnerJoin2();

        ForeachOperation foreachOperation = new ForeachOperation(ImmutableMap.of("t1", data1, "t2", data2), ImmutableSet.of("year"));
        foreachOperation.setBlock(bindings -> {
            VTLDataset t1 = (VTLDataset) bindings.get("t1");
            VTLDataset t2 = (VTLDataset) bindings.get("t2");
            ImmutableMap<String, Dataset> namedDataset = ImmutableMap.of(
                    "t1", t1.get(),
                    "t2", t2.get()
            );
            ImmutableSet<Component> identifier = ImmutableSet.of(
                    t1.get().getDataStructure().get("year"),
                    t1.get().getDataStructure().get("id")
            );
            return VTLDataset.of(new InnerJoinOperation(namedDataset, identifier));
        });
        System.out.println(foreachOperation.getDataStructure());

        Order orderedByYear = Order.create(foreachOperation.getDataStructure())
                .put("year", Order.Direction.DESC).build();

        Optional<Stream<DataPoint>> data = foreachOperation.getData(orderedByYear);
        assertThat(data).isNotEmpty();
        Stream<DataPoint> stream = data.get();
        assertThat(stream).containsExactly(
                createInnerJoinResult()
        );

    }

    private DataPoint[] createInnerJoinResult() {
        return new DataPoint[]{DataPoint.create(2004, 1, "m1", "t1-2004", "m1", "t2-2004"),
                DataPoint.create(2004, 2, "m2", "t1-2004", "m2", "t2-2004"),
                DataPoint.create(2004, 3, "m3", "t1-2004", "m3", "t2-2004"),
                DataPoint.create(2003, 1, "m1", "t1-2003", "m1", "t2-2003"),
                DataPoint.create(2003, 2, "m2", "t1-2003", "m2", "t2-2003"),
                DataPoint.create(2003, 3, "m3", "t1-2003", "m3", "t2-2003"),
                DataPoint.create(2000, 1, "m1", "t1-2000", "m1", "t2-2000"),
                DataPoint.create(2000, 2, "m2", "t1-2000", "m2", "t2-2000"),
                DataPoint.create(2000, 3, "m3", "t1-2000", "m3", "t2-2000")};
    }

    private Dataset createInnerJoin2() {
        return StaticDataset.create()
                    .addComponent("year", IDENTIFIER, Long.class)
                    .addComponent("id", IDENTIFIER, Long.class)
                    .addComponent("measure", MEASURE, String.class)
                    .addComponent("attribute", ATTRIBUTE, String.class)

                    .addPoints(2000L, 1L, "m1", "t2-2000")
                    .addPoints(2000L, 2L, "m2", "t2-2000")
                    .addPoints(2000L, 3L, "m3", "t2-2000")
                    .addPoints(2000L, 4L, "m4", "t2-2000")

                    .addPoints(2002L, 1L, "m1", "t2-2002")
                    .addPoints(2002L, 2L, "m2", "t2-2002")
                    .addPoints(2002L, 3L, "m3", "t2-2002")
                    .addPoints(2002L, 4L, "m4", "t2-2002")

                    .addPoints(2003L, 1L, "m1", "t2-2003")
                    .addPoints(2003L, 2L, "m2", "t2-2003")
                    .addPoints(2003L, 3L, "m3", "t2-2003")

                    .addPoints(2004L, 1L, "m1", "t2-2004")
                    .addPoints(2004L, 2L, "m2", "t2-2004")
                    .addPoints(2004L, 3L, "m3", "t2-2004")

                    .build();
    }

    private Dataset createInnerJoin1() {
        return StaticDataset.create()
                    .addComponent("year", IDENTIFIER, Long.class)
                    .addComponent("id", IDENTIFIER, Long.class)
                    .addComponent("measure", MEASURE, String.class)
                    .addComponent("attribute", ATTRIBUTE, String.class)

                    .addPoints(2000L, 1L, "m1", "t1-2000")
                    .addPoints(2000L, 2L, "m2", "t1-2000")
                    .addPoints(2000L, 3L, "m3", "t1-2000")

                    .addPoints(2001L, 1L, "m1", "t1-2001")
                    .addPoints(2001L, 2L, "m2", "t1-2001")
                    .addPoints(2001L, 3L, "m3", "t1-2001")
                    .addPoints(2001L, 4L, "m4", "t1-2001")

                    .addPoints(2003L, 1L, "m1", "t1-2003")
                    .addPoints(2003L, 2L, "m2", "t1-2003")
                    .addPoints(2003L, 3L, "m3", "t1-2003")
                    .addPoints(2003L, 4L, "m4", "t1-2003")

                    .addPoints(2004L, 1L, "m1", "t1-2004")
                    .addPoints(2004L, 2L, "m2", "t1-2004")
                    .addPoints(2004L, 3L, "m3", "t1-2004")

                    .build();
    }

    @Test
    public void testHierarchy() {
        Dataset data1 = StaticDataset.create()
                .addComponent("year", IDENTIFIER, Long.class)
                .addComponent("id", IDENTIFIER, String.class)
                .addComponent("measure", MEASURE, Long.class)
                .addComponent("attribute", ATTRIBUTE, String.class)

                .addPoints(2000L, "m1", 1L , "t1-2000")
                .addPoints(2000L, "m2", 2L , "t1-2000")
                .addPoints(2000L, "m3", 3L , "t1-2000")

                .addPoints(2001L, "m1", 1L , "t1-2001")
                .addPoints(2001L, "m2", 2L , "t1-2001")
                .addPoints(2001L, "m3", 3L , "t1-2001")
                .addPoints(2001L, "m4", 4L , "t1-2001")

                .addPoints(2003L, "m1", 1L , "t1-2003")
                .addPoints(2003L, "m2", 2L , "t1-2003")
                .addPoints(2003L, "m3", 3L , "t1-2003")
                .addPoints(2003L, "m4", 4L , "t1-2003")

                .addPoints(2004L, "m1", 1L , "t1-2004")
                .addPoints(2004L, "m2", 2L , "t1-2004")
                .addPoints(2004L, "m3", 3L , "t1-2004")

                .build();

        StaticDataset hierarchy = StaticDataset.create()
                .addComponent("year", IDENTIFIER, Long.class)
                .addComponent("from", IDENTIFIER, String.class)
                .addComponent("to", IDENTIFIER, String.class)
                .addComponent("sign", IDENTIFIER, String.class)

                .addPoints(2001L, "m1", "total", "+")
                .addPoints(2001L, "m2", "total", "+")
                .addPoints(2001L, "m3", "total", "+")

                .addPoints(2003L, "m1", "total", "+")
                .addPoints(2003L, "m2", "total", "+")

                .addPoints(2004L, "m1", "total", "+")

                .build();

        ForeachOperation foreachOperation = new ForeachOperation(ImmutableMap.of("t1", data1, "hier", hierarchy), ImmutableSet.of("year"));
        foreachOperation.setBlock(bindings -> {
            VTLDataset t1 = (VTLDataset) bindings.get("t1");
            VTLDataset hier = (VTLDataset) bindings.get("hier");
            return VTLDataset.of(new HierarchyOperation(t1.get(), hier.get(), t1.get().getDataStructure().get("id")));
        });

        System.out.println(foreachOperation.getDataStructure());
        foreachOperation.getData().forEach(System.out::println);
        System.out.println(foreachOperation.getDataStructure());

        Order orderedByMeasureAndYearDesc = Order.create(foreachOperation.getDataStructure())
                .put("measure", Order.Direction.DESC)
                .put("year", Order.Direction.DESC)
                .build();

        Optional<Stream<DataPoint>> data = foreachOperation.getData(
                orderedByMeasureAndYearDesc
        );
        assertThat(data).as("result of the foreach operation")
                .isNotEmpty();

        List<DataPoint> points = data.get().collect(Collectors.toList());
        assertThat(points).isSortedAccordingTo(orderedByMeasureAndYearDesc);
        points.forEach(System.out::println);


    }
}