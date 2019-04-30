package no.ssb.vtl.script.operations.join;

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
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLBoolean;
import no.ssb.vtl.model.VTLDate;
import no.ssb.vtl.model.VTLFloat;
import no.ssb.vtl.model.VTLInteger;
import no.ssb.vtl.model.VTLNumber;
import no.ssb.vtl.model.VTLString;
import no.ssb.vtl.model.VTLTyped;
import org.junit.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class ComponentBindingsTest {

    @Test
    public void testDatasetBindings() {

        Dataset dataset = StaticDataset.create()
                .addComponent("c1", Component.Role.IDENTIFIER, String.class)
                .addComponent("c2", Component.Role.IDENTIFIER, Long.class)
                .addComponent("c3", Component.Role.IDENTIFIER, Double.class)
                .addComponent("c4", Component.Role.IDENTIFIER, Instant.class)
                .addComponent("c5", Component.Role.IDENTIFIER, Boolean.class)
                .addComponent("c6", Component.Role.IDENTIFIER, Number.class)
                .build();

        ComponentBindings bindings = new ComponentBindings(dataset);

        assertThat(((VTLTyped<?>) bindings.get("c1")).getVTLType()).isEqualTo(VTLString.class);
        assertThat(((VTLTyped<?>) bindings.get("c2")).getVTLType()).isEqualTo(VTLInteger.class);
        assertThat(((VTLTyped<?>) bindings.get("c3")).getVTLType()).isEqualTo(VTLFloat.class);
        assertThat(((VTLTyped<?>) bindings.get("c4")).getVTLType()).isEqualTo(VTLDate.class);
        assertThat(((VTLTyped<?>) bindings.get("c5")).getVTLType()).isEqualTo(VTLBoolean.class);
        assertThat(((VTLTyped<?>) bindings.get("c6")).getVTLType()).isEqualTo(VTLNumber.class);

    }

    @Test
    public void testJoinBindingsOneDataset() {

        StaticDataset t1 = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("id2", Component.Role.IDENTIFIER, Long.class)
                .addComponent("uni1", Component.Role.IDENTIFIER, Double.class)
                .addComponent("m1", Component.Role.MEASURE, Instant.class)
                .addComponent("a1", Component.Role.MEASURE, Boolean.class)
                .build();

        ComponentBindings result = new ComponentBindings(ImmutableMap.of(
                "t1", t1
        ));

        assertThat(result).containsOnlyKeys("id1", "id2", "uni1", "m1", "a1", "t1");

    }
    @Test
    public void testJoinBindings() {

        StaticDataset t1 = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("id2", Component.Role.IDENTIFIER, Long.class)
                .addComponent("uni1", Component.Role.IDENTIFIER, Double.class)
                .addComponent("m1", Component.Role.MEASURE, Instant.class)
                .addComponent("a1", Component.Role.MEASURE, Boolean.class)
                .addComponent("t3", Component.Role.MEASURE, Boolean.class)
                .build();

        StaticDataset t2 = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("id2", Component.Role.IDENTIFIER, Long.class)
                .addComponent("uni2", Component.Role.IDENTIFIER, Double.class)
                .addComponent("uni5", Component.Role.MEASURE, Instant.class)
                .addComponent("a1", Component.Role.MEASURE, Boolean.class)
                .addComponent("t1", Component.Role.MEASURE, Boolean.class)
                .build();

        StaticDataset t3 = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("id2", Component.Role.IDENTIFIER, Long.class)
                .addComponent("uni3", Component.Role.IDENTIFIER, Double.class)
                .addComponent("m1", Component.Role.MEASURE, Instant.class)
                .addComponent("uni4", Component.Role.MEASURE, Boolean.class)
                .addComponent("t2", Component.Role.MEASURE, Boolean.class)
                .build();

        ComponentBindings result = new ComponentBindings(ImmutableMap.of(
                "t1", t1,
                "t2", t2,
                "t3", t3
        ));

        assertThat(result).containsOnlyKeys("uni4", "uni5", "uni2", "uni3", "uni1", "t1", "t2", "t3");

        assertThat(result.get("uni1")).isInstanceOf(VTLTyped.class);
        assertThat(result.get("uni2")).isInstanceOf(VTLTyped.class);
        assertThat(result.get("uni3")).isInstanceOf(VTLTyped.class);
        assertThat(result.get("uni4")).isInstanceOf(VTLTyped.class);
        assertThat(result.get("uni5")).isInstanceOf(VTLTyped.class);

        assertThat(result.get("t1")).isInstanceOf(ComponentBindings.class);
        assertThat(result.get("t2")).isInstanceOf(ComponentBindings.class);
        assertThat(result.get("t3")).isInstanceOf(ComponentBindings.class);
    }
}
