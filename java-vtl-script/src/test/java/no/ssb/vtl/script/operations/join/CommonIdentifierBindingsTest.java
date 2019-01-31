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
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLTyped;
import org.junit.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class CommonIdentifierBindingsTest {

    @Test
    public void testJoinBindingsOneDataset() {

        StaticDataset t1 = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("id2", Component.Role.IDENTIFIER, Long.class)
                .addComponent("uni1", Component.Role.IDENTIFIER, Double.class)
                .addComponent("m1", Component.Role.MEASURE, Instant.class)
                .addComponent("a1", Component.Role.MEASURE, Boolean.class)
                .build();

        CommonIdentifierBindings result = new CommonIdentifierBindings(ImmutableMap.of(
                "t1", t1
        ));

        assertThat(result).containsOnlyKeys("id1", "id2", "uni1");
        assertThat(result.getComponentReferences()).containsOnlyKeys("id1", "id2", "uni1");

    }
    @Test
    public void testJoinBindings() {

        StaticDataset t1 = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("id2", Component.Role.IDENTIFIER, Long.class)
                .addComponent("id3", Component.Role.IDENTIFIER, Double.class)
                .addComponent("m1", Component.Role.MEASURE, Instant.class)
                .addComponent("a1", Component.Role.MEASURE, Boolean.class)
                .addComponent("t3", Component.Role.MEASURE, Boolean.class)
                .build();

        StaticDataset t2 = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("id2", Component.Role.IDENTIFIER, Long.class)
                .addComponent("id3", Component.Role.IDENTIFIER, Double.class)
                .addComponent("m1", Component.Role.MEASURE, Instant.class)
                .addComponent("a1", Component.Role.MEASURE, Boolean.class)
                .addComponent("t1", Component.Role.MEASURE, Boolean.class)
                .build();

        StaticDataset t3 = StaticDataset.create()
                .addComponent("id1", Component.Role.IDENTIFIER, String.class)
                .addComponent("id2", Component.Role.IDENTIFIER, Long.class)
                .addComponent("m1", Component.Role.MEASURE, Instant.class)
                .addComponent("t2", Component.Role.MEASURE, Boolean.class)
                .build();

        CommonIdentifierBindings result = new CommonIdentifierBindings(ImmutableMap.of(
                "t1", t1,
                "t2", t2,
                "t3", t3
        ));

        assertThat(result).containsOnlyKeys("id1", "id2");
        assertThat(result.getComponentReferences()).containsOnlyKeys("id1", "id2");

        assertThat(result.get("id1")).isInstanceOf(VTLTyped.class);
        assertThat(result.get("id2")).isInstanceOf(VTLTyped.class);
    }
}
