package no.ssb.vtl.model;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2018 Hadrien Kohl
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

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static no.ssb.vtl.model.VtlFiltering.eq;
import static no.ssb.vtl.model.VtlFiltering.ge;
import static no.ssb.vtl.model.VtlFiltering.gt;
import static no.ssb.vtl.model.VtlFiltering.le;
import static no.ssb.vtl.model.VtlFiltering.lt;
import static no.ssb.vtl.model.VtlFiltering.neq;
import static org.assertj.core.api.Assertions.assertThat;

public class VtlFilteringTest {

    private List<DataPoint> data;
    private DataStructure structure;

    @Before
    public void setUp() {
        structure = DataStructure.of(
                "1", Component.Role.IDENTIFIER, String.class,
                "2", Component.Role.IDENTIFIER, String.class
        );

        ArrayList<String> dims = Lists.newArrayList("a", "b", "c", "d", "e");
        data = Lists.cartesianProduct(dims, dims).stream().map(strings -> {
            return DataPoint.create(strings.toArray(new Object[0]));
        }).collect(Collectors.toList());
    }

    @Test
    public void testEqual() {

        VtlFiltering filtering = VtlFiltering.using(structure).or(
                eq("1", "a"),
                eq("2", "a")
        ).build();

        List<DataPoint> result = data.stream().filter(filtering).collect(Collectors.toList());
        assertThat(result).containsExactly(
                DataPoint.create("a", "a"),
                DataPoint.create("a", "b"),
                DataPoint.create("a", "c"),
                DataPoint.create("a", "d"),
                DataPoint.create("a", "e"),
                DataPoint.create("b", "a"),
                DataPoint.create("c", "a"),
                DataPoint.create("d", "a"),
                DataPoint.create("e", "a")
        );

        assertThat(filtering.toString()).isEqualTo("(1=a|2=a)");
    }

    @Test
    public void testNotEqual() {

        // (1 = a) and (not 2 = a)
        VtlFiltering filtering = VtlFiltering.using(structure).and(
                eq("1", "a"),
                neq("2", "a")
        ).build();

        List<DataPoint> result = data.stream().filter(filtering).collect(Collectors.toList());

        assertThat(result).containsExactly(
                DataPoint.create("a", "b"),
                DataPoint.create("a", "c"),
                DataPoint.create("a", "d"),
                DataPoint.create("a", "e")
        );

        assertThat(filtering.toString()).isEqualTo("(1=a&2!=a)");
    }

    @Test
    public void testGreaterThan() {

        VtlFiltering filtering = VtlFiltering.using(structure).and(
                eq("1", "a"),
                gt("2", "c")
        ).build();

        List<DataPoint> result = data.stream().filter(filtering).collect(Collectors.toList());
        assertThat(result).containsExactly(
                DataPoint.create("a", "d"),
                DataPoint.create("a", "e")
        );

        assertThat(filtering.toString()).isEqualTo("(1=a&2>c)");
    }

    @Test
    public void testGreaterOrEqual() {

        VtlFiltering filtering = VtlFiltering.using(structure).and(
                eq("1", "a"),
                ge("2", "c")
        ).build();

        List<DataPoint> result = data.stream().filter(filtering).collect(Collectors.toList());
        assertThat(result).containsExactly(
                DataPoint.create("a", "c"),
                DataPoint.create("a", "d"),
                DataPoint.create("a", "e")
        );

        assertThat(filtering.toString()).isEqualTo("(1=a&2>=c)");
    }


    @Test
    public void testLessThan() {

        // (1 = a) and (not 2 = a)
        VtlFiltering filtering = VtlFiltering.using(structure).and(
                eq("1", "a"),
                lt("2", "c")
        ).build();

        List<DataPoint> result = data.stream().filter(filtering).collect(Collectors.toList());
        assertThat(result).containsExactly(
                DataPoint.create("a", "a"),
                DataPoint.create("a", "b")
        );

        assertThat(filtering.toString()).isEqualTo("(1=a&2<c)");
    }

    @Test
    public void testLessOrEqual() {

        // (1 = a) and (not 2 = a)
        VtlFiltering filtering = VtlFiltering.using(structure).and(
                eq("1", "a"),
                le("2", "c")
        ).build();

        List<DataPoint> result = data.stream().filter(filtering).collect(Collectors.toList());
        assertThat(result).containsExactly(
                DataPoint.create("a", "a"),
                DataPoint.create("a", "b"),
                DataPoint.create("a", "c")
        );

        assertThat(filtering.toString()).isEqualTo("(1=a&2<=c)");
    }
}
