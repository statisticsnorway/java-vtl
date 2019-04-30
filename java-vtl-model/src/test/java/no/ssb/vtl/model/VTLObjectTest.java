package no.ssb.vtl.model;

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
import org.assertj.core.api.AutoCloseableSoftAssertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.Test;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Java6Assertions.assertThat;
import static org.assertj.core.api.Java6Assertions.assertThatThrownBy;

public class VTLObjectTest {

    @Test
    public void testGenericFactory() {

        Map<Object, Class<?>> types = ImmutableMap.<Object, Class<?>>builder()
                .put("string", VTLString.class)
                .put(1.0D, VTLFloat.class)
                .put(1.0F, VTLFloat.class)
                .put(1L, VTLInteger.class)
                .put(1, VTLInteger.class)
                .put(Instant.now(), VTLDate.class)
                .put(true, VTLBoolean.class)
                .build();

        for (Map.Entry<Object, Class<?>> entry : types.entrySet()) {
            VTLObject vtlObject = VTLObject.of(entry.getKey());
            assertThat(vtlObject).isInstanceOf(entry.getValue());
        }

        assertThat(VTLObject.of((Object) null)).isSameAs(VTLObject.NULL);

        assertThat(VTLObject.of(VTLObject.NULL)).isSameAs(VTLObject.NULL);
        assertThat(VTLObject.NULL.get()).isNull();
        assertThat(VTLObject.NULL.toString()).isEqualTo("[NULL]");

        assertThatThrownBy(() -> VTLObject.of(new HashSet<>()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("could not create a VTLObject from");

    }

    @Test
    public void testFloat() {
        VTLFloat aFloat = VTLObject.of(1.0);
        assertThat(aFloat).isInstanceOf(VTLFloat.class);
        assertThat(aFloat.getVTLType()).isEqualTo(VTLFloat.class);

        VTLFloat aFloatFromDouble = VTLObject.of(1.0D);
        assertThat(aFloatFromDouble).isInstanceOf(VTLFloat.class);
        assertThat(aFloatFromDouble.getVTLType()).isEqualTo(VTLFloat.class);
    }

    @Test
    public void testString() {
        VTLString aString = VTLObject.of("");
        assertThat(aString).isInstanceOf(VTLString.class);
        assertThat(aString.getVTLType()).isEqualTo(VTLString.class);
    }

    @Test
    public void testEquals() {
        VTLInteger vtlInteger = VTLObject.of(1L);
        assertThat(vtlInteger.equals(vtlInteger)).isTrue();
        assertThat(vtlInteger.equals(VTLObject.of(1L))).isTrue();
        assertThat(vtlInteger.equals(VTLObject.of(2L))).isFalse();
        assertThat(vtlInteger.equals(null)).isFalse();
        assertThat(vtlInteger.equals(new Object())).isFalse();
    }

    @Test
    public void testCompare() {
        SoftAssertions softly = new SoftAssertions();
        Instant now = Instant.now();
        Set<Object> values = ImmutableSet.builder().add("string",1.0D, 1.0F, 1L, 1, now, false).build();
        Map<Object, Object> greater = ImmutableMap.builder()
                .put("string", "zstring")
                .put(1.0D, 2.0D)
                .put(1.0F, 2.0F)
                .put(1L, 2L)
                .put(1, 2)
                .put(now, now.plusSeconds(60))
                .put(false, true)
                .build();
        Map<Object, Object> less = ImmutableMap.builder()
                .put("string", "astring")
                .put(1.0D, 0.5D)
                .put(1.0F, 0.5F)
                .put(1L, 0L)
                .put(1, 0)
                .put(now, now.minusSeconds(60))
                .build();

        for (Object value : values) {
            VTLObject vtlObject = VTLObject.of(value);
            if (greater.containsKey(value)) {
                VTLObject greaterVtlObject = VTLObject.of(greater.get(value));
                softly.assertThat(vtlObject.compareTo(greaterVtlObject)).isLessThan(0);
                softly.assertThat(greaterVtlObject.compareTo(vtlObject)).isGreaterThan(0);
            }
            if (less.containsKey(value)) {
                VTLObject lesserVtlObject = VTLObject.of(less.get(value));
                softly.assertThat(vtlObject.compareTo(lesserVtlObject)).isGreaterThan(0);
                softly.assertThat(lesserVtlObject.compareTo(vtlObject)).isLessThan(0);
            }
        }

        softly.assertAll();
    }

    @Test
    public void testIntegerCompare() throws Exception {

        VTLObject one = VTLObject.of(new Integer(1));
        VTLObject two = VTLObject.of(new Integer(2));
        VTLObject otherOne = VTLObject.of(new Integer(1));

        assertThat(one.compareTo(two)).isLessThan(0);
        assertThat(two.compareTo(one)).isGreaterThan(0);
        assertThat(one.compareTo(otherOne)).isEqualTo(0);
        assertThat(otherOne.compareTo(one)).isEqualTo(0);

    }

    @Test
    public void testDoubleCompare() throws Exception {

        VTLObject one = VTLObject.of(new Double(1));
        VTLObject two = VTLObject.of(new Double(2));
        VTLObject otherOne = VTLObject.of(new Double(1));

        assertThat(one.compareTo(two)).isLessThan(0);
        assertThat(two.compareTo(one)).isGreaterThan(0);
        assertThat(one.compareTo(otherOne)).isEqualTo(0);
        assertThat(otherOne.compareTo(one)).isEqualTo(0);

    }

    @Test
    public void testDifferentNumberCompare() throws Exception {

        VTLObject one = VTLObject.of(new Double(1));
        VTLObject two = VTLObject.of(new Long(2));
        VTLObject otherOne = VTLObject.of(new Long(1));

        try (AutoCloseableSoftAssertions softly = new AutoCloseableSoftAssertions()) {
            softly.assertThat(one.compareTo(two)).isEqualTo(-1);

            softly.assertThat(two.compareTo(one)).isEqualTo(1);

            softly.assertThat(one.compareTo(otherOne)).isEqualTo(0);

            softly.assertThat(otherOne.compareTo(one)).isEqualTo(0);
        }


    }
}
