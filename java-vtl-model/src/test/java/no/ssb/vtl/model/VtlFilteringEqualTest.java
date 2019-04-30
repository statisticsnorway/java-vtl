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

import org.junit.Test;

import static no.ssb.vtl.model.FilteringSpecification.Operator.TRUE;
import static no.ssb.vtl.model.VtlFiltering.and;
import static no.ssb.vtl.model.VtlFiltering.eq;
import static no.ssb.vtl.model.VtlFiltering.ge;
import static no.ssb.vtl.model.VtlFiltering.gt;
import static no.ssb.vtl.model.VtlFiltering.le;
import static no.ssb.vtl.model.VtlFiltering.literal;
import static no.ssb.vtl.model.VtlFiltering.lt;
import static no.ssb.vtl.model.VtlFiltering.neq;
import static no.ssb.vtl.model.VtlFiltering.not;
import static no.ssb.vtl.model.VtlFiltering.or;
import static org.assertj.core.api.Assertions.assertThat;

public class VtlFilteringEqualTest {

    @Test
    public void testTrueLiteral() {
        VtlFiltering trueA = literal(false, TRUE, null, null);
        VtlFiltering trueB = literal(false, TRUE, null, null);

        assertThat(trueA).isEqualTo(trueB);
        assertThat(trueB).isEqualTo(trueA);

        VtlFiltering falseA = literal(true, TRUE, null, null);
        VtlFiltering falseB = literal(true, TRUE, null, null);

        assertThat(falseA).isEqualTo(falseB);
        assertThat(falseB).isEqualTo(falseA);

        assertThat(falseA).isNotEqualTo(trueA);
        assertThat(trueB).isNotEqualTo(falseB);
    }

    @Test
    public void testEqualLiteral() {
        VtlFiltering a1 = eq("a", 1);
        VtlFiltering a2 = eq("a", 1);

        VtlFiltering a3 = eq("a", 2);
        VtlFiltering a4 = eq("b", 1);

        assertThat(a1).isEqualTo(a2);
        assertThat(a2).isEqualTo(a1);

        assertThat(a1).isNotEqualTo(a3);
        assertThat(a3).isNotEqualTo(a1);

        assertThat(a1).isNotEqualTo(a4);
        assertThat(a4).isNotEqualTo(a1);
    }

    @Test
    public void testNotEqualLiteral() {
        VtlFiltering a1 = neq("a", 1);
        VtlFiltering a2 = neq("a", 1);

        VtlFiltering a3 = neq("a", 2);
        VtlFiltering a4 = neq("b", 1);

        assertThat(a1).isEqualTo(a2);
        assertThat(a2).isEqualTo(a1);

        assertThat(a1).isNotEqualTo(a3);
        assertThat(a3).isNotEqualTo(a1);

        assertThat(a1).isNotEqualTo(a4);
        assertThat(a4).isNotEqualTo(a1);
    }

    @Test
    public void testNotLiteral() {
        VtlFiltering eq = eq("a", 1);
        VtlFiltering neq = neq("a", 1);
        VtlFiltering noteq = not(eq);
        VtlFiltering notnoteq = not(neq);

        assertThat(eq).isEqualTo(notnoteq);
        assertThat(notnoteq).isEqualTo(eq);

        assertThat(neq).isEqualTo(noteq);
        assertThat(noteq).isEqualTo(neq);

        assertThat(eq).isNotEqualTo(neq);
        assertThat(neq).isNotEqualTo(eq);

        assertThat(eq).isNotEqualTo(noteq);
        assertThat(noteq).isNotEqualTo(eq);

    }

    @Test
    public void testLessThanLiteral() {
        VtlFiltering a1 = lt("a", 1);
        VtlFiltering a2 = lt("a", 1);

        VtlFiltering a3 = lt("a", 2);
        VtlFiltering a4 = lt("b", 1);

        assertThat(a1).isEqualTo(a2);
        assertThat(a2).isEqualTo(a1);

        assertThat(a1).isNotEqualTo(a3);
        assertThat(a3).isNotEqualTo(a1);

        assertThat(a1).isNotEqualTo(a4);
        assertThat(a4).isNotEqualTo(a1);
    }

    @Test
    public void testLessEqualLiteral() {
        VtlFiltering a1 = le("a", 1);
        VtlFiltering a2 = le("a", 1);

        VtlFiltering a3 = le("a", 2);
        VtlFiltering a4 = le("b", 1);

        assertThat(a1).isEqualTo(a2);
        assertThat(a2).isEqualTo(a1);

        assertThat(a1).isNotEqualTo(a3);
        assertThat(a3).isNotEqualTo(a1);

        assertThat(a1).isNotEqualTo(a4);
        assertThat(a4).isNotEqualTo(a1);
    }

    @Test
    public void testGreaterEqualLiteral() {
        VtlFiltering a1 = ge("a", 1);
        VtlFiltering a2 = ge("a", 1);

        VtlFiltering a3 = ge("a", 2);
        VtlFiltering a4 = ge("b", 1);

        assertThat(a1).isEqualTo(a2);
        assertThat(a2).isEqualTo(a1);

        assertThat(a1).isNotEqualTo(a3);
        assertThat(a3).isNotEqualTo(a1);

        assertThat(a1).isNotEqualTo(a4);
        assertThat(a4).isNotEqualTo(a1);
    }

    @Test
    public void testGreaterThanLiteral() {
        VtlFiltering a1 = gt("a", 1);
        VtlFiltering a2 = gt("a", 1);

        VtlFiltering a3 = gt("a", 2);
        VtlFiltering a4 = gt("b", 1);

        assertThat(a1).isEqualTo(a2);
        assertThat(a2).isEqualTo(a1);

        assertThat(a1).isNotEqualTo(a3);
        assertThat(a3).isNotEqualTo(a1);

        assertThat(a1).isNotEqualTo(a4);
        assertThat(a4).isNotEqualTo(a1);
    }

    @Test
    public void testAnd() {
        VtlFiltering abc = and(gt("a", 1), eq("b", 2), neq("c", 3));
        VtlFiltering cba = and(neq("c", 3), eq("b", 2), gt("a", 1));

        assertThat(abc).isEqualTo(cba);
        assertThat(cba).isEqualTo(abc);
    }

    @Test
    public void testOr() {
        VtlFiltering abc = or(gt("a", 1), eq("b", 2), neq("c", 3));
        VtlFiltering cba = or(neq("c", 3), eq("b", 2), gt("a", 1));

        assertThat(abc).isEqualTo(cba);
        assertThat(cba).isEqualTo(abc);
    }

    @Test
    public void testAndWithTrue() {
        VtlFiltering trueLit = literal(false, TRUE, null, null);

        VtlFiltering trueBc = and(trueLit, eq("b", 2), neq("c", 3));
        VtlFiltering aTruec = and(trueLit, eq("b", 2), neq("c", 3));
        VtlFiltering abTrue = and(trueLit, eq("b", 2), neq("c", 3));

        assertThat(trueBc).isNotEqualTo(trueLit);
        assertThat(trueLit).isNotEqualTo(trueBc);
        assertThat(aTruec).isNotEqualTo(trueLit);
        assertThat(trueLit).isNotEqualTo(aTruec);
        assertThat(abTrue).isNotEqualTo(trueLit);
        assertThat(trueLit).isNotEqualTo(abTrue);

        assertThat(trueBc).isEqualTo(trueBc);
        assertThat(trueBc).isEqualTo(aTruec);
        assertThat(trueBc).isEqualTo(abTrue);

        assertThat(aTruec).isEqualTo(trueBc);
        assertThat(aTruec).isEqualTo(aTruec);
        assertThat(aTruec).isEqualTo(abTrue);

        assertThat(aTruec).isEqualTo(abTrue);
        assertThat(aTruec).isEqualTo(abTrue);
        assertThat(aTruec).isEqualTo(abTrue);

    }
}
