package no.ssb.vtl.script.functions;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Arild Johan Takvam-Borge
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

import no.ssb.vtl.model.VTLNumber;
import no.ssb.vtl.model.VTLObject;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.*;

public class VTLRoundTest extends AbstractVTLNumberBinaryFunctionTest {

    @Before
    public void setUp() {
        vtlBinaryFunction = VTLRound.getInstance();
    }

    @Test
    @Override
    public void testInvokeWithPositiveNumber() {
        VTLObject<?> result = vtlBinaryFunction.invoke(
                Lists.newArrayList(
                        VTLNumber.of(0.5377),
                        VTLNumber.of(2)
                )
        );

        assertThat(result).isNotNull();
        assertThat(result).isEqualTo(VTLNumber.of(0.54));

        result = vtlBinaryFunction.invoke(
                Lists.newArrayList(
                        VTLNumber.of(1.52222),
                        VTLNumber.of(4)
                )
        );

        assertThat(result).isNotNull();
        assertThat(result).isEqualTo(VTLNumber.of(1.5222));
    }

    @Test
    @Override
    public void testInvokeWithNegativeNumber() {
        VTLObject<?> result = vtlBinaryFunction.invoke(
                Lists.newArrayList(
                        VTLNumber.of(-0.1234),
                        VTLNumber.of(2)
                )
        );

        assertThat(result).isNotNull();
        assertThat(result).isEqualTo(VTLNumber.of(-0.12));

        result = vtlBinaryFunction.invoke(
                Lists.newArrayList(
                        VTLNumber.of(-9.3456789),
                        VTLNumber.of(4)
                )
        );

        assertThat(result).isNotNull();
        assertThat(result).isEqualTo(VTLNumber.of(-9.3457));
    }


    @Test
    @Override
    public void testInvokeWithNullAsSecondParameter() {
        assertThatThrownBy(() -> vtlBinaryFunction.invoke(
                Lists.newArrayList(
                        VTLNumber.of(4),
                        VTLNumber.of((Double) null)
                )
        ))
                .as("exception when passing null where not null is expected")
                .hasMessage("Argument{name=decimals, type=VTLNumber} must be greater than zero, was [NULL]")
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testInvokeWithNegativeDecimalNumber() {
        assertThatThrownBy(() -> vtlBinaryFunction.invoke(
                Lists.newArrayList(
                        VTLNumber.of(3.444445),
                        VTLNumber.of(-5)
                )
        ))
                .as("exception when passing a negative number where a positive value is expected")
                .hasMessage("Argument{name=decimals, type=VTLNumber} must be greater than zero, was -5")
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testInvokeWithZeroDecimals() {
        VTLObject<?> result = vtlBinaryFunction.invoke(
                Lists.newArrayList(
                        VTLNumber.of(10.1234),
                        VTLNumber.of(0)
                )
        );

        assertThat(result).isNotNull();
        assertThat(result).isEqualTo(VTLNumber.of(10));
    }
}
