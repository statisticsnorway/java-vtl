package no.ssb.vtl.script.functions;

/*
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Pawel Buczek
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import no.ssb.vtl.model.VTLNumber;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.model.VTLString;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;


public class VTLSubstrTest implements VTLFunctionTest {

    private VTLSubstr vtlSubstr;

    @Before
    public void setUp() throws Exception {
        vtlSubstr = VTLSubstr.getInstance();
    }

    @Test
    public void testInvokeWithPositiveArgs() throws Exception {
        VTLObject<?> result = vtlSubstr.invoke(
                createArguments("Hello, world!", 2, 5)
        );
        assertThat(result).isNotNull();
        assertThat(result).isEqualTo(VTLString.of("llo, "));

        result = vtlSubstr.invoke(
                createArguments("Hello, world!", 0, 4)
        );
        assertThat(result).isNotNull();
        assertThat(result).isEqualTo(VTLString.of("Hell"));

        result = vtlSubstr.invoke(
                createArguments("", 0, 4)
        );
        assertThat(result).isNotNull();
        //null in string operations is considered an empty string ("")
        assertThat(result).isEqualTo(VTLString.of((String)null));
        assertThat(result).isEqualTo(VTLString.of(""));
        assertThat(result.get()).isNull();

        //deviation from the VTL specification 1.1: return empty string
        //if start position equal or greater than the whole length of the input string
        result = vtlSubstr.invoke(
                createArguments("Hello", 6, 2)
        );
        assertThat(result).isNotNull();
        assertThat(result).isEqualTo(VTLString.of(""));

        //length argument will be ignored
        result = vtlSubstr.invoke(
                createArguments("Hello", 3, 3)
        );
        assertThat(result).isNotNull();
        assertThat(result).isEqualTo(VTLString.of("lo"));
    }

    @Test
    public void testInvokeWithNegativeArgs() throws Exception {
        assertThatThrownBy(() -> vtlSubstr.invoke(
                createArguments("Hello, world!", -1, 4)
        ))
                .as("exception when passing -1 as start position")
                .hasMessage("Argument{name=startPosition, type=VTLInteger} must be greater than zero, was -1")
                .isExactlyInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> vtlSubstr.invoke(
                createArguments("Hello, world!", 1, -1)
        ))
                .as("exception when passing -1 as length")
                .hasMessage("Argument{name=length, type=VTLInteger} must be greater than zero, was -1")
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @Override
    public void testInvokeWithTooManyArguments() throws Exception {
        assertThatThrownBy(() -> vtlSubstr.invoke(
                Lists.newArrayList(
                        VTLString.of("Hello, world!"),
                        VTLNumber.of(1),
                        VTLNumber.of(1),
                        VTLNumber.of(1)
                )
        ))
                .as("exception when passing too many arguments")
                .hasMessage("expected 3 argument(s) but got 4")
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @Override
    public void testInvokeWithEmptyArgumentsList() throws Exception {
        assertThatThrownBy(() -> vtlSubstr.invoke(
                Lists.emptyList()
        ))
                .as("exception when passing no arguments")
                .hasMessage("Required argument not present")
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @Override
    public void testInvokeWithNullValue() throws Exception {
        VTLObject<?> result = vtlSubstr.invoke(
                createArguments(null, 0, 4)
        );

        assertThat(result).isEqualTo(VTLString.of((String) null));

        result = vtlSubstr.invoke(
                Lists.newArrayList(
                        VTLString.of("Hello, world!"),
                        VTLNumber.of(0),
                        VTLNumber.of((Long)null)
                )
        );

        assertThat(result).isEqualTo(VTLString.of("Hello, world!"));

        assertThatThrownBy(() -> vtlSubstr.invoke(
                Lists.newArrayList(
                        VTLString.of("Hello, world!"),
                        VTLNumber.of((Long) null),
                        VTLNumber.of(2)
                )
        ))
                .as("exception when passing -1 as start position")
                .hasMessage("Argument{name=startPosition, type=VTLInteger} must be greater than zero, was [NULL]")
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    private List<VTLObject> createArguments(String ds, Integer startPosition, Integer length) {
        return Lists.newArrayList(
                VTLString.of(ds),
                VTLNumber.of(startPosition),
                VTLNumber.of(length)
        );
    }
}