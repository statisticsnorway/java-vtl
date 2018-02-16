package no.ssb.vtl.model;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2018 Pawel Buczek
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

public class VTLNumberTest {

    @Test(expected = NullPointerException.class)
    public void divideNumberByNull() {
        VTLNumber dividend = VTLNumber.of(1);
        dividend.divide((Number)null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void divideNumberByZero() {
        VTLNumber dividend = VTLNumber.of(1);
        dividend.divide(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void divideZeroByZero() {
        VTLNumber dividend = VTLNumber.of(0);
        dividend.divide(0);
    }

}
