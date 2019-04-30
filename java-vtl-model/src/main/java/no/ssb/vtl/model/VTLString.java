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

public abstract class VTLString extends VTLObject<String> implements VTLTyped<VTLString> {

    @Override
    public Class<VTLString> getVTLType() {
        return VTLString.class;
    }

    private VTLString() {
        // private.
    }

    public static VTLString of(String string) {
        return new VTLString() {
            @Override
            public String get() {
                // TODO: This is hot code. We should rather check in the comparator.
                return "".equals(string) ? null : string;
            }
        };
    }

}
