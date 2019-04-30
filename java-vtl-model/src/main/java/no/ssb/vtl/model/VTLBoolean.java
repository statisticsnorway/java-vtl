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

public abstract class VTLBoolean extends VTLObject<Boolean> implements VTLTyped<VTLBoolean> {

    private VTLBoolean() {
        // private.
    }

    private static final VTLBoolean TRUE = new VTLBoolean() {
        @Override
        public Boolean get() {
            return true;
        }
    };

    private static final VTLBoolean FALSE = new VTLBoolean() {
        @Override
        public Boolean get() {
            return false;
        }
    };

    @Override
    public Class<VTLBoolean> getVTLType() {
        return VTLBoolean.class;
    }

    public static VTLBoolean of(Boolean value) {
        return value == null ? new VTLBoolean() {
            @Override
            public Boolean get() {
                return null;
            }
        } : value ? TRUE : FALSE;
    }

}
