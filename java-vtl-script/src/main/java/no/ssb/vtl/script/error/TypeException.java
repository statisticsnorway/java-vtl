package no.ssb.vtl.script.error;

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

import static no.ssb.vtl.script.error.VTLErrorCodeUtil.checkVTLCode;

/**
 * Thrown when the VTL received an expression with type error.
 */
public class TypeException extends CompilationException {
    private static final long serialVersionUID = 9220460392615795969L;
    private final String VTLCode;

    public TypeException(String s, String vtlCode) {
        super(s);
        this.VTLCode = checkVTLCode(vtlCode, "VTL-02");
    }

    @Override
    public String getVTLCode() {
        return VTLCode;
    }
}
