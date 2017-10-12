package no.ssb.vtl.script.functions;

/*
 * -
 *  * ========================LICENSE_START=================================
 * * Java VTL
 *  *
 * %%
 * Copyright (C) 2017 Arild Johan Takvam-Borge
 *  *
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

import com.google.common.annotations.VisibleForTesting;
import no.ssb.vtl.model.VTLNumber;
import no.ssb.vtl.model.VTLObject;

public class VTLSqrt extends AbstractVTLFunction<VTLNumber> {

    private static final Argument<VTLNumber> DS = new Argument<>("ds", VTLNumber.class);

    @VisibleForTesting
    VTLSqrt() {
        super("sqrt", VTLNumber.class, DS);
    }


    @Override
    protected VTLNumber safeInvoke(TypeSafeArguments arguments) {
        VTLNumber ds = arguments.get(DS);

        if (ds.get() == null) {
            return VTLObject.of((Number) null);
        }

        return VTLNumber.of(Math.sqrt(ds.get().doubleValue()));
    }
}