package no.ssb.vtl.script.functions;

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

import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.model.VTLString;

/**
 * The "||" operator of VTL.
 *
 * leftExpr || rightExpr
 *
 */
public class VTLConcatenation extends AbstractVTLFunction {

    private static final Argument<VTLString> LEFT = new Argument<>("left", VTLString.class);
    private static final Argument<VTLString> RIGHT = new Argument<>("right", VTLString.class);

    protected VTLConcatenation() {
        super("||", LEFT, RIGHT);
    }

    @Override
    VTLObject safeInvoke(TypeSafeArguments arguments) {

        VTLString left = arguments.get(LEFT);
        VTLString right = arguments.get(LEFT);

        // TODO: add isNull() in VTLObject.
        if (left.get() == null || right.get() == null) {
            return VTLObject.NULL;
        } else {
            return VTLObject.of(left.get().concat(right.get()));
        }
    }
}