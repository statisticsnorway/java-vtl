package no.ssb.vtl.script.expressions;

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

import no.ssb.vtl.model.VTLObject;

import javax.script.Bindings;

import static com.google.common.base.Preconditions.checkNotNull;

public class MembershipExpression extends VariableExpression {

    private String dataset;

    public MembershipExpression(Class<?> type, String dataset, String right) {
        super(type, right);
        this.dataset = checkNotNull(dataset);
    }

    public String getDatasetIdentifier() {
        return dataset;
    }

    @Override
    public VTLObject resolve(Bindings bindings) {
        return super.resolve((Bindings) bindings.get(dataset));
    }
}
