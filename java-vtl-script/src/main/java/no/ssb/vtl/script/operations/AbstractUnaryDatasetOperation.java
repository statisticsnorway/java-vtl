package no.ssb.vtl.script.operations;

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

import com.google.common.collect.ImmutableList;
import no.ssb.vtl.model.Dataset;

public abstract class AbstractUnaryDatasetOperation extends AbstractDatasetOperation {

    protected AbstractUnaryDatasetOperation(AbstractDatasetOperation child) {
        super(ImmutableList.of(child));
    }

    public AbstractUnaryDatasetOperation(Dataset child) {
        super(ImmutableList.of(new DatasetOperationWrapper(child)));
    }

    public AbstractDatasetOperation getChild() {
        return getChildren().get(0);
    }
}
