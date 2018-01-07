package no.ssb.vtl.script.operations;

/*
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
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

import com.google.common.collect.ImmutableList;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class for DatasetOperations.
 */
public abstract class AbstractDatasetOperation implements Dataset {

    private final ImmutableList<Dataset> children;
    private DataStructure cache;

    protected AbstractDatasetOperation(List<Dataset> children) {
        this.children = ImmutableList.copyOf(checkNotNull(children));
    }

    @Override
    public final DataStructure getDataStructure() {
        return cache = (cache == null ? computeDataStructure() : cache);
    }

    protected abstract DataStructure computeDataStructure();

    public ImmutableList<Dataset> getChildren() {
        return children;
    }

}
