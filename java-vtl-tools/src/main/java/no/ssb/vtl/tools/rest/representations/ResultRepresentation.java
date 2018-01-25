package no.ssb.vtl.tools.rest.representations;

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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Json representation of the result of an execution.
 */
public class ResultRepresentation {

    @JsonProperty
    private List<DatasetRepresentation> datasets = Lists.newArrayList();
    
    @JsonProperty
    private DatasetRepresentation resultDataset;

    public List<DatasetRepresentation> getDatasets() {
        return datasets;
    }

    public void setDatasets(List<DatasetRepresentation> datasets) {
        this.datasets = datasets;
    }

    public DatasetRepresentation getResultDataset() {
        return resultDataset;
    }

    public void setResultDataset(DatasetRepresentation resultDataset) {
        this.resultDataset = resultDataset;
    }
}
