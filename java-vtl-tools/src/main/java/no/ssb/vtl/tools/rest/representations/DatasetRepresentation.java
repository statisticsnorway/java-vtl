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
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.VTLObject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Json representation of a {@link no.ssb.vtl.model.Dataset}.
 */
public class DatasetRepresentation {

    @JsonProperty
    private String name;

    @JsonProperty
    private List<ComponentRepresentation> structure = Lists.newArrayList();

    @JsonProperty
    private List<List<Object>> data = Lists.newArrayList();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<ComponentRepresentation> getStructure() {
        return structure;
    }

    public void setStructure(List<ComponentRepresentation> structure) {
        this.structure = structure;
    }

    public List<List<Object>> getData() {
        return data;
    }

    public void setData(List<List<Object>> data) {
        this.data = data;
    }

    public static DatasetRepresentation create(String name, Dataset dataset) {
        DatasetRepresentation wrapper = new DatasetRepresentation();
        wrapper.setName(name);
        List<ComponentRepresentation> structure = wrapper.getStructure();
        for (Map.Entry<String, Component> component : dataset.getDataStructure().entrySet()) {
            ComponentRepresentation componentRepresentation = new ComponentRepresentation();
            componentRepresentation.setName(component.getKey());
            componentRepresentation.setRole(component.getValue().getRole());
            componentRepresentation.setType(component.getValue().getType());
            structure.add(componentRepresentation);
        }
        List<List<Object>> data = dataset.getData()
                .map(tuple -> tuple.stream()
                        .map(VTLObject::get).collect(Collectors.toList())
                ).collect(Collectors.toList());
        wrapper.setData(data);
        return wrapper;
    }

    public static Dataset convertToDataset(DatasetRepresentation dataset) {
        DataStructure.Builder builder = DataStructure.builder();
        for (ComponentRepresentation component : dataset.getStructure()) {
            builder.put(component.getName(), component.getRole(), component.getType());
        }

        final DataStructure structure = builder.build();
        return new Dataset() {

            @Override
            public DataStructure getDataStructure() {
                return structure;
            }

            @Override
            public Stream<DataPoint> getData() {
                return dataset.getData().stream()
                        .map(objects -> DataPoint.create(objects.stream().map(VTLObject::of)
                        .collect(Collectors.toList())
                ));
            }

            @Override
            public Optional<Map<String, Integer>> getDistinctValuesCount() {
                return Optional.empty();
            }

            @Override
            public Optional<Long> getSize() {
                return Optional.empty();
            }

            @Override
            public Optional<Stream<DataPoint>> getData(Order orders, Filtering filtering, Set<String> components) {
                return Optional.of(getData().sorted(orders).filter(filtering).map(o -> {
                    // TODO
                    return o;
                }));
            }
        };
    }
}
