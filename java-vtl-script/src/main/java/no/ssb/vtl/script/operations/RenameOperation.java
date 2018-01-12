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

/*-
 * #%L
 * java-vtl-script
 * %%
 * Copyright (C) 2016 Hadrien Kohl
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
 * #L%
 */

import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import no.ssb.vtl.model.AbstractUnaryDatasetOperation;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Rename operation.
 * <p>
 * TODO: Implement {@link Dataset}
 */
public class RenameOperation extends AbstractUnaryDatasetOperation {

    private final Map<Component, String> newNames;
    private final Map<Component, Component.Role> newRoles;

    public RenameOperation(Dataset dataset, Map<Component, String> newNames) {
        this(dataset, newNames, Collections.emptyMap());
    }

    public RenameOperation(Dataset dataset, Map<Component, String> newNames, Map<Component, Component.Role> newRoles) {
        super(checkNotNull(dataset));
        this.newNames = newNames;
        this.newRoles = newRoles;
    }


    @Override
    protected DataStructure computeDataStructure() {
        Map<Component, String> map = Maps.newHashMap();
        DataStructure.Builder newDataStructure = DataStructure.builder();
        for (Map.Entry<String, Component> componentEntry : getChild().getDataStructure().entrySet()) {
            Component component = componentEntry.getValue();
            if (newNames.containsKey(component)) {
                String newName = newNames.get(component);
                map.put(component, newName);
                newDataStructure.put(
                        newName,
                        newRoles.getOrDefault(component, component.getRole()),
                        component.getType()
                );
            } else {
                newDataStructure.put(componentEntry);
            }
        }

        return newDataStructure.build();
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(newNames);
        helper.addValue(newRoles);
        helper.add("structure", getDataStructure());
        return helper.omitNullValues().toString();
    }

    @Override
    public Stream<DataPoint> getData() {
        return getChild().getData();
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        // TODO: Adjust the names.
        return getChild().getDistinctValuesCount();
    }

    @Override
    public Optional<Long> getSize() {
        return getChild().getSize();
    }
}
