package no.ssb.vtl.script.operations.rename;

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
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.FilteringSpecification;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.OrderingSpecification;
import no.ssb.vtl.model.VtlFiltering;
import no.ssb.vtl.model.VtlOrdering;
import no.ssb.vtl.script.operations.AbstractUnaryDatasetOperation;
import no.ssb.vtl.script.operations.VtlStream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;


/**
 * Rename operation.
 */
public class RenameOperation extends AbstractUnaryDatasetOperation {

    private final ImmutableBiMap<String, String> nameMapping;
    private final ImmutableMap<String, Component.Role> roleMapping;

    public RenameOperation(Dataset child, Map<String, String> nameMapping) {
        this(child, nameMapping, Collections.emptyMap());
    }

    public RenameOperation(Dataset child, Map<String, String> nameMapping, Map<String, Component.Role> roleMapping) {
        super(child);
        this.nameMapping = ImmutableBiMap.copyOf(nameMapping);
        this.roleMapping = ImmutableMap.copyOf(roleMapping);
    }

    @Override
    protected DataStructure computeDataStructure() {
        DataStructure.Builder structure = DataStructure.builder();
        for (Map.Entry<String, Component> componentEntry : getChild().getDataStructure().entrySet()) {
            Component oldComponent = componentEntry.getValue();
            if (nameMapping.containsKey(componentEntry.getKey())) {
                String newName = nameMapping.get(componentEntry.getKey());
                structure.put(
                        newName,
                        roleMapping.getOrDefault(componentEntry.getKey(), oldComponent.getRole()),
                        oldComponent.getType()
                );
            } else {
                structure.put(componentEntry);
            }
        }
        return structure.build();
    }


    @Override
    public FilteringSpecification computeRequiredFiltering(FilteringSpecification filtering) {
        if (filtering == Filtering.ALL) {
            return VtlFiltering.literal(false, FilteringSpecification.Operator.TRUE, null, null);
        }
        Boolean negated = filtering.isNegated();
        FilteringSpecification.Operator operator = filtering.getOperator();
        if (filtering.getOperator() == FilteringSpecification.Operator.OR || filtering.getOperator() == FilteringSpecification.Operator.AND) {
            List<VtlFiltering> operands = new ArrayList<>();
            for (FilteringSpecification operand : filtering.getOperands()) {
                operands.add((VtlFiltering) computeRequiredFiltering(operand));
            }
            return VtlFiltering.nary(negated, operator, operands);
        } else {
            ImmutableBiMap<String, String> reverseMap = ImmutableBiMap.copyOf(nameMapping).inverse();
            return VtlFiltering.literal(
                    negated,
                    operator,
                    reverseMap.getOrDefault(filtering.getColumn(), filtering.getColumn()),
                    filtering.getValue()
            );
        }
    }

    @Override
    public OrderingSpecification computeRequiredOrdering(OrderingSpecification ordering) {
        VtlOrdering.Builder builder = VtlOrdering.using(getChild());
        for (String column : ordering.columns()) {
            String childColumn = nameMapping.inverse().getOrDefault(column, column);
            Ordering.Direction direction = ordering.getDirection(column);
            builder.then(direction, childColumn);
        }
        return builder.build();
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        for (String from : nameMapping.keySet()) {
            String to = nameMapping.get(from);
            Component.Role role = roleMapping.get(from);
            if (role != null) {
                to = to.concat("(" + role + ")");
            }
            helper.add(from, to);
        }
        return helper.omitNullValues().toString();
    }

    @Override
    public Stream<DataPoint> computeData(Ordering oldOrdering, Filtering oldFiltering, Set<String> oldComponents) {
        VtlFiltering childFiltering = (VtlFiltering) computeRequiredFiltering(oldFiltering);
        VtlOrdering childOrdering = (VtlOrdering) computeRequiredOrdering(oldOrdering);

        Set<String> components = renameComponent(oldComponents);

        // No post filter/order since rename does not change the structure.
        Stream<DataPoint> original = getChild().computeData(childOrdering, childFiltering, components);
        return new VtlStream(this, original,
                original,
                oldOrdering,
                oldFiltering,
                childOrdering,
                childFiltering
        );
    }

    private Set<String> renameComponent(Set<String> oldComponents) {
        ImmutableSet.Builder<String> components = ImmutableSet.builder();
        for (String column : oldComponents) {
            String childColumn = nameMapping.getOrDefault(column, column);
            components.add(childColumn);
        }
        return components.build();
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        Dataset child = getChild();
        return child.getDistinctValuesCount().map(oldMap -> {
            ImmutableMap.Builder<String, Integer> builder = ImmutableMap.builder();
            for (String oldName : oldMap.keySet()) {
                Integer count1 = oldMap.get(oldName);
                String name = nameMapping.getOrDefault(oldName, oldName);
                builder.put(name, count1);
            }
            return builder.build();
        });
    }

    @Override
    public Optional<Long> getSize() {
        return getChild().getSize();
    }
}
