package no.ssb.vtl.script.operations.join;

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

import com.google.common.annotations.VisibleForTesting;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.FilteringSpecification;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.OrderingSpecification;
import no.ssb.vtl.model.VTLBoolean;
import no.ssb.vtl.model.VTLDate;
import no.ssb.vtl.model.VTLExpression;
import no.ssb.vtl.model.VTLFloat;
import no.ssb.vtl.model.VTLInteger;
import no.ssb.vtl.model.VTLNumber;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.model.VTLString;
import no.ssb.vtl.model.VtlFiltering;
import no.ssb.vtl.model.VtlOrdering;
import no.ssb.vtl.script.operations.AbstractUnaryDatasetOperation;
import no.ssb.vtl.script.operations.VtlStream;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class JoinAssignment extends AbstractUnaryDatasetOperation {

    private final VTLExpression expression;
    private final Component.Role role;
    private final Boolean implicit;
    private final String identifier;
    private final ComponentBindings componentBindings;

    public JoinAssignment(Dataset dataset, VTLExpression expression, String identifier, Component.Role role,
                          Boolean implicit) {
        super(checkNotNull(dataset));
        this.expression = checkNotNull(expression);
        this.role = checkNotNull(role);
        this.implicit = checkNotNull(implicit);
        this.identifier = checkNotNull(identifier);
        this.componentBindings = null;
    }

    public JoinAssignment(Dataset dataset, VTLExpression expression, String identifier, Component.Role role,
                          Boolean implicit, ComponentBindings componentBindings) {
        super(checkNotNull(dataset));
        this.expression = checkNotNull(expression);
        this.role = checkNotNull(role);
        this.implicit = checkNotNull(implicit);
        this.identifier = checkNotNull(identifier);
        this.componentBindings = ComponentBindings.copyOf(checkNotNull(componentBindings));
    }

    @VisibleForTesting
    static Class<?> convertToComponentType(Class<? extends VTLObject> vtlType) {
        if (vtlType == VTLString.class)
            return String.class;
        if (vtlType == VTLInteger.class)
            return Long.class;
        if (vtlType == VTLFloat.class)
            return Double.class;
        if (vtlType == VTLNumber.class)
            return Number.class;
        if (vtlType == VTLDate.class)
            return Instant.class;
        if (vtlType == VTLBoolean.class)
            return Boolean.class;
        throw new IllegalArgumentException("not a VTL type " + vtlType);
    }

    @Override
    protected DataStructure computeDataStructure() {
        Class<?> type = convertToComponentType(expression.getVTLType());

        DataStructure.Builder builder = DataStructure.builder();
        DataStructure dataStructure = getChild().getDataStructure();

        // Replace in place or add at the end.
        if (dataStructure.containsKey(identifier)) {

            Component existingComponent = dataStructure.get(identifier);

            // Overriding identifier is not permitted.
            checkArgument(existingComponent.getRole() != Component.Role.IDENTIFIER,
                    "an identifier %s already exists in %s", identifier, getChild()
            );

            // Implicit fails if a component with the same name but a different role
            // already exists.
            if (implicit) {
                checkArgument(role.equals(existingComponent.getRole()),
                        "the role of the component %s must be %s",
                        identifier, existingComponent.getRole()
                );
            }

            for (Map.Entry<String, Component> entry : dataStructure.entrySet()) {
                if (entry.getKey().equals(identifier)) {
                    builder.put(identifier, role, type);
                } else {
                    builder.put(entry.getKey(), entry.getValue());
                }
            }
        } else {
            builder.putAll(dataStructure);
            builder.put(identifier, role, type);
        }
        return builder.build();
    }

    @Override
    public Stream<DataPoint> computeData(Ordering ordering, Filtering filtering, Set<String> components) {
        DataStructure childDataStructure = getChild().getDataStructure();

        DataStructure dataStructure = getDataStructure();
        Component component = dataStructure.get(identifier);
        DataPointBindings dataPointBindings = new DataPointBindings(
                componentBindings,
                childDataStructure
        );

        VtlFiltering childFiltering = (VtlFiltering) computeRequiredFiltering(filtering);
        VtlOrdering childOrdering = (VtlOrdering) computeRequiredOrdering(ordering);

        final Stream<DataPoint> original = getChild().computeData(childOrdering, childFiltering, components);
        Stream<DataPoint> stream = original.peek(datapoint -> {

            if (childDataStructure.size() < dataStructure.size())
                datapoint.add(VTLObject.NULL);

            dataPointBindings.setDataPoint(datapoint);
            VTLObject resolved = expression.resolve(dataPointBindings);

            dataStructure.asMap(datapoint).put(component, resolved);
        });

        return new VtlStream(this, stream, original, ordering, filtering, childOrdering, childFiltering);
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return getChild().getDistinctValuesCount();
    }

    @Override
    public Optional<Long> getSize() {
        return getChild().getSize();
    }

    @Override
    public FilteringSpecification computeRequiredFiltering(FilteringSpecification filtering) {
        // TODO: transform the filter on identity assignments.
        return VtlFiltering.using(getChild()).transpose(filtering);
    }

    @Override
    public OrderingSpecification computeRequiredOrdering(OrderingSpecification ordering) {
        // Remove the assigned column.
        // TODO: Rename the ordering with identity assignments.
        VtlOrdering.Builder builder = VtlOrdering.using(getChild());
        for (String column : ordering.columns()) {
            if (!identifier.equals(column)) {
                builder.then(ordering.getDirection(column), column);
            }
        }
        return builder.build();
    }
}
