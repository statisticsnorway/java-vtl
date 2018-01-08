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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.*;
import static no.ssb.vtl.model.Component.*;

/**
 * Fold clause.
 */
public class FoldOperation extends AbstractUnaryDatasetOperation {

    private final String dimension;
    private final String measure;
    private final Set<Component> elements;

    public FoldOperation(Dataset dataset, String dimensionReference, String measureReference, Set<Component> elements) {
        // TODO: Introduce type here. Elements should be of the type of the Component.

        super(checkNotNull(dataset, "dataset cannot be null"));
        checkArgument(!(this.dimension = checkNotNull(dimensionReference, "dimensionReference cannot be null")).isEmpty(),
                "dimensionReference was empty");
        checkArgument(!(this.measure = checkNotNull(measureReference, "measureReference cannot be null")).isEmpty(),
                "measureReference was empty");
        checkArgument(!(this.elements = checkNotNull(elements, "elements cannot be null")).isEmpty(),
                "elements was empty");
    }

    @Override
    protected Optional<Stream<DataPoint>> computeData(Order orders, Filtering filtering, Set<String> components) {
        throw new UnsupportedOperationException("NOT IMPLEMENTED");
    }

    @Override
    public DataStructure computeDataStructure() {
        DataStructure structure = getChild().getDataStructure();

        /*
        Check that all the elements are contained inside the structure.
            TODO: This is a constraint error.
        */
        checkArgument(
                structure.values().containsAll(elements),
                "the element(s) [%s] were not found in [%s]",
                Sets.difference(elements, structure.keySet()), structure.keySet()
        );

        /*
         Checks that elements are of the same type using a Multimap.
         */
        ListMultimap<Class<?>, Component> classes = ArrayListMultimap.create();
        for (Component element : elements) {
            classes.put(element.getType(), element);
        }
        checkArgument(
                classes.asMap().size() == 1,
                "the element(s) [%s] must be of the same type, found [%s] in dataset [%s]",
                elements, classes, structure
        );

        DataStructure.Builder structureBuilder = DataStructure.builder();
        for (Map.Entry<String, Component> componentEntry : structure.entrySet()) {
            if (!elements.contains(componentEntry.getValue())) {
                structureBuilder.put(componentEntry);
            }
        }

        structureBuilder.put(dimension, Role.IDENTIFIER, String.class);
        structureBuilder.put(measure, Role.MEASURE, classes.keySet().iterator().next());

        return structureBuilder.build();
    }

    @Override
    public Stream<DataPoint> getData() {

        final DataStructure dataStructure = getDataStructure();
        final DataStructure childStructure = getChild().getDataStructure();
        final Component dimensionComponent = dataStructure.get(dimension);
        final Component measureComponent = dataStructure.get(measure);

        final List<Component> identifiers = dataStructure.entrySet()
                .stream()
                .map(Map.Entry::getValue)
                .filter(Component::isIdentifier)
                .filter(Predicate.isEqual(dimensionComponent).negate())
                .collect(Collectors.toList());

        /* Fold the values using the component in elements. */
        return getChild().getData().flatMap(dataPoint -> {

            List<DataPoint> newDataPoints = Lists.newArrayListWithExpectedSize(elements.size());

            // Got through the values that will be folded.
            Map<Component, VTLObject> dataPointMap = childStructure.asMap(dataPoint);
            for (Component component : elements) {

                VTLObject value = dataPointMap.get(component);
                if (value == null || value == VTLObject.NULL || value.get() == null)
                    continue;

                String columnName = childStructure.getName(component);

                DataPoint newDataPoint = DataPoint.create(dataStructure.size());
                Map<Component, VTLObject> resultAsMap = dataStructure.asMap(newDataPoint);

                // Put the new values
                resultAsMap.put(dimensionComponent, VTLObject.of(columnName));
                resultAsMap.put(measureComponent, value);

                // Put the identifiers
                for (Component identifier : identifiers) {
                    resultAsMap.put(identifier, dataPointMap.get(identifier));
                }
                newDataPoints.add(newDataPoint);
            }
            return newDataPoints.stream();
        });
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return getChild().getDistinctValuesCount();
    }

    @Override
    public Optional<Long> getSize() {
        return getChild().getSize().map(size -> size * elements.size());
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(elements);
        helper.add("identifier", dimension);
        helper.add("measure", measure);
        //helper.add("structure", getDataStructure());
        return helper.omitNullValues().toString();
    }
}
