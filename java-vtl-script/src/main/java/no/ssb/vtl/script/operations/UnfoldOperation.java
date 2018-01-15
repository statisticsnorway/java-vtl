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

import com.codepoetics.protonpack.StreamUtils;
import com.google.common.base.MoreObjects;
import no.ssb.vtl.model.AbstractUnaryDatasetOperation;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Component.Role;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.VTLObject;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.*;

/**
 * Unfold clause.
 */
public class UnfoldOperation extends AbstractUnaryDatasetOperation {

    private final Component dimension;
    private final Component measure;
    private final Set<String> elements;

    public UnfoldOperation(Dataset dataset, Component dimensionReference, Component measureReference, Set<String> elements) {
        super(checkNotNull(dataset, "dataset cannot be null"));

        this.dimension = checkNotNull(dimensionReference, "dimensionReference cannot be null");
        this.measure = checkNotNull(measureReference, "measureReference cannot be null");
        checkArgument(!(this.elements = checkNotNull(elements, "elements cannot be null")).isEmpty(),
                "elements was empty");
        // TODO: Introduce type here. Elements should be of the type of the Component.
    }

    @Override
    public Stream<DataPoint> getData() {
        // TODO: Add an ANY sort option?
        // TODO: Filter on elements so that we minimize data transfer.

        DataStructure dataStructure = getDataStructure();
        DataStructure childStructure = getChild().getDataStructure();

        Order defaultOrder = Order.createDefault(childStructure);

        // Order by identifier, but dimension and measure must be the last ones.
        Order.Builder orderBuilder = Order.create(childStructure)
                .putAll(defaultOrder.entrySet().stream()
                        .filter(e -> e.getKey().isIdentifier())
                        .filter(e -> !e.getKey().equals(dimension))
                        .collect(Collectors.toList())
                );

        Order commonIdentifierOrder = orderBuilder.build();
        Order requiredOrder = orderBuilder.put(dimension, Order.Direction.ASC)
                .put(measure, Order.Direction.ASC).build();

        // Try to get data sorted as required. If impossible, sort it.
        Stream<? extends DataPoint> stream = getChild()
                .getData(requiredOrder)
                .orElse(getChild().getData().sorted(requiredOrder));


        return StreamUtils.aggregate(stream, (left, right) -> {
            // Checks if the previous ids (except the one with unfold on) where different.
            return commonIdentifierOrder.compare(left, right) == 0;
        }).map(dataPoints -> {

            DataPoint result = DataPoint.create(dataStructure.size());
            Map<Component, VTLObject> resultAsMap = dataStructure.asMap(result);

            for (DataPoint dataPoint : dataPoints) {
                Map<Component, VTLObject> dataPointAsMap = childStructure.asMap(dataPoint);

                VTLObject unfoldedValue = null;
                Component unfoldedComponent = null;

                for (Component component : dataPointAsMap.keySet()) {
                    if (component.equals(dimension)) {
                        VTLObject value = dataPointAsMap.get(component);
                        if (elements.contains(value.get())) {
                            unfoldedComponent = dataStructure.get(value.get());
                        }
                    } else if (component.equals(measure)) {
                        unfoldedValue = dataPointAsMap.get(component);
                    } else if (component.isIdentifier()) {
                            resultAsMap.put(component, dataPointAsMap.get(component));
                    }
                }
                resultAsMap.put(unfoldedComponent, unfoldedValue);
            }
            return result;
        });
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return null;
    }

    @Override
    public Optional<Long> getSize() {
        return null;
    }

    @Override
    public Optional<Stream<DataPoint>> getData(Order orders, Filtering filtering, Set<String> components) {
        return Optional.of(getData().sorted(orders).filter(filtering).map(o -> {
            // TODO
            return o;
        }));
    }

    @Override
    public DataStructure computeDataStructure() {
        Dataset dataset = getChild();
        DataStructure dataStructure = dataset.getDataStructure();

        // TODO: Does those check still make sense with the Reference visitor?
        checkArgument(
                dataStructure.containsValue(dimension),
                "the dimension [%s] was not found in %s", dimension, dataset
        );
        checkArgument(
                dataStructure.containsValue(measure),
                "the measure [%s] was not found in %s", measure, dataset
        );

        checkArgument(
                dimension.isIdentifier(),
                "[%s] in dataset [%s] was not a dimension", dimension, dataset
        );
        checkArgument(
                measure.isMeasure(),
                "[%s] in dataset [%s] was not a measure", measure, dataset
        );

            /*
                The spec is a bit unclear here; it does not say how to handle the measure values
                that are not part of the fold operation. Given the dataset:
                [A:ID, B:ID, C:ME, D:ME, E:AT]

                the result of "unfold B, C to foo, bar" could be either:
                 [A:ID, foo:ME, bar:ME, D:ME] with repeated values or nulls
                or
                 [A:ID, foo:ME, bar:ME]

                 until further clarification, the later is implemented.
             */

        DataStructure.Builder newDataStructure = DataStructure.builder();
        for (Map.Entry<String, Component> componentEntry : dataStructure.entrySet()) {
            Component component = componentEntry.getValue();
            if (component != dimension && component != measure) {
                if (component.isIdentifier()) {
                    newDataStructure.put(componentEntry);
                }
            }
        }

        Class<?> type = measure.getType();
        for (String element : elements) {
            newDataStructure.put(element, Role.MEASURE, type);
        }
        return newDataStructure.build();
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.add("identifier", dimension);
        helper.add("measure", measure);
        helper.addValue(elements);
        helper.add("structure", getDataStructure());
        return helper.omitNullValues().toString();
    }
}
