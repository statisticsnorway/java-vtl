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
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.VTLNumber;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.script.error.TypeException;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AggregationOperation extends AbstractUnaryDatasetOperation {

    private final List<Component> groupBy;
    private final List<Component> aggregationComponents;
    private final Function<List<VTLNumber>, VTLNumber> aggregationFunction;

    public AggregationOperation(Dataset child, List<Component> groupBy, List<Component> aggregationComponents, Function<List<VTLNumber>, VTLNumber> aggregationFunction) {
        super(child);
        this.groupBy = groupBy;
        this.aggregationComponents = aggregationComponents;
        this.aggregationFunction = aggregationFunction;
        
    }

    @Override
    protected Optional<Stream<DataPoint>> computeData(Order orders, Filtering filtering, Set<String> components) {
        return null;
    }

    @Override
    protected DataStructure computeDataStructure() {
        DataStructure.Builder newDataStructure = DataStructure.builder();
        for (Map.Entry<String, Component> entry : getChild().getDataStructure().entrySet()) {
            if (groupBy.contains(entry.getValue())) {
                newDataStructure.put(entry);
            } else if (aggregationComponents.contains(entry.getValue())) {
                if (Number.class.isAssignableFrom(entry.getValue().getType())) {
                    newDataStructure.put(entry);
                } else {
                    // TODO: This should be handled in the visitor (before execution)
                    throw new ParseCancellationException(
                            new TypeException(String.format("Cannot aggregate component %s of type %s. It must be numeric", entry.getKey(), entry.getValue().getType()), "VTL-02xx"));
                }
            }
        }
        return newDataStructure.build();
    }

    @Override
    public Stream<DataPoint> getData() {
        DataStructure childStructure = getChild().getDataStructure();
        DataStructure structure = getDataStructure();
    
        Order.Builder builder = Order.create(childStructure);
        groupBy.forEach(component -> builder.put(component, Order.Direction.ASC));
        Order order = builder.build();
    
        Stream<DataPoint> data = getChild().getData(order).orElseGet(() -> getChild().getData().sorted(order));
        Stream<List<DataPoint>> groupedDataPoints = StreamUtils.aggregate(data,
                (dataPoint1, dataPoint2) -> order.compare(dataPoint1, dataPoint2) == 0)
                .onClose(data::close);
    
        @SuppressWarnings("UnnecessaryLocalVariable")
        Stream<DataPoint> aggregatedDataPoints = groupedDataPoints.map(dataPoints -> {
            DataPoint firstDataPointOfGroup = DataPoint.create(dataPoints.get(0));
            Map<Component, VTLObject> resultAsMap = childStructure.asMap(firstDataPointOfGroup);
    
            for (Component aggregationComponent : aggregationComponents) {
    
                List<VTLNumber> aggregationValues = (List) dataPoints.stream()
                        .map(dataPoint -> childStructure.asMap(dataPoint).get(aggregationComponent))
                        .filter(vtlObject -> !VTLObject.NULL.equals(vtlObject))
                        .map(vtlObject -> VTLObject.of(vtlObject.get()))
                        .collect(Collectors.toList());
    
                resultAsMap.put(aggregationComponent, aggregationFunction.apply(aggregationValues));
            }
            return structure.fromMap(resultAsMap);
        });
        
        return aggregatedDataPoints;
    }

    /**
     * Returns the count of unique values by column.
     */
    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return Optional.empty();
    }

    /**
     * Return the amount of {@link DataPoint} the stream obtained by the
     * method {@link Dataset#getData()} will return.
     */
    @Override
    public Optional<Long> getSize() {
        return Optional.empty();
    }
}
