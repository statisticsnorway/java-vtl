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

import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.VTLBoolean;
import no.ssb.vtl.model.VTLExpression;
import no.ssb.vtl.script.operations.join.ComponentBindings;
import no.ssb.vtl.script.operations.join.DataPointBindings;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

public class FilterOperation extends AbstractUnaryDatasetOperation {

    private final VTLExpression predicate;
    private final ComponentBindings componentBindings;

    public FilterOperation(Dataset dataset, VTLExpression predicate, ComponentBindings componentBindings) {
        super(checkNotNull(dataset, "the dataset was null"));
        this.predicate = checkNotNull(predicate);
        this.componentBindings = checkNotNull(componentBindings);
    }

    @Override
    protected Optional<Stream<DataPoint>> computeData(Order orders, Filtering filtering, Set<String> components) {

        Optional<Stream<DataPoint>> data = getChild().getData(orders, filtering, components);
        Stream<DataPoint> sortedStream = data.orElseGet(() -> {
            // TODO: Limit and warn the user.
            return getChild().getData().sorted(orders).filter(filtering);
        });

        DataPointBindings dataPointBindings = new DataPointBindings(componentBindings, getDataStructure());
        return Optional.of(
                sortedStream.map(dataPointBindings::setDataPoint).filter(bindings -> {
                    VTLBoolean resolved = (VTLBoolean) predicate.resolve(dataPointBindings);
                    Boolean predicate = resolved.get();
                    return predicate == null ? false : predicate;
                }).map(DataPointBindings::getDataPoint)
        );

    }

    @Override
    public Stream<DataPoint> getData() {
        return null;
    }

    protected DataStructure computeDataStructure() {
        return getChild().getDataStructure();
    }


    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return Optional.empty();
    }

    @Override
    public Optional<Long> getSize() {
        return Optional.empty();
    }
}
