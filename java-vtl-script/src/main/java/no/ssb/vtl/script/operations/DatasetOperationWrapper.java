package no.ssb.vtl.script.operations;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2018 Hadrien Kohl
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
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.FilteringSpecification;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.OrderingSpecification;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Convert a {@link Dataset} to an {@link AbstractDatasetOperation}
 */
public class DatasetOperationWrapper extends AbstractDatasetOperation {

    private final Dataset dataset;

    public DatasetOperationWrapper(Dataset dataset) {
        super(Collections.emptyList());
        this.dataset = checkNotNull(dataset);
    }

    @Override
    public String toString() {
        if (dataset instanceof AbstractDatasetOperation) {
            return dataset.toString();
        } else {
            return MoreObjects.toStringHelper(this)
                    .add("dataset", dataset)
                    .toString();
        }
    }

    /**
     * Sorts and filters the stream if the underlying dataset does not support it.
     */
    private Stream<DataPoint> ensureSortedFilteredStream(Ordering orders, Filtering filtering, Set<String> components) {
        Optional<Stream<DataPoint>> sorted = dataset.getData(orders, filtering, components);
        if (sorted.isPresent()) {
            return new VtlStream(
                    this, sorted.get(), Collections.emptyList(), orders, filtering, orders, Filtering.ALL);
        } else {
            return new VtlStream(
                    this,
                    dataset.getData(), Collections.emptyList(), orders, filtering, Ordering.ANY, Filtering.ALL);
        }
    }

    @Override
    public Stream<DataPoint> computeData(Ordering orders, Filtering filtering, Set<String> components) {
        if (dataset instanceof AbstractDatasetOperation) {
            return ((AbstractDatasetOperation) dataset).computeData(orders, filtering, components);
        } else {
            return ensureSortedFilteredStream(orders, filtering, components);
        }
    }

    @Override
    protected DataStructure computeDataStructure() {
        return dataset.getDataStructure();
    }

    @Override
    public FilteringSpecification computeRequiredFiltering(FilteringSpecification filtering) {
        return filtering;
    }

    @Override
    public OrderingSpecification computeRequiredOrdering(OrderingSpecification ordering) {
        return ordering;
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return dataset.getDistinctValuesCount();
    }

    @Override
    public Optional<Long> getSize() {
        return dataset.getSize();
    }
}
