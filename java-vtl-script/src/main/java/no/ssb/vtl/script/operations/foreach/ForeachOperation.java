package no.ssb.vtl.script.operations.foreach;

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

import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.FilteringSpecification;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.OrderingSpecification;
import no.ssb.vtl.model.VtlOrdering;
import no.ssb.vtl.script.VTLDataset;
import no.ssb.vtl.script.operations.AbstractDatasetOperation;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import static no.ssb.vtl.model.Ordering.Direction.ANY;
import static no.ssb.vtl.model.Ordering.Direction.ASC;

/**
 * Operation that repeats a set of operations
 * <p>
 * This operation repeats a set of operation for every distinct values
 * of a set of identifiers in a set of datasets.
 * <p>
 * for example:
 *
 * <pre>
 *     data := get("...")
 *     hierarchy := get("...")
 *
 *     result := foreach periode, region in (data, hierarchy) {
 *         subresult := hierarchy(data, data.measure, hierarchy, false)
 *     }
 * </pre>
 * <p>
 * The set of identifiers much be a subset of the common identifiers of the
 * set of datasets.
 */
public final class ForeachOperation extends AbstractDatasetOperation {

    private final ImmutableMap<String, Dataset> sources;
    private final ImmutableSet<String> identifiers;
    private DataStructure structure;
    private Function<Bindings, VTLDataset> block;

    public ForeachOperation(Map<String, Dataset> sourceDatasets, Set<String> identifiers) {
        super(sourceDatasets.values());
        this.sources = ImmutableMap.copyOf(sourceDatasets);
        this.identifiers = ImmutableSet.copyOf(identifiers);
    }

    public static Stream<DataPoint> sort(Stream<DataPoint> stream, Comparator<DataPoint> order) {
        System.out.println("WARN: needed to sort");
        Stopwatch started = Stopwatch.createStarted();
        Stream<DataPoint> sorted = stream.sorted(order);
        System.out.println("WARN: done sorting: " + started.stop().elapsed(TimeUnit.SECONDS));
        return sorted;
    }

    public void setBlock(Function<Bindings, VTLDataset> block) {
        this.block = block;
    }

    private Stream<DataPoint> sortIfNeeded(Dataset dataset, Ordering order) {
        Ordering actualOrder = rearrangeOrder(order, dataset.getDataStructure());
        return dataset.getData(actualOrder).orElseGet(() -> sort(dataset.getData(), actualOrder));
    }


    @Override
    public Stream<DataPoint> computeData(Ordering orders, Filtering filtering, Set<String> components) {
        Boolean needSort = !isCompatible(orders);

        ImmutableMap.Builder<String, PeekingIterator<DataPointMap.View>> iteratorBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, DataStructure> structureBuilder = ImmutableMap.builder();
        for (String name : sources.keySet()) {
            Dataset dataset = sources.get(name);
            DataPointMap mapView = new DataPointMap(dataset.getDataStructure());
            PeekingIterator<DataPointMap.View> iterator = Iterators.peekingIterator(
                    sortIfNeeded(dataset, orders).map(mapView::wrap).iterator()
            );
            iteratorBuilder.put(name, iterator);
            structureBuilder.put(name, dataset.getDataStructure());
        }
        ImmutableMap<String, PeekingIterator<DataPointMap.View>> sources = iteratorBuilder.build();
        ImmutableMap<String, DataStructure> structures = structureBuilder.build();

        Stream<DataPoint> stream = Streams.stream(new AbstractIterator<DataPoint>() {

            Iterator<DataPoint> current = null;

            @Override
            protected DataPoint computeNext() {
                if (current == null || !current.hasNext()) {
                    current = slice(structures, sources, orders);
                }
                if (!current.hasNext()) {
                    // Continue as long as source has values.
                    for (PeekingIterator<DataPointMap.View> it : sources.values()) {
                        if (it.hasNext()) {
                            current = slice(structures, sources, orders);
                            if (current.hasNext()) {
                                return current.next();
                            }
                        }
                    }
                    return endOfData();
                } else {
                    return current.next();
                }
            }
        });

        return needSort ? stream.sorted(orders) : stream;
    }

    /**
     * Checks that the order if compatible.
     * <p>
     * To be able to slice the data, the data needs to be ordered first by
     * the identifiers we slice along.
     * <p>
     * The direction and order of the identifiers
     * is not important.
     *
     * @param orders the order to check
     * @return true if compatible.
     */
    private boolean isCompatible(Ordering orders) {
        HashSet<String> requiredIdentifiers = Sets.newHashSet(this.identifiers);
        for (String name : orders.columns()) {
            if (!requiredIdentifiers.remove(name)) {
                break;
            }
        }
        return requiredIdentifiers.isEmpty();
    }

    /**
     * Create order with identifiers first.
     */
    private Ordering rearrangeOrder(Ordering order, DataStructure structure) {
        ImmutableMap.Builder<String, Ordering.Direction> orderBuilder = ImmutableMap.builder();
        for (String identifier : this.identifiers) {
            Ordering.Direction direction = order.getDirection(identifier);
            if (direction == null || direction.equals(ANY)) {
                direction = ASC;
            }
            orderBuilder.put(identifier, direction);
        }

        for (String column : order.columns()) {
            if (structure.containsKey(column) && !this.identifiers.contains(column)) {
                Ordering.Direction direction = order.getDirection(column);
                if (direction == null || direction.equals(ANY)) {
                    direction = ASC;
                }
                orderBuilder.put(column, direction);
            }
        }
        return new VtlOrdering(orderBuilder.build(), structure);
    }

    /**
     * Create a "slice" of data and execute the block on it.
     * <p>
     * In order to do so, the maximum value of each input is calculated
     * and each input stream (iterator for simplicity here) first discards
     * any values <b>less than max</b> then produces values <b>as long as
     * it is equal to max</b>.
     *
     * @param structures the structure of the datasets.
     * @param iterators  the iterators for the data of the datasets.
     * @param orders
     * @return the result of the block as an iterator.
     */
    private Iterator<DataPoint> slice(
            Map<String, DataStructure> structures,
            Map<String, PeekingIterator<DataPointMap.View>> iterators,
            Ordering orders) {
        Bindings scope = new SimpleBindings(new LinkedHashMap<>());


        Comparator<DataPointMap.View> comparator = createComparator(orders);

        MaxSelector<DataPointMap.View> maxSupplier = new MaxSelector<>(iterators.values(), comparator);
        Optional<DataPointMap.View> max = maxSupplier.get();
        if (!max.isPresent()) {
            return Collections.emptyIterator();
        }

        for (String name : iterators.keySet()) {
            PeekingIterator<DataPointMap.View> iterator = iterators.get(name);
            DataPointMap.View finalMax = max.get();
            Iterator<DataPointMap.View> slice = new AbstractIterator<DataPointMap.View>() {
                @Override
                protected DataPointMap.View computeNext() {
                    while (iterator.hasNext() && comparator.compare(iterator.peek(), finalMax) < 0) {
                        iterator.next();
                    }
                    if (iterator.hasNext() && comparator.compare(iterator.peek(), finalMax) == 0) {
                        return iterator.next();
                    } else {
                        return endOfData();
                    }
                }
            };
            DataStructure structure = structures.get(name);
            IteratorDataset dataset = new IteratorDataset(
                    structure,
                    Iterators.transform(slice, DataPointMap.View::unwrap)
            );
            scope.put(name, VTLDataset.of(dataset));
        }
        Dataset dataset = block.apply(scope).get();
        return dataset.getData(orders).orElseGet(() -> sort(dataset.getData(), orders)).iterator();
    }

    private Comparator<DataPointMap.View> createComparator(Ordering orders) {
        ImmutableMap.Builder<String, Ordering.Direction> identifierOrder = ImmutableMap.builder();
        for (String column : orders.columns()) {
            if (identifiers.contains(column)) {
                identifierOrder.put(column, orders.getDirection(column));
            }
        }
        return new DataPointMapComparator(identifierOrder.build());
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
    public DataStructure computeDataStructure() {
        if (structure == null) {
            Bindings scope = new SimpleBindings(new LinkedHashMap<>());
            for (String name : sources.keySet()) {
                scope.put(name, VTLDataset.of(sources.get(name)));
            }
            structure = block.apply(scope).get().getDataStructure();
        }
        return structure;
    }

    @Override
    public FilteringSpecification computeRequiredFiltering(FilteringSpecification filtering) {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public OrderingSpecification computeRequiredOrdering(OrderingSpecification filtering) {
        throw new UnsupportedOperationException("TODO");
    }
}
