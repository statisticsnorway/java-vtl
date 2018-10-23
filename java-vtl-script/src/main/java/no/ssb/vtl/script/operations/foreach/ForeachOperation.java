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
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.script.VTLDataset;

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

import static no.ssb.vtl.model.Order.Direction.ASC;

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
public final class ForeachOperation implements Dataset {

    private final ImmutableMap<String, Dataset> sources;
    private final ImmutableSet<String> identifiers;
    private DataStructure structure;
    private Function<Bindings, VTLDataset> block;

    public ForeachOperation(Map<String, Dataset> sourceDatasets, Set<String> identifiers) {
        this.sources = ImmutableMap.copyOf(sourceDatasets);
        this.identifiers = ImmutableSet.copyOf(identifiers);
    }

    public static Stream<DataPoint> sort(Stream<DataPoint> stream, Order order) {
        System.out.println("WARN: needed to sort");
        Stopwatch started = Stopwatch.createStarted();
        Stream<DataPoint> sorted = stream.sorted(order);
        System.out.println("WARN: done sorting: " + started.stop().elapsed(TimeUnit.SECONDS));
        return sorted;
    }

    public void setBlock(Function<Bindings, VTLDataset> block) {
        this.block = block;
    }

    private Stream<DataPoint> sortIfNeeded(Dataset dataset, Order order) {
        Order actualOrder = rearrangeOrder(order, dataset.getDataStructure());
        return dataset.getData(actualOrder).orElseGet(() -> sort(dataset.getData(), actualOrder));
    }

    @Override
    public Optional<Stream<DataPoint>> getData(Order orders, Filtering filtering, Set<String> components) {

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

        return Optional.of(needSort ? stream.sorted(orders) : stream);
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
    private boolean isCompatible(Order orders) {
        DataStructure structure = getDataStructure();
        HashSet<String> requiredIdentifiers = Sets.newHashSet(this.identifiers);
        for (Component component : orders.keySet()) {
            String name = structure.getName(component);
            if (!requiredIdentifiers.remove(name)) {
                break;
            }
        }
        return requiredIdentifiers.isEmpty();
    }

    /**
     * Create order with identifiers first.
     */
    private Order rearrangeOrder(Order order, DataStructure structure) {
        Order.Builder orderBuilder = Order.create(structure);
        for (String identifier : this.identifiers) {
            orderBuilder.put(identifier, order.getOrDefault(identifier, ASC));
        }

        DataStructure originalStructure = getDataStructure();
        for (Component component : order.keySet()) {
            String name = originalStructure.getName(component);
            if (structure.containsKey(name) && !this.identifiers.contains(name)) {
                orderBuilder.put(name, order.get(component));
            }
        }
        return orderBuilder.build();
    }

    /**
     * Create a "slice" of data and execute the block on it.
     * <p>
     * In order to do so, the maximum value of each input is calculated
     * and each input stream (iterator for simplicity here) first discard
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
            Order orders) {
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

    private Comparator<DataPointMap.View> createComparator(Order orders) {
        return new DataPointMapComparator(
                Maps.filterKeys(getDataStructure(), identifiers::contains),
                orders
        );
    }

    private Order getDefaultOrder() {
        Order.Builder order = Order.create(getDataStructure());
        for (String identifier : identifiers) {
            order.put(identifier, ASC);
        }
        return order.build();
    }

    @Override
    public Stream<DataPoint> getData() {
        return getData(getDefaultOrder()).orElseThrow(() -> new RuntimeException("failed"));
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
    public DataStructure getDataStructure() {
        if (structure == null) {
            Bindings scope = new SimpleBindings(new LinkedHashMap<>());
            for (String name : sources.keySet()) {
                scope.put(name, VTLDataset.of(sources.get(name)));
            }
            structure = block.apply(scope).get().getDataStructure();
        }
        return structure;
    }
}
