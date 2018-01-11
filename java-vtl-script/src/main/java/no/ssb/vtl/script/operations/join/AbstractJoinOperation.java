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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import no.ssb.vtl.model.AbstractDatasetOperation;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.script.support.Closer;
import no.ssb.vtl.script.support.JoinSpliterator;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static no.ssb.vtl.model.Order.Direction.ASC;

/**
 * Abstract join operation.
 * <p>
 * Contains the base logic for inner join and outer join operations.
 */
public abstract class AbstractJoinOperation extends AbstractDatasetOperation implements WorkingDataset {

    private static final String ERROR_EMPTY_DATASET_LIST = "join operation impossible on empty dataset list";
    private static final String ERROR_INCOMPATIBLE_TYPES = "incompatible identifier types: %s";
    private static final String ERROR_NO_COMMON_IDENTIFIERS = "could not find common identifiers in the datasets %s";

    private final Table<Component, Dataset, Component> componentMapping;
    private final ImmutableMap<String, Dataset> datasets;
    private final ImmutableSet<Component> commonIdentifiers;

    private final ComponentBindings joinScope;

    AbstractJoinOperation(Map<String, Dataset> namedDatasets, Set<Component> identifiers) {
        super(Lists.newArrayList(checkNotNull(namedDatasets).values()));

        checkArgument(
                !namedDatasets.isEmpty(),
                ERROR_EMPTY_DATASET_LIST
        );
        this.datasets = ImmutableMap.copyOf(checkNotNull(namedDatasets));

        checkNotNull(identifiers);

        this.joinScope = createJoinScope(namedDatasets);

        this.componentMapping = createComponentMapping(this.datasets.values());

        Map<Component, Map<Dataset, Component>> idMap = this.componentMapping.rowMap().entrySet().stream()
                .filter(e -> e.getKey().isIdentifier())
                .filter(e -> e.getValue().size() == datasets.size())
                // identifiers can be from any dataset
                .filter(e -> identifiers.isEmpty() || !Collections.disjoint(e.getValue().values(), identifiers))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // No common identifier
        checkArgument(namedDatasets.size() == 1 || !idMap.isEmpty(), ERROR_NO_COMMON_IDENTIFIERS, namedDatasets);

        this.commonIdentifiers = ImmutableSet.copyOf(idMap.keySet());

        // Type mismatch check
        List<String> typeMismatches = Lists.newArrayList();
        for (Map.Entry<Component, Map<Dataset, Component>> entry : idMap.entrySet()) {
            Component identifier = entry.getKey();

            Multimap<Class<?>, Dataset> typeMap = ArrayListMultimap.create();
            entry.getValue().entrySet().forEach(datasetComponent -> {
                typeMap.put(
                        datasetComponent.getValue().getType(),
                        datasetComponent.getKey()
                );
            });
            if (typeMap.keySet().size() != 1)
                typeMismatches.add(String.format("%s -> (%s)", identifier, typeMap));
        }
        checkArgument(
                typeMismatches.isEmpty(),
                ERROR_INCOMPATIBLE_TYPES,
                String.join(", ", typeMismatches)
        );
    }

    /**
     * Creates a Bindings that contains the unique components of this join operation and the
     * datasets.
     */
    @VisibleForTesting
    static ComponentBindings createJoinScope(Map<String, Dataset> namedDatasets) {
        return new ComponentBindings(namedDatasets);
    }

    /**
     * Create a table that maps the components of the resulting dataset to the component of the underlying
     * datasets.
     * <p>
     * <pre>
     * +------+-----------------+
     * |  re  |     Dataset     |
     * |  su  +-----+-----+-----+
     * |  lt  | ds1 | dsN | ... |
     * +------+-----+-----+-----+
     * | ref  | ref | ref |     |
     * +------+-----+-----+     +
     * | ref  | ref | ref |     |
     * +------+-----+-----+     +
     * | ...  |                 |
     * +------+-----+-----+-----+
     * </pre>
     *
     * @param datasets the datasets
     * @return the component table
     */
    @VisibleForTesting
    static Table<Component, Dataset, Component> createComponentMapping(Iterable<Dataset> datasets) {

        Map<String, Component> seen = Maps.newHashMap();

        ImmutableTable.Builder<Component, Dataset, Component> table;
        table = ImmutableTable.builder();

        for (Dataset dataset : datasets) {
            DataStructure structure = dataset.getDataStructure();
            for (Map.Entry<String, Component> entry : structure.entrySet()) {
                Component component;

                if (entry.getValue().isIdentifier())
                    component = seen.computeIfAbsent(entry.getKey(), s -> entry.getValue());
                else
                    component = entry.getValue();

                table.put(component, dataset, entry.getValue());
            }
        }

        return table.build();
    }

    private static Function<DataPoint, Map<Component, VTLObject>> createKeyExtractor(final DataStructure structure) {
        return dataPoint -> dataPoint != null ? structure.asMap(dataPoint) : null;
    }

    protected abstract BiFunction<DataPoint, DataPoint, DataPoint> getMerger(
            Dataset leftDataset, Dataset rightDataset
    );

    /**
     * Ensure sorted.
     */
    private Stream<DataPoint> sortIfNeeded(Dataset dataset, Order order) {
        // Adjust the order to the structure.

        Order.Builder adjustedOrder = Order.create(dataset.getDataStructure());
        Table<Component, Dataset, Component> mapping = getComponentMapping();

        for (Map.Entry<Component, Order.Direction> orderEntry : order.entrySet()) {
            Map<Dataset, Component> rowMapping = mapping.row(orderEntry.getKey());
            if (!rowMapping.containsKey(dataset))
                continue;

            Component component = rowMapping.get(dataset);
            if (component.isIdentifier()) {
                Order.Direction direction = orderEntry.getValue();
                adjustedOrder.put(component, direction);
            }
        }
        return dataset.getData(adjustedOrder.build()).orElseGet(() -> dataset.getData().sorted(adjustedOrder.build()));
    }

    private Comparator<Map<Component, VTLObject>> createKeyComparator(
            Dataset rightDataset,
            Order order
    ) {

        // Only check the values of the common identifiers.

        HashSet<Component> commonComponents = Sets.newHashSet(getCommonIdentifiers());
        final Map<Component, Order.Direction> commonOrder = Maps.filterKeys(order, commonComponents::contains);
        final Table<Component, Dataset, Component> componentMap = getComponentMapping();
        return (left, right) -> {

            if (left == null)
                return -1;
            if (right == null)
                return 1;
            if (left == right)
                return 0;

            int result;
            for (Map.Entry<Component, Order.Direction> entry : commonOrder.entrySet()) {
                Component component = entry.getKey();
                Order.Direction direction = entry.getValue();

                Map<Dataset, Component> map = componentMap.row(component);

                Component leftComponent = component; // kept for clarity
                Component rightComponent = map.get(rightDataset);

                VTLObject leftValue = left.get(leftComponent);
                VTLObject rightValue = right.get(rightComponent);

                result = Order.NULLS_FIRST.compare(leftValue, rightValue);

                if (result != 0)
                    return direction == ASC ? result : -1 * result;

            }
            return 0;
        };
    }

    @Override
    public Optional<Stream<DataPoint>> getData(Order requestedOrder, Filtering filtering, Set<String> components) {

        // Optimization.
        if (datasets.size() == 1) {
            Dataset dataset = datasets.values().iterator().next();
            return Optional.of(sortIfNeeded(dataset, requestedOrder));
        }

        // Check if requested order is compatible.
        Optional<Order> order = createCompatibleOrder(getDataStructure(), getCommonIdentifiers(), requestedOrder);
        if (!order.isPresent())
            return Optional.empty();

        // TODO: Filtering

        // TODO: Components

        Iterator<Dataset> iterator = datasets.values().iterator();

        Dataset left = iterator.next();
        Dataset right = left;

        // Create the resulting data points.
        final DataStructure joinStructure = getDataStructure();
        final DataStructure structure = left.getDataStructure();

        // Close all children
        Closer closer = Closer.create();

        Stream<DataPoint> result = closer.register(
                sortIfNeeded(left, requestedOrder)
                        .map(dataPoint -> joinStructure.fromMap(
                                structure.asMap(dataPoint)
                        ))
        );

        while (iterator.hasNext()) {
            left = right;
            right = iterator.next();
            result = StreamSupport.stream(
                    new JoinSpliterator<>(
                            createKeyComparator(right, requestedOrder),
                            result.spliterator(),
                            closer.register(
                                    sortIfNeeded(right, requestedOrder)
                            ).spliterator(),
                            createKeyExtractor(joinStructure),
                            createKeyExtractor(right.getDataStructure()),
                            getMerger(left, right)
                    ), false
            );
        }
        return Optional.of(result.onClose(() -> {
            try {
                closer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    protected ImmutableSet<Component> getCommonIdentifiers() {
        return this.commonIdentifiers;
    }

    @Override
    public Stream<DataPoint> getData() {
        Order order = createDefaultOrder(getDataStructure(), getCommonIdentifiers());
        return getData(order)
                .orElseThrow(() -> new RuntimeException("could not sort data"));
    }

    @VisibleForTesting
    Order createDefaultOrder(DataStructure structure, Set<Component> firstComponent) {
        LinkedHashSet<Component> components = Sets.newLinkedHashSet(
                structure.values()
        );
        components.removeAll(firstComponent);

        Order.Builder order = Order.create(structure);
        for (Component component : firstComponent) {
            order.put(component, ASC);
        }
        for (Component component : components) {
            order.put(component, ASC);
        }
        return order.build();
    }

    /**
     * Try to create an order that is compatible with the join using the requested order
     * <p>
     * Join operations need the common identifiers to be first.
     *
     * @param structure
     * @param firstComponents
     * @param requestedOrder  the requested order
     */
    @VisibleForTesting
    Optional<Order> createCompatibleOrder(DataStructure structure, ImmutableSet<Component> firstComponents, Order requestedOrder) {

        Set<Component> identifiers = Sets.newHashSet(firstComponents);

        Order.Builder compatibleOrder = Order.create(structure);
        for (Map.Entry<Component, Order.Direction> order : requestedOrder.entrySet()) {
            Component key = order.getKey();
            Order.Direction direction = order.getValue();

            if (!identifiers.isEmpty() && !identifiers.remove(key))
                return Optional.empty();

            compatibleOrder.put(key, direction);
        }
        return Optional.of(compatibleOrder.build());
    }

    /**
     * Checks if component name is unique among other datasets
     */
    @VisibleForTesting
    boolean componentNameIsUnique(String datasetName, String componentName) {
        for (String otherDatasetName : datasets.keySet()) {
            if (!datasetName.equals(otherDatasetName)) {
                DataStructure structure = datasets.get(otherDatasetName).getDataStructure();
                if (!Sets.intersection(structure.keySet(), Sets.newHashSet(componentName)).isEmpty()) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    protected DataStructure computeDataStructure() {
        // Optimization.
        if (datasets.size() == 1) {
            return datasets.values().iterator().next().getDataStructure();
        }

        Set<String> ids = Sets.newHashSet();
        DataStructure.Builder newDataStructure = DataStructure.builder();
        for (String datasetName : datasets.keySet()) {
            DataStructure structure = datasets.get(datasetName).getDataStructure();
            for (Map.Entry<String, Component> componentEntry : structure.entrySet()) {
                if (!componentEntry.getValue().isIdentifier()) {
                    if (componentNameIsUnique(datasetName, componentEntry.getKey())) {
                        newDataStructure.put(componentEntry.getKey(), componentEntry.getValue());
                    } else {
                        newDataStructure.put(datasetName.concat("_".concat(componentEntry.getKey())), componentEntry.getValue());
                    }
                } else {
                    if (ids.add(componentEntry.getKey())) {
                        newDataStructure.put(componentEntry);
                    }
                }
            }
        }
        return newDataStructure.build();
    }

    public ComponentBindings getJoinScope() {
        return joinScope;
    }

    public Table<Component, Dataset, Component> getComponentMapping() {
        return componentMapping;
    }

}
