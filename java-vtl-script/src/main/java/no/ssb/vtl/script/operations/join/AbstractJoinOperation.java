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
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import no.ssb.vtl.model.AbstractDatasetOperation;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    protected final ImmutableMap<String, Dataset> datasets;
    private final Table<Component, Dataset, Component> componentMapping;
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
     *
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

    @VisibleForTesting
    static Table<String, String, String> getColumnMapping(Map<String, Dataset> datasets) {

        ImmutableTable.Builder<String, String, String> table;
        table = ImmutableTable.builder();

        // Counting column occurrences.
        Multiset<String> sharedColumns = HashMultiset.create();
        for (Dataset dataset : datasets.values()) {
            DataStructure structure = dataset.getDataStructure();
            sharedColumns.addAll(structure.keySet());
        }

        Multiset<String> identifierColumns = HashMultiset.create();
        for (Dataset dataset : datasets.values()) {
            DataStructure structure = dataset.getDataStructure();
            Set<String> identifiers = structure.entrySet().stream()
                    .filter(e -> e.getValue().isIdentifier())
                    .map(e -> e.getKey())
                    .collect(Collectors.toSet());
            identifierColumns.addAll(identifiers);
        }

        for (String datasetName : datasets.keySet()) {
            Dataset dataset = datasets.get(datasetName);
            DataStructure structure = dataset.getDataStructure();
            for (Map.Entry<String, Component> entry : structure.entrySet()) {
                String column = entry.getKey();
                if (identifierColumns.count(column) != datasets.size() && sharedColumns.count(column) > 1) {
                    table.put(datasetName + "_" + column, datasetName, column);
                } else {
                    table.put(column, datasetName, column);
                }
            }
        }

        return table.build();
    }

    protected ImmutableSet<Component> getCommonIdentifiers() {
        return this.commonIdentifiers;
    }

    @Override
    public final Stream<DataPoint> getData() {
        // Use the order that is best; using the identifiers we are joining on only.
        Order.Builder orderBuilder = Order.create(getDataStructure());
        for (Component identifier : getCommonIdentifiers()) {
            orderBuilder.put(identifier, ASC);
        }
        return getData(orderBuilder.build(), Filtering.ALL, getDataStructure().keySet())
                .orElseThrow(() -> new RuntimeException("could not sort data"));
    }

    @Override
    public abstract Optional<Stream<DataPoint>> getData(Order orders, Filtering filtering, Set<String> components);

    /**
     * Try to create an order that is compatible with the join using the requested order
     * <p>
     * Join operations need the common identifiers to be first.
     *
     * @param structure       the structure the returned order will apply to.
     * @param firstComponents the identifiers to use.
     * @param requestedOrder  a compatible order.
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
     * Checks if component name is unique among all datasets
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

    /**
     * Compute the predicate.
     *
     * @param requestedOrder the requested order.
     * @return order of the common identifiers only.
     */
    protected Order computePredicate(Order requestedOrder) {
        DataStructure structure = getDataStructure();

        // We need to create a fake structure to allow the returned
        // Order to work with the result of the key extractors.

        ImmutableSet<Component> commonIdentifiers = getCommonIdentifiers();
        DataStructure.Builder fakeStructure = DataStructure.builder();
        for (Component component : commonIdentifiers) {
            fakeStructure.put(structure.getName(component), component);
        }

        Order.Builder predicateBuilder = Order.create(fakeStructure.build());
        for (Component component : commonIdentifiers) {
            predicateBuilder.put(component, requestedOrder.getOrDefault(component, ASC));
        }
        return predicateBuilder.build();
    }

    protected Stream<DataPoint> getOrSortData(Dataset dataset, Order order, Filtering filtering, Set<String> components) {
        Optional<Stream<DataPoint>> sortedData = dataset.getData(order, filtering, components);
        if (sortedData.isPresent()) {
            return sortedData.get();
        } else {
            return dataset.getData().sorted(order).filter(filtering);
        }
    }

    /**
     * Convert the {@link Order} so it uses the given structure.
     */
    protected Order adjustOrderForStructure(Order orders, DataStructure dataStructure) {

        DataStructure structure = getDataStructure();
        Order.Builder adjustedOrders = Order.create(dataStructure);

        // Uses names since child structure can be different.
        for (Component component : orders.keySet()) {
            String name = structure.getName(component);
            if (dataStructure.containsKey(name))
                adjustedOrders.put(name, orders.get(component));
        }
        return adjustedOrders.build();
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        if (getChildren().size() == 1) {
            return getChildren().get(0).getDistinctValuesCount();
        } else {
            // TODO
            return Optional.empty();
        }
    }

    @Override
    public Optional<Long> getSize() {
        if (getChildren().size() == 1) {
            return getChildren().get(0).getSize();
        } else {
            // TODO
            return Optional.empty();
        }
    }
}
