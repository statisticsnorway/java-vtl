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
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.Ordering.Direction;
import no.ssb.vtl.model.VtlFiltering;
import no.ssb.vtl.model.VtlOrdering;
import no.ssb.vtl.script.operations.AbstractDatasetOperation;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static no.ssb.vtl.model.Ordering.Direction.ASC;

/**
 * Abstract join operation.
 * <p>
 * Contains the base logic for inner join and outer join operations.
 */
public abstract class AbstractJoinOperation extends AbstractDatasetOperation implements WorkingDataset {

    private static final String ERROR_EMPTY_DATASET_LIST = "join operation impossible on empty dataset list";
    private static final String ERROR_INCOMPATIBLE_TYPES = "incompatible identifier types: %s";
    private static final String ERROR_NO_COMMON_IDENTIFIERS = "could not find common identifiers in the datasets %s";
    private static final String ERROR_NOT_IN_COMMON_IDENTIFIERS = "join operation has identifiers %s in the 'on' "
            + "clause that is not a common identifier in the datasets %s";
    protected final ImmutableMap<String, Dataset> datasets;
    private final ImmutableMap<String, Component> commonIdentifiers;
    // Contains name mappings for all datasets
    private final Table<String, String, String> columnMapping;

    private final ComponentBindings joinScope;

    AbstractJoinOperation(Map<String, Dataset> namedDatasets, Map<String, Component> identifiers) {
        super(Lists.newArrayList(checkNotNull(namedDatasets).values()));

        checkArgument(
                !namedDatasets.isEmpty(),
                ERROR_EMPTY_DATASET_LIST
        );
        this.datasets = ImmutableMap.copyOf(checkNotNull(namedDatasets));

        checkNotNull(identifiers);

        this.commonIdentifiers = ImmutableMap.copyOf(new CommonIdentifierBindings(this.datasets)
                .getComponentReferences().entrySet().stream()
                .filter(entry -> identifiers.isEmpty() || identifiers.values().contains(entry.getValue()))
                .collect(Collectors.toSet()));

        // Check for common identifiers
        if(namedDatasets.size() > 1 && commonIdentifiers.isEmpty()) {
            if (identifiers.isEmpty()) {
                throw new IllegalStateException(String.format(ERROR_NO_COMMON_IDENTIFIERS, namedDatasets));
            } else {
                throw new IllegalStateException(String.format(ERROR_NOT_IN_COMMON_IDENTIFIERS, identifiers, namedDatasets));
            }
        }


        // Type mismatch check
        List<String> typeMismatches = Lists.newArrayList();
        for (Map.Entry<String, Component> entry : commonIdentifiers.entrySet()) {
            Multimap<Class<?>, Dataset> typeMap = ArrayListMultimap.create();
            for (Dataset dataset : datasets.values()) {

                typeMap.put(dataset.getDataStructure().get(entry.getKey()).getType(), dataset);

                if (typeMap.keySet().size() != 1)
                    typeMismatches.add(String.format("%s -> (%s)", entry.getKey(), typeMap));
            }
        }
        checkArgument(
                typeMismatches.isEmpty(),
                ERROR_INCOMPATIBLE_TYPES,
                String.join(", ", typeMismatches)
        );
        this.columnMapping = getColumnMapping(namedDatasets, commonIdentifiers.keySet());

        this.joinScope = createJoinScope(namedDatasets, commonIdentifiers);
    }

    /**
     * Creates a Bindings that contains the unique components of this join operation and the
     * datasets.
     */
    @VisibleForTesting
    ComponentBindings createJoinScope(Map<String, Dataset> namedDatasets, ImmutableMap<String, Component> commonIdentifiers) {
        ComponentBindings joinScope = new ComponentBindings(namedDatasets);
        // The common (joined) identifiers should be accessible inside the join scope, e.g:
        // test := [a, b] {
        //   idCopy := id,
        // }
        for (Map.Entry<String, Component> entry : commonIdentifiers.entrySet()) {
            joinScope.put(entry.getKey(), entry.getValue());
        }
        return joinScope;
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
        return getColumnMapping(datasets, new HashSet<>());
    }

    /**
     * Create a mapping between the names of join scoped variable, dataset and dataset variable.
     *
     * From VTL 1.1 specification (1810-1818): If the <strong>on</strong> clause is specified then the join is possibly
     * defined on a subset of the common Identifier Components of the Datasets. If the Datasets have common Identifier
     * Components (i.e. with identical names, data type and values domain) that are not specified in the on clause then
     * it is mandatory to refer to those Identifier Components by specifying both the Dataset name and the measure name.
     *
     * For example, if ds1 and ds2 have some common Identifier Components d1, d2 and d3, the following expression:
     *    [ ds1,ds2 on d1, d2 ]
     * returns a Dataset with the following Identifier Components:
     *    d1, d2, 'ds1.d3', 'ds2.d3'
     *
     * @param datasets dataset map
     * @param onIdentifiers identifiers that are listed after the on clause
     * @return column mappings for all datasets (identifiers, measures and attributes)
     */
    @VisibleForTesting
    static Table<String, String, String> getColumnMapping(Map<String, Dataset> datasets, Set<String> onIdentifiers) {

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
                    .filter((e -> onIdentifiers.isEmpty() || onIdentifiers.contains(e.getKey())))
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

    protected ImmutableMap<String, Component> getCommonIdentifiers() {
        return this.commonIdentifiers;
    }

    /**
     * Try to create an order that is compatible with the join using the requested order
     * <p>
     * Join operations need the common identifiers to be first.
     *
     * @param structure       the structure the returned order will apply to.
     * @param firstComponents the identifiers to use.
     * @param ordering  a compatible order.
     */
    @VisibleForTesting
    Ordering createCompatibleOrder(DataStructure structure, ImmutableMap<String, Component> firstComponents, Ordering ordering) {

        LinkedHashMap<String, Direction> directionMap = new LinkedHashMap<>();

        // Add the required columns, respecting the requested order.
        List<String> requestedColumns = ordering.columns();
        for (String requiredColumn : firstComponents.keySet()) {
            if (requestedColumns.contains(requiredColumn)) {
                directionMap.put(requiredColumn, ordering.getDirection(requiredColumn));
            } else {
                directionMap.put(requiredColumn, ASC);
            }
        }

        // Then add remaining columns from the requested order.
        for (String requiredColumn : requestedColumns) {
            directionMap.putIfAbsent(requiredColumn, ordering.getDirection(requiredColumn));
        }

        return new VtlOrdering(directionMap, structure);
    }

    /**
     * Checks if component name is unique among all datasets
     */
    @VisibleForTesting
    boolean componentNameIsUnique(String datasetName, String componentName) {
        for (Map.Entry<String, Dataset> entry : datasets.entrySet()) {
            if (!datasetName.equals(entry.getKey())) {
                DataStructure structure = entry.getValue().getDataStructure();
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
        for (Map.Entry<String, Dataset> entry : datasets.entrySet()) {
            String datasetName = entry.getKey();
            DataStructure structure = entry.getValue().getDataStructure();
            for (Map.Entry<String, Component> componentEntry : structure.entrySet()) {
                // Map from dataset column to join scope column
                ImmutableBiMap<String, String> columnMap = ImmutableBiMap.copyOf(columnMapping.column(datasetName))
                        .inverse();
                String possibleName = columnMap.getOrDefault(componentEntry.getKey(), componentEntry.getKey());
                if (ids.add(possibleName)) {
                    newDataStructure.put(possibleName, componentEntry.getValue());
                }
            }
        }
        return newDataStructure.build();
    }

    public ComponentBindings getJoinScope() {
        return joinScope;
    }

    /**
     * Compute an Ordering that is compatible with the result of the key extractor.
     *
     * @param ordering the requested order.
     * @return order of the common identifiers only.
     */
    protected Ordering computePredicate(Ordering ordering) {
        // We need to create a fake structure to allow the returned
        // Order to work with the result of the key extractors.

        DataStructure.Builder fakeStructureBuilder = DataStructure.builder();
        for (Map.Entry<String, Component> entry : getCommonIdentifiers().entrySet()) {
            fakeStructureBuilder.put(entry.getKey(), entry.getValue());
        }
        DataStructure fakeStructure = fakeStructureBuilder.build();
        VtlOrdering.Builder builder = VtlOrdering.using(fakeStructure);
        for (String column : ordering.columns()) {
            if (fakeStructure.containsKey(column)) {
                builder.then(ordering.getDirection(column), column);
            }
        }

        return builder.build();
    }

    protected Stream<DataPoint> getOrSortData(Dataset dataset, Ordering order, Filtering filtering, Set<String> components) {
        VtlFiltering vtlFiltering = VtlFiltering.using(dataset).transpose(filtering);
        // TODO: Refactor to use AbstractOperation directly.
        if (dataset instanceof AbstractDatasetOperation) {
            return ((AbstractDatasetOperation) dataset).computeData(new VtlOrdering(order, dataset.getDataStructure()), vtlFiltering, components);
        } else {
            //throw new UnsupportedOperationException("Parent should sort.");
            Optional<Stream<DataPoint>> sortedData = dataset.getData(order, vtlFiltering, components);
            if (sortedData.isPresent()) {
                return sortedData.get();
            } else {
                throw new UnsupportedOperationException("Parent should sort.");
                //return dataset.getData().sorted(order).filter(filtering);
            }
        }
    }

    /**
     * Convert the {@link Ordering} so it uses the given structure.
     */
    protected Ordering adjustOrderForStructure(Ordering orders, DataStructure dataStructure) {
        VtlOrdering.Builder using = VtlOrdering.using(dataStructure);
        Set<String> actualColumns = dataStructure.keySet();
        for (String column : orders.columns()) {
            if (actualColumns.contains(column)) {
                using.then(orders.getDirection(column), column);
            }
        }
        return using.build();
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
