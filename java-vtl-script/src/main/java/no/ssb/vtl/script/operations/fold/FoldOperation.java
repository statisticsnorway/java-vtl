package no.ssb.vtl.script.operations.fold;

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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.FilteringSpecification;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.OrderingSpecification;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.model.VtlFiltering;
import no.ssb.vtl.model.VtlOrdering;
import no.ssb.vtl.script.operations.AbstractUnaryDatasetOperation;
import no.ssb.vtl.script.operations.VtlStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static no.ssb.vtl.model.Component.Role;

/**
 * Fold clause.
 */
public class FoldOperation extends AbstractUnaryDatasetOperation {

    private final String dimension;
    private final String measure;
    private final ImmutableSet<String> elements;

    private int[] copyIndices;
    private int[] elementIndices;
    private int measureIndex;
    private int dimensionIndex;
    private int size;
    private String[] elementNames;

    public FoldOperation(Dataset dataset, String dimension, String measure, Set<String> elements) {
        super(dataset);

        this.elements = ImmutableSet.copyOf(elements);
        checkArgument(!elements.isEmpty());

        this.dimension = checkNotNull(dimension);
        checkArgument(!dimension.isEmpty());

        this.measure = checkNotNull(measure);
        checkArgument(!measure.isEmpty());

    }

    /**
     * Check that all of the given columns are of the same type
     *
     * @return the type of all the columns.
     * @throws IllegalArgumentException if columns are not of the same type.
     */
    private static Class<?> checkColumnType(DataStructure structure, ImmutableSet<String> columns) {
        ListMultimap<Class<?>, String> classes = ArrayListMultimap.create();
        for (String element : columns) {
            classes.put(structure.get(element).getType(), element);
        }
        checkArgument(
                classes.asMap().size() == 1,
                "all element(s) [%s] are not of the same type [%s]",
                columns, classes
        );
        return classes.keys().iterator().next();
    }

    /**
     * Checks that all of the given columns are present in the structure.
     *
     * @throws IllegalArgumentException if one or more columns are missing.
     */
    private static void checkContainsColumns(DataStructure structure, ImmutableSet<String> columns) {
        Set<String> structureColumns = ImmutableSet.copyOf(structure.keySet());
        checkArgument(
                structureColumns.containsAll(columns),
                "the element(s) [%s] were not found",
                Sets.difference(columns, structureColumns)
        );
    }

    /**
     * Checks that none of the given columns have the identifier role.
     *
     * @throws IllegalArgumentException if one or more columns have the identifier role.
     */
    private static void checkNoIdentifiers(DataStructure structure, ImmutableSet<String> columns) {
        ListMultimap<Role, String> roles = ArrayListMultimap.create();
        for (String elements : columns) {
            roles.put(structure.get(elements).getRole(), elements);
        }
        checkArgument(
                !roles.containsKey(Role.IDENTIFIER),
                "cannot fold identifier(s) [%s]",
                roles.get(Role.IDENTIFIER)
        );
    }

    @Override
    public DataStructure computeDataStructure() {

        // In its normal form, all elements should be present in a fold operation.
        // This should be picked up by the parser but we check once anyways.
        DataStructure childStructure = getChild().getDataStructure();

        checkContainsColumns(childStructure, elements);

        // All the elements should be of the same type.
        Class<?> newType = checkColumnType(childStructure, elements);

        // Cannot remove identifiers without grouping.
        checkNoIdentifiers(childStructure, elements);

        // Copy each columns that is not in elements.
        // Putting them first optimizes the fold operation.
        DataStructure.Builder structureBuilder = DataStructure.builder();
        for (String columnName : Sets.difference(childStructure.keySet(), elements)) {
            structureBuilder.put(columnName, childStructure.get(columnName));
        }

        structureBuilder.put(dimension, Role.IDENTIFIER, String.class);
        structureBuilder.put(measure, Role.MEASURE, newType);
        DataStructure structure = structureBuilder.build();

        computeIndices(structure);

        return structure;
    }

    /**
     * Create indices used by the fold operation.
     *
     * @param structure the new structure.
     */
    private void computeIndices(DataStructure structure) {
        ImmutableSet<String> originalColumns = ImmutableSet.copyOf(getChild().getDataStructure().keySet());
        ImmutableSet<String> columns = ImmutableSet.copyOf(structure.keySet());

        // Indices of the common columns.
        copyIndices = Sets.intersection(columns, originalColumns).stream()
                .mapToInt(originalColumns.asList()::indexOf)
                .toArray();

        elementIndices = Sets.intersection(elements, originalColumns).stream()
                .mapToInt(originalColumns.asList()::indexOf)
                .toArray();

        elementNames = elements.asList().toArray(new String[]{});

        measureIndex = columns.asList().indexOf(measure);
        dimensionIndex = columns.asList().indexOf(dimension);
        size = columns.size();
    }

    /**
     * Fold a single datapoint.
     */
    private Stream<DataPoint> fold(DataPoint dataPoint) {

        // Create a new datapoint
        DataPoint baseDatapoint = DataPoint.create(size);
        for (int i = 0; i < copyIndices.length; i++) {
            baseDatapoint.set(i, dataPoint.get(copyIndices[i]));
        }

        List<DataPoint> foldedDatapoints = Lists.newArrayListWithCapacity(elements.size());
        for (int i = 0; i < elementIndices.length; i++) {

            VTLObject elementValue = dataPoint.get(elementIndices[i]);
            if (VTLObject.NULL == elementValue || elementValue == null || elementValue.get() == null) {
                continue;
            }

            // TODO: Still unclear from benchmark.
            //DataPoint clone = DataPoint.create(baseDatapoint);
            DataPoint clone = (DataPoint) baseDatapoint.clone();

            clone.set(dimensionIndex, VTLObject.of(elementNames[i]));
            clone.set(measureIndex, elementValue);

            foldedDatapoints.add(clone);
        }

        return foldedDatapoints.stream();

    }

    @Override
    public Stream<DataPoint> computeData(Ordering ordering, Filtering filtering, Set<String> components) {
        // To initialize the indices.
        getDataStructure();

        VtlOrdering childOrdering = (VtlOrdering) computeRequiredOrdering(ordering);
        VtlFiltering childFiltering = (VtlFiltering) computeRequiredFiltering(filtering);

        final Stream<DataPoint> original = getChild().computeData(childOrdering, childFiltering, components);
        Stream<DataPoint> stream = original.flatMap(this::fold);

        return new VtlStream(this, stream, original, ordering, filtering, childOrdering, childFiltering);
    }

    @Override
    public FilteringSpecification computeRequiredFiltering(FilteringSpecification filtering) {
        // Transform any filtering referring to the folded columns.
        VtlFiltering transposed = VtlFiltering.using(this).transpose(filtering);
        VtlFiltering transformed = VtlFiltering.transform(transposed, (parent, filter) -> {
            if (measure.equals(filter.getColumn())) {
                List<VtlFiltering> ops = new ArrayList<>();
                for (String element : elements) {
                    ops.add(VtlFiltering.literal(filter.isNegated(), filter.getOperator(), element, filter.getValue()));
                }
                return VtlFiltering.or(ops);
            } else {
                return filter;
            }
        });

        return VtlFiltering.using(getChild().getDataStructure()).transpose(transformed);
    }

    @Override
    public OrderingSpecification computeRequiredOrdering(OrderingSpecification ordering) {

        // Remove the identifier and the dimension since they are not present
        // in the child operation.
        ImmutableMap.Builder<String, Ordering.Direction> foldOrder = ImmutableMap.builder();
        for (String column : ordering.columns()) {
            if (!column.equals(dimension) && !column.equals(measure)) {
                foldOrder.put(column, ordering.getDirection(column));
            }
        }
        return new VtlOrdering(foldOrder.build(), getChild().getDataStructure());
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
        return helper.omitNullValues().toString();
    }
}
