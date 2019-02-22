package no.ssb.vtl.script.operations.union;

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
import com.codepoetics.protonpack.selectors.Selector;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.FilteringSpecification;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.OrderingSpecification;
import no.ssb.vtl.model.VtlFiltering;
import no.ssb.vtl.model.VtlOrdering;
import no.ssb.vtl.script.operations.AbstractDatasetOperation;
import no.ssb.vtl.script.operations.DataPointMap;
import no.ssb.vtl.script.operations.DataPointMapComparator;
import no.ssb.vtl.script.operations.VtlStream;
import no.ssb.vtl.script.operations.join.DataPointCapacityExpander;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Arrays.asList;
import static no.ssb.vtl.model.DataStructure.Entry;
import static no.ssb.vtl.model.DataStructure.builder;

/**
 * Union operator
 */
public class UnionOperation extends AbstractDatasetOperation {

    @VisibleForTesting
    UnionOperation(Dataset... dataset) {
        this(asList(dataset));
    }

    public UnionOperation(List<Dataset> datasets) {
        super(datasets);
        Iterator<Dataset> iterator = datasets.iterator();
        DataStructure firstDataStructure = iterator.next().getDataStructure();
        while (iterator.hasNext())
            checkDataStructures(firstDataStructure, iterator.next().getDataStructure());
    }

    @Override
    protected DataStructure computeDataStructure() {
        if (getChildren().size() == 1) {
            return getChildren().get(0).getDataStructure();
        }

        // Get base structure, that is, structure from first parameter, without attributes
        List<Entry<String, Component>> baseStructure
                = new ArrayList<>(getChildren().get(0).getDataStructure().entrySet())
                .stream().filter(entry -> entry.getValue().getRole() != Component.Role.ATTRIBUTE)
                .collect(Collectors.toList());

        // Add attributes, sorted by name
        List<Entry<String, Component>> allAttributes = new ArrayList<>();
        for(Dataset dataset : getChildren()) {
            List<Entry<String, Component>> childAttributes = dataset.getDataStructure().entrySet()
                    .stream().filter(entry -> entry.getValue().getRole() == Component.Role.ATTRIBUTE)
                    .filter(entry -> allAttributes.stream()
                            .noneMatch(attributeEntry -> attributeEntry.getKey().equals(entry.getKey())))
                    .collect(Collectors.toList());
            allAttributes.addAll(childAttributes);
        }
        allAttributes.sort(Comparator.comparing(Entry::getKey));
        baseStructure.addAll(allAttributes);
        return builder().putAll(baseStructure).build();
    }

    @Override
    public FilteringSpecification computeRequiredFiltering(FilteringSpecification filtering) {
        return VtlFiltering.using(this).transpose(filtering);
    }

    @Override
    public OrderingSpecification computeRequiredOrdering(OrderingSpecification ordering) {
        // Union requires data to be sorted on all identifiers. Start with requested. Add all missing.
        VtlOrdering.Builder unionOrder = VtlOrdering.using(this);
        for (String column : ordering.columns()) {
            unionOrder.then(ordering.getDirection(column), column);
        }
        DataStructure structure = getDataStructure();
        for (String column : structure.keySet()) {
            if (ordering.columns().contains(column)) {
                continue;
            }
            if (structure.get(column).isIdentifier()) {
                unionOrder.asc(column);
            }
        }
        return unionOrder.build();
    }

    private void checkDataStructures(DataStructure baseDataStructure, DataStructure nextDataStructure) {
        // Identifiers and attribute should be equals in name, role and type.
        Set<String> requiredNames = nonAttributeNames(baseDataStructure);
        Set<String> providedNames = nonAttributeNames(nextDataStructure);

        checkArgument(
                requiredNames.equals(providedNames),
                "dataset was incompatible with the required data structure, missing: %s, unexpected %s",
                Sets.difference(requiredNames, providedNames),
                Sets.difference(providedNames, requiredNames)
        );

        Map<String, Component.Role> requiredRoles = Maps.filterKeys(baseDataStructure.getRoles(), requiredNames::contains);
        Map<String, Component.Role> providedRoles = Maps.filterKeys(nextDataStructure.getRoles(), requiredNames::contains);

        checkArgument(
                requiredRoles.equals(providedRoles),
                "dataset was incompatible with the required data structure, missing: %s, unexpected %s",
                Sets.difference(requiredRoles.entrySet(), providedRoles.entrySet()),
                Sets.difference(providedRoles.entrySet(), requiredRoles.entrySet())
        );

        Map<String, Class<?>> requiredTypes = Maps.filterKeys(baseDataStructure.getTypes(), requiredNames::contains);
        Map<String, Class<?>> providedTypes = Maps.filterKeys(nextDataStructure.getTypes(), requiredNames::contains);

        checkArgument(
                requiredTypes.equals(providedTypes),
                "dataset was incompatible with the required data structure, missing: %s, unexpected %s",
                Sets.difference(requiredTypes.entrySet(), providedTypes.entrySet()),
                Sets.difference(providedTypes.entrySet(), requiredTypes.entrySet())
        );

    }

    private Set<String> nonAttributeNames(DataStructure dataStructure) {
        return Maps.filterValues(dataStructure.getRoles(), role -> role != Component.Role.ATTRIBUTE).keySet();
    }

    @Override
    public Stream<DataPoint> computeData(Ordering ordering, Filtering filtering, Set<String> components) {

        // Optimization.
        if (getChildren().size() == 1)
            return getChildren().get(0).computeData(ordering, filtering, components);

        VtlFiltering childFiltering = (VtlFiltering) computeRequiredFiltering(filtering);
        VtlOrdering unionOrder = (VtlOrdering) computeRequiredOrdering(ordering);

        DataStructure structure = getDataStructure();
        ImmutableList.Builder<Stream<DataPointMap>> streams = ImmutableList.builder();
        ImmutableList.Builder<Stream<DataPoint>> originals = ImmutableList.builder();
        for (AbstractDatasetOperation child : getChildren()) {

            DataStructure childStructure = getNormalizedChildStructure(child.getDataStructure(), structure);
            VtlOrdering unionOrdering = new VtlOrdering(unionOrder, childStructure);
            VtlFiltering unionFilter = VtlFiltering.using(child).transpose(childFiltering);

            Stream<DataPoint> stream = child.computeData(unionOrdering, unionFilter, components)
                    .peek(
                            new DataPointCapacityExpander(structure.size()));

            originals.add(stream);
            Stream<DataPoint> testStream = StreamSupport.stream(stream.spliterator(), false);

            DataPointMap map = new DataPointMap(childStructure);
            Stream<DataPointMap> dataPointMapStream = testStream.map(map::withDataPoint);
            streams.add(dataPointMapStream);
        }

        VtlOrdering unionOrdering = new VtlOrdering(unionOrder, structure);
        Comparator<DataPointMap> comparing = new DataPointMapComparator(unionOrdering);

        ImmutableList<Stream<DataPointMap>> build = streams.build();

        DataPointMap resultMap = new DataPointMap(structure);
        Stream<DataPoint> result = StreamUtils.interleave(
                createSelector(comparing), build)
                .map(source -> {
                    resultMap.setDataPoint(DataPoint.create(structure.size()));
                    structure.keySet().forEach(col -> resultMap.set(col, source.get(col)));
                    return resultMap.getDataPoint();
                }).map(new DuplicateChecker(unionOrdering, structure));

        return new VtlStream(
                this, result, originals.build(), ordering, filtering, unionOrdering, childFiltering);
    }

    /**
     * Concatenates the child's structure with the base structure, to add attributes not present in child
     *
     * @param childStructure the child's structure
     * @param baseStructure the base structure for the expression, containing the sum of all attributes from all
     *                      parameters
     * @return the concatenated structure
     */
    private DataStructure getNormalizedChildStructure(DataStructure childStructure, DataStructure baseStructure) {
        if (childStructure.getRoles().size() == baseStructure.getRoles().size()
                && childStructure.getRoles().keySet().equals(baseStructure.getRoles().keySet())) {
            return childStructure;
        }

        List<Entry<String, Component>> childStructureList = new ArrayList<>(childStructure.entrySet());

        // append missing attributes from baseStructure
        childStructureList.addAll(baseStructure.entrySet().stream()
                .filter(entry -> childStructureList.stream()
                        .map(Entry::getKey).noneMatch(key -> key.equals(entry.getKey())))
                .collect(Collectors.toList()));

        return DataStructure.builder().putAll(childStructureList).build();
    }

    private <T> Selector<T> createSelector(Comparator<T> comparator) {
        return new MinimumSelector<>(comparator);
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return Optional.empty();
    }

    @Override
    public Optional<Long> getSize() {
        Long size = 0L;
        for (Dataset child : getChildren()) {
            Optional<Long> childSize = child.getSize();
            if (!childSize.isPresent())
                return Optional.empty();
            size += childSize.get();
        }
        return Optional.of(size);
    }

}
