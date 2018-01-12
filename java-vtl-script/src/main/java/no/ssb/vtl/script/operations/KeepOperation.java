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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import no.ssb.vtl.model.AbstractUnaryDatasetOperation;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;

import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Keep operation
 */
public class KeepOperation extends AbstractUnaryDatasetOperation {

    protected final Set<Component> components;

    public KeepOperation(Dataset dataset, Set<Component> names) {
        super(checkNotNull(dataset, "the dataset was null"));
        this.components = checkNotNull(names, "the component list was null");

        checkArgument(!names.isEmpty(), "the list of component to keep was null");
    }

    /**
     * Compute the new data structure.
     */
    @Override
    protected DataStructure computeDataStructure() {
        DataStructure.Builder newDataStructure = DataStructure.builder();
        for (Map.Entry<String, Component> componentEntry : getChild().getDataStructure().entrySet()) {
            Component component = componentEntry.getValue();
            if (components.contains(component) || component.isIdentifier()) {
                newDataStructure.put(componentEntry);
            }
        }
        return newDataStructure.build();
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.addValue(components);
        helper.add("structure", getDataStructure());
        return helper.omitNullValues().toString();
    }

    @Override
    public Stream<DataPoint> getData() {

        ImmutableList<Component> componentsToRemove = getComponentsToRemove();

        // Optimization.
        if (componentsToRemove.isEmpty())
            return getChild().getData();

        // Compute indexes to remove (in reverse order to avoid shifting).
        final ImmutableSet<Integer> indexes = computeIndexes(componentsToRemove);

        return getChild().getData().peek(
                dataPoints -> {
                    for (Integer index : indexes)
                        dataPoints.remove((int) index);
                }
        );
    }

    /**
     * Find the index of the component in the child data structure.
     */
    private ImmutableSet<Integer> computeIndexes(List<Component> componentsToRemove) {
        TreeSet<Integer> indexes = Sets.newTreeSet();
        List<Component> components = Lists.newArrayList(getChild().getDataStructure().values());
        for (Component component : componentsToRemove) {
            ListIterator<Component> iterator = components.listIterator();
            while (iterator.hasNext()) {
                int index = iterator.nextIndex();
                Component next = iterator.next();
                if (component.equals(next))
                    indexes.add(index);
            }
        }
        return ImmutableSet.copyOf(indexes.descendingIterator());
    }

    /**
     * Compute the list of component that need to be removed.
     */
    private ImmutableList<Component> getComponentsToRemove() {
        HashSet<Component> oldComponents = Sets.newLinkedHashSet(getChild().getDataStructure().values());
        HashSet<Component> newComponents = Sets.newLinkedHashSet(getDataStructure().values());

        return ImmutableList.copyOf(Sets.difference(oldComponents, newComponents));
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return getChild().getDistinctValuesCount();
    }

    @Override
    public Optional<Long> getSize() {
        return getChild().getSize();
    }
}
