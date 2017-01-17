package no.ssb.vtl.script.visitors.join;
/*-
 * #%L
 * java-vtl-script
 * %%
 * Copyright (C) 2016 Hadrien Kohl
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
 * #L%
 */

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.script.operations.RenameOperation;

import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static no.ssb.vtl.model.Component.Role;

/**
 * Abstract join operation.
 */
public abstract class AbstractJoinOperation implements Dataset {

    // The datasets the join operates on.
    private final Map<String, Dataset> datasets = Maps.newHashMap();

    public Set<String> getCommonIdentifierNames() {
        return commonIdentifierNames;
    }

    // The identifiers that will be used to join the datasets.
    private final Set<String> commonIdentifierNames;

    // Holds the operations of the join.
    private final List<JoinClause> clauses = Lists.newArrayList();
    //private final Iterator<JoinClause> clauseIterator = ;

    private WorkingDataset workingDataset;

    public AbstractJoinOperation(Map<String, Dataset> namedDatasets) {

        checkArgument(
                !namedDatasets.isEmpty(),
                "join operation impossible on empty dataset list"
        );

        // Find the common identifier.
        Multiset<Component> components = HashMultiset.create();
        for (Dataset dataset : namedDatasets.values()) {
            DataStructure structure = dataset.getDataStructure();
            components.addAll(structure.values());
        }

        commonIdentifierNames = components.entrySet().stream()
                .filter(entry -> entry.getCount() == namedDatasets.size())
                .map(Multiset.Entry::getElement)
                .filter(component -> component.getRole() == Role.IDENTIFIER)
                .map(Component::getName)
                .collect(Collectors.toSet());
        checkArgument(!commonIdentifierNames.isEmpty(), "could not find common identifiers in the datasets %s", namedDatasets);


        // Rename all the components except the common identifiers.
        for (String datasetName : namedDatasets.keySet()) {
            Dataset dataset = namedDatasets.get(datasetName);

            Map<String, String> newNames = Maps.newHashMap();
            Map<String, Component.Role> newRoles = Maps.newHashMap();

            for (Component component : dataset.getDataStructure().values()) {
                String newName;
                if (commonIdentifierNames.contains(component.getName())) {
                    newName = component.getName();
                } else {
                    newName = datasetName.concat(".").concat(component.getName());
                }
                newNames.put(component.getName(), newName);
                newRoles.put(component.getName(), component.getRole());
            }
            this.datasets.put(datasetName, new RenameOperation(dataset, newNames, newRoles));
        }

    }

    Map<String, Dataset> getDatasets() {
        return datasets;
    }

    Set<String> getIds() {
        return commonIdentifierNames;
    }

    public List<JoinClause> getClauses() {
        return clauses;
    }

    abstract WorkingDataset workDataset();

    private WorkingDataset applyClauses() {
        WorkingDataset dataset = workDataset();
        for (JoinClause clause : clauses) {
            dataset = clause.apply(dataset);
        }
        return dataset;
    }
    @Override
    public Stream<Tuple> get() {
        return (workingDataset = (workingDataset == null ? applyClauses() : workingDataset)).get();
    }

    @Override
    public DataStructure getDataStructure() {
        return (workingDataset = (workingDataset == null ? applyClauses() : workingDataset)).getDataStructure();
    }

    /**
     * Holds the "working dataset" tuples.
     */
    static final class JoinTuple extends Dataset.AbstractTuple implements RandomAccess {

        private final List<DataPoint> delegate = Lists.newArrayList();

        public JoinTuple(List<DataPoint> ids) {
            this.delegate.addAll(ids);
        }

        @Override
        protected List<DataPoint> delegate() {
            return delegate;
        }

    }
}
