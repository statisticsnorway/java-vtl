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

import com.google.common.collect.Maps;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.script.operations.join.AbstractJoinOperation;
import no.ssb.vtl.script.support.JoinSpliterator;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Represent an inner join on datasets.
 */
public class InnerJoinOperation extends AbstractJoinOperation {

    public InnerJoinOperation(Map<String, Dataset> namedDatasets) {
        super(namedDatasets);
    }

    @Override
    WorkingDataset workDataset() {

        return new WorkingDataset() {
            @Override
            public DataStructure getDataStructure() {
                Map<String, Component> newComponents = Maps.newHashMap();
                Map<String, Dataset> datasets = getDatasets();

                for (String datasetName : datasets.keySet()) {
                    DataStructure structure = datasets.get(datasetName).getDataStructure();
                    for (String componentName : structure.keySet()) {
                        newComponents.put(componentName, structure.get(componentName));
                    }
                }

                return DataStructure.copyOf((o, aClass) -> o, newComponents);
            }

            @Override
            public Stream<Tuple> get() {
                Collection<Dataset> datasets = getDatasets().values();
                checkArgument(!datasets.isEmpty(), "no dataset for the join %s", this);

                // Optimization.
                if (datasets.size() == 1) {
                    return datasets.iterator().next().get();
                }

                // Simple algorithm for now.
                final Set<String> dimensions = getIds();

                // Get the key comparator.
                Comparator<List<DataPoint>> keyComparator = getKeyComparator(dimensions);

                // How to merge the tuples.
                BiFunction<JoinTuple, Dataset.Tuple, JoinTuple> merger = getMerger();

                DataStructure joinedDataStructure = getDataStructure();
                Iterator<Dataset> iterator = datasets.iterator();
                Stream<JoinTuple> result = iterator.next().get().map(components -> {
                    components.replaceAll(dataPoint -> {
                        // Get the new component.
                        Component newComponent = joinedDataStructure.get(joinedDataStructure.getName(dataPoint.getComponent()));
                        return new DataPoint(newComponent) {
                            @Override
                            public Object get() {
                                return dataPoint.get();
                            }
                        };
                    });
                    JoinTuple joinTuple = new JoinTuple(components.ids());
                    joinTuple.addAll(components.values());
                    return joinTuple;
                });

                while (iterator.hasNext()) {
                    Function<Tuple, List<DataPoint>> keyExtractor = tuple -> {
                        // Filter by common ids.
                        List<DataPoint> ids = tuple.stream().filter(dataPoint ->
                                getCommonIdentifierNames().contains(dataPoint.getName())
                        ).collect(Collectors.toList());
                        return ids;
                    };
                    Function<JoinTuple, List<DataPoint>> joinKeyExtractor = tuple -> {
                        // Filter by common ids.
                        List<DataPoint> ids =  tuple.stream().filter(dataPoint ->
                                getCommonIdentifierNames().contains(dataPoint.getName())
                        ).collect(Collectors.toList());
                        return ids;
                    };
                    result = StreamSupport.stream(
                            new JoinSpliterator<>(
                                    keyComparator,
                                    result.spliterator(),
                                    iterator.next().get().spliterator(),
                                    joinKeyExtractor,
                                    keyExtractor,
                                    merger
                            ), false
                    );
                }
                return result.map(tuple -> (Tuple) tuple);
            }
        };
    }

    private BiFunction<JoinTuple, Tuple, JoinTuple> getMerger() {
        return new BiFunction<JoinTuple, Tuple, JoinTuple>() {

            @Override
            public JoinTuple apply(JoinTuple joinTuple, Tuple components) {
                joinTuple.addAll(components.values());
                return joinTuple;
            }
        };
    }

    private Comparator<List<DataPoint>> getKeyComparator(final Set<String> dimensions) {
        return new Comparator<List<DataPoint>>() {
            @Override
            public int compare(List<DataPoint> l, List<DataPoint> r) {
                // TODO: Tuple should expose method to handle this.
                // TODO: Evaluate migrating to DataPoint.
                // TODO: When using on, the left over identifiers should be transformed to measures.
                Map<String, Comparable> lIds = l.stream()
                        .collect(Collectors.toMap(
                                DataPoint::getName,
                                t -> (Comparable) t.get()
                        ));
                Map<String, Object> rIds = r.stream()
                        .collect(Collectors.toMap(
                                DataPoint::getName,
                                Supplier::get
                        ));
                for (String key : dimensions) {
                    int res = lIds.get(key).compareTo(rIds.get(key));
                    if (res != 0)
                        return res;
                }
                return 0;
            }
        };
    }
}
