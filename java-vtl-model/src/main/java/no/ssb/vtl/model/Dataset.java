package kohl.hadrien.vtl.model;

/*-
 * #%L
 * java-vtl-model
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

import com.codepoetics.protonpack.Streamable;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ForwardingList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A data structure that allows relational operations.
 */
public interface Dataset extends Streamable<Dataset.Tuple> {

    static Comparator<Tuple> comparatorFor(Component.Role... roles) {
        ImmutableSet<Component.Role> roleSet = Sets.immutableEnumSet(Arrays.asList(roles));
        return new Comparator<Tuple>() {
            @Override
            public int compare(Tuple li, Tuple ri) {
                Comparator comparator = Comparator.naturalOrder();

                Map<String, Object> lm = li.stream().filter(dataPoint -> roleSet.contains(dataPoint.getRole()))
                        .collect(Collectors.toMap(
                                DataPoint::getName,
                                DataPoint::get
                        ));

                Map<String, Object> rm = ri.stream().filter(dataPoint -> roleSet.contains(dataPoint.getRole()))
                        .collect(Collectors.toMap(
                                DataPoint::getName,
                                DataPoint::get
                        ));

                checkArgument(lm.keySet().equals(rm.keySet()));
                int i = 0;
                for (String key : lm.keySet()) {
                    i = comparator.compare(lm.get(key), rm.get(key));
                    if (i != 0)
                        return i;
                }
                return i;

            }
        };
    }

    /**
     * Returns the data structure of the DataSet.
     */
    DataStructure getDataStructure();

    interface Tuple extends List<DataPoint>, Comparable<Tuple> {

        static Tuple create(List<DataPoint> components) {
            return new AbstractTuple() {
                @Override
                protected List<DataPoint> delegate() {
                    return components;
                }
            };
        }

        List<DataPoint> ids();

        List<DataPoint> values();

    }

    abstract class AbstractTuple extends ForwardingList<DataPoint> implements Tuple {

        @Override
        public List<DataPoint> ids() {
            return stream()
                    .filter(dataPoint -> dataPoint.getComponent().getRole().equals(Component.Role.IDENTIFIER))
                    .collect(Collectors.toList());
        }

        @Override
        public List<DataPoint> values() {
            return stream()
                    .filter(dataPoint -> !dataPoint.getComponent().getRole().equals(Component.Role.IDENTIFIER))
                    .collect(Collectors.toList());
        }

        @Override
        public int compareTo(Tuple o) {
            checkNotNull(o);

            Comparator comparator = Comparator.naturalOrder();
            Iterator<DataPoint> li = this.ids().iterator();
            Iterator<DataPoint> ri = o.ids().iterator();
            int i = 0;
            while (li.hasNext() && ri.hasNext()) {
                i = comparator.compare(li.next().get(), ri.next().get());
                if (i != 0)
                    return i;
            }
            return i;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(Tuple.class)
                    .add("id", ids())
                    .add("values", values())
                    .toString();
        }

    }

}
