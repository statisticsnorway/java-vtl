package no.ssb.vtl.model;

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

import com.google.common.annotations.Beta;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.*;
import static no.ssb.vtl.model.Order.Direction.*;

/**
 * Represent the ordering the {@link DataPoint}s in a Dataset.
 */
public final class Order extends ForwardingMap<Component, Order.Direction> implements Comparator<DataPoint> {

    public static final Comparator<Comparable> NULLS_FIRST = Comparator.<Comparable>nullsFirst(Comparator.naturalOrder());

    public static final Comparator<Entry<String, Component>> BY_ROLE = Comparator.comparing(
            entry -> entry.getValue().getRole(),
            Ordering.explicit(
                    Component.Role.IDENTIFIER,
                    Component.Role.MEASURE,
                    Component.Role.ATTRIBUTE
            )
    );

    public static final Comparator<Entry<String, Component>> BY_NAME = Comparator.comparing(Entry::getKey);

    private final DataStructure structure;
    private final ImmutableMap<Component, Direction> delegate;

    Order(DataStructure structure, ImmutableMap<Component, Direction> orders) {
        this.delegate = ImmutableMap.copyOf(orders);
        this.structure = checkNotNull(structure);
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper stringHelper = MoreObjects.toStringHelper(this);
        for (Entry<Component, Direction> directionEntry : delegate.entrySet()) {
            String name = structure.getName(directionEntry.getKey());
            Direction direction = directionEntry.getValue();
            stringHelper.add(name, direction);
        }
        return stringHelper.toString();
    }

    /**
     * Create a new Order.Builder instance.
     *
     * @param structure the structure this Order will operate on.
     */
    public static Builder create(DataStructure structure) {
        return new Builder(structure);
    }

    /**
     * Create a copy of the order.
     * <p>
     * Useful to change data structure.
     */
    public static Order.Builder createCopyOf(Order order) {
        return create(order.structure);
    }

    /**
     * Return the default Order for the given DataStructure.
     */
    public static Order createDefault(DataStructure dataStructure) {
        Set<Entry<String, Component>> sortedEntrySet = Sets.newTreeSet(BY_ROLE.thenComparing(BY_NAME));
        sortedEntrySet.addAll(dataStructure.entrySet());

        ImmutableMap.Builder<Component, Direction> order = ImmutableMap.builder();
        for (Entry<String, Component> entry : sortedEntrySet) {
            order.put(entry.getValue(), ASC);
        }
        return new Order(dataStructure, order.build());
    }

    @Override
    public int compare(DataPoint o1, DataPoint o2) {
        int result;

        // TODO migrate to Map<Component, Direction> and remove the DataStructure dependency.
        Map<Component, ? extends Comparable> m1 = structure.asMap(o1), m2 = structure.asMap(o2);
        for (Entry<Component, Direction> order : delegate.entrySet()) {
            result = NULLS_FIRST.compare(m1.get(order.getKey()), m2.get(order.getKey()));
            if (result != 0) {
                return order.getValue() == ASC ? result : -result;
            }
        }

        // TODO build an index?
//        Comparable[] c1 = new Comparable[1], c2 = new Comparable[1];
//        ImmutableMap<Integer, Direction> index = ImmutableMap.copyOf(Collections.emptyMap());
//        for (Entry<Integer, Direction> order : index.entrySet()) {
//            Integer i = order.getKey();
//            result = NULLS_FIRST.compare(c1[i], c2[i]);
//            if (result != 0) {
//                return order.getValue() == ASC ? result : -result;
//            }
//        }
        return 0;
    }

    @Override
    protected Map<Component, Direction> delegate() {
        return delegate;
    }

    public enum Direction {
        ASC, DESC
    }

    public static class Builder {

        private final ImmutableMap.Builder<Component, Direction> delegate = ImmutableMap.builder();
        private final DataStructure structure;

        private Builder(DataStructure structure) {
            this.structure = checkNotNull(structure);
        }

        public Builder put(Component key, Direction value) {
            delegate.put(key, value);
            return this;
        }

        public Builder put(String key, Direction value) {
            delegate.put(structure.get(key), value);
            return this;
        }

        public Builder put(Entry<Component, Direction> entry) {
            delegate.put(entry);
            return this;
        }

        public Builder putAll(Map<Component, Direction> map) {
            delegate.putAll(map);
            return this;
        }

        @Beta
        public Builder putAll(Iterable<? extends Entry<Component, Direction>> entries) {
            delegate.putAll(entries);
            return this;
        }

        public Order build() {
            return new Order(structure, delegate.build());
        }
    }
}
