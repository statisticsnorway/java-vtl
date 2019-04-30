package no.ssb.vtl.model;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2018 Hadrien Kohl
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
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Use this class to simplify implementation of the {@link Ordering} and {@link OrderingSpecification}
 */
public final class VtlOrdering implements Ordering {

    public static final Comparator<Map.Entry<String, Component>> BY_ROLE = Comparator.comparing(
            entry -> entry.getValue().getRole(),
            com.google.common.collect.Ordering.explicit(
                    Component.Role.IDENTIFIER,
                    Component.Role.MEASURE,
                    Component.Role.ATTRIBUTE
            )
    );

    public static final Comparator<Map.Entry<String, Component>> BY_NAME = Comparator.comparing(Map.Entry::getKey);

    private final ImmutableMap<String, Direction> delegate;
    private final int[] indices;
    private final Direction[] directions;

    public VtlOrdering(OrderingSpecification specification, DataStructure structure) {
        this(toMap(specification), structure);
    }

    public VtlOrdering(Map<String, Direction> specification, DataStructure structure) {
        this.delegate = ImmutableMap.copyOf(specification);

        ArrayList<Integer> indices = Lists.newArrayList();
        ArrayList<Direction> directions = Lists.newArrayList();

        Sets.SetView<String> difference = Sets.difference(specification.keySet(), structure.keySet());
        if (!difference.isEmpty()) {
            throw new IllegalArgumentException(String.format(
                    "columns %s not found in structure",
                    difference
            ));
        }

        ImmutableList<String> columns = ImmutableList.copyOf(structure.keySet());
        for (String column : specification.keySet()) {
            indices.add(columns.indexOf(column));
            directions.add(specification.get(column));
        }

        this.indices = Ints.toArray(indices);
        this.directions = directions.toArray(new Direction[]{});
    }

    private static ImmutableMap<String, Direction> toMap(OrderingSpecification specification) {
        ImmutableMap.Builder<String, Direction> delegate = ImmutableMap.builder();
        for (String column : specification.columns()) {
            delegate.put(column, specification.getDirection(column));
        }
        return delegate.build();
    }

    public static Builder using(DataStructure structure) {
        return new Builder(structure);
    }

    public static Builder using(Dataset dataset) {
        return new Builder(dataset.getDataStructure());
    }

    public final ImmutableMap<String, Direction> toMap() {
        return delegate;
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper stringHelper = MoreObjects.toStringHelper(this);
        for (String column : columns()) {
            stringHelper.add(column, getDirection(column));
        }
        return stringHelper.toString();
    }

    @Override
    public List<String> columns() {
        return delegate.keySet().asList();
    }

    @Override
    public Direction getDirection(String column) {
        return delegate.get(column);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VtlOrdering other = (VtlOrdering) o;

        // Ordering is the same if all columns/direction in this
        // are present and in the same order in other.

        Iterator<Map.Entry<String, Direction>> it = delegate.entrySet().iterator();
        Iterator<Map.Entry<String, Direction>> otherIt = other.delegate.entrySet().iterator();
        while (it.hasNext() && otherIt.hasNext()) {
            if (!Objects.equal(it.next(), otherIt.next())) {
                return false;
            }
        }
        return !it.hasNext();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(delegate);
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(DataPoint dp1, DataPoint dp2) {
        int result;

        for (int i = 0; i < indices.length; i++) {

            Comparable o1 = (Comparable) dp1.get(indices[i]).get();
            Comparable o2 = (Comparable) dp2.get(indices[i]).get();

            // TODO: Nullfirst?
            boolean nullFirst = false;
            if (o1 == null) {
                result = (o2 == null) ? 0 : (nullFirst ? -1 : 1);
            } else if (o2 == null) {
                result = nullFirst ? 1 : -1;
            } else {
                result = o1.compareTo(o2);
            }

            if (result != 0) {
                return directions[i] == Direction.ASC ? result : -result;
            }
        }
        return 0;
    }

    public static class Builder {

        private ImmutableMap.Builder<String, Direction> map = ImmutableMap.builder();
        private DataStructure structure;

        private Builder(DataStructure structure) {
            this.structure = checkNotNull(structure);
        }

        public VtlOrdering build() {
            return new VtlOrdering(map.build(), structure);
        }

        public Builder then(Direction direction, String... columns) {
            for (String column : columns) {
                this.map.put(column, direction);
            }
            return this;
        }

        public Builder any(String... columns) {
            return then(Direction.ANY, columns);
        }

        public Builder asc(String... columns) {
            return then(Direction.ASC, columns);
        }

        public Builder desc(String... columns) {
            return then(Direction.DESC, columns);
        }


    }
}
