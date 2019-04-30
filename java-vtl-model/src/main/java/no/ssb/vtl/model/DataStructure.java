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

import com.google.common.annotations.Beta;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.*;

/**
 * Data structure of a {@link Dataset}.
 * <p>
 * The data structure defines the role and type of the columns of a data set and
 * serves as a {@link VTLObject}s and {@link DataPoint}s factory.
 */
public class DataStructure extends ForwardingMap<String, Component> {

    private final BiFunction<Object, Class<?>, ?> converter;

    private final ImmutableMap<String, Component> delegate;

    private final IdentityHashMap<Component, String> inverseCache;
    private final ImmutableMap<String, Component.Role> roleCache;
    private final ImmutableMap<String, Class<?>> typeCache;
    private final ImmutableList<Component> indexListCache;

    protected DataStructure(BiFunction<Object, Class<?>, ?> converter, ImmutableMap<String, Component> map) {
        this.converter = checkNotNull(converter);
        this.delegate = checkNotNull(map);
        this.inverseCache = computeInverseCache(this.delegate);
        this.roleCache = computeRoleCache(delegate);
        this.typeCache = computeTypeCache(delegate);
        this.indexListCache = computeIndexCache(delegate);
    }

    private static ImmutableMap<String, Component.Role> computeRoleCache(ImmutableMap<String, Component> delegate) {
        ImmutableMap.Builder<String, Component.Role> builder = ImmutableMap.builder();
        for (Entry<String, Component> entry : delegate.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().getRole());
        }
        return builder.build();
    }

    private static ImmutableMap<String, Class<?>> computeTypeCache(ImmutableMap<String, Component> delegate) {
        ImmutableMap.Builder<String, Class<?>> builder = ImmutableMap.builder();
        for (Entry<String, Component> entry : delegate.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().getType());
        }
        return builder.build();
    }

    private static ImmutableList<Component> computeIndexCache(ImmutableMap<String, Component> delegate) {
        return ImmutableList.copyOf(delegate.values());

    }

    public static DataStructure.Builder builder() {
        return new DataStructure.Builder();
    }

    public static DataStructure.Builder copyOf(
            Map<String, Component> dataStructure
    ) {
        Builder builder = new Builder();
        builder.putAll(dataStructure);
        return builder;
    }

    public static DataStructure of(Map<String, Class<?>> types, Map<String, Component.Role> roles) {
        checkArgument(types.keySet().equals(roles.keySet()));
        Builder builder = builder();
        for (String name : types.keySet()) {
            builder.put(name, roles.get(name), types.get(name));
        }
        return builder.build();
    }

    /**
     * Creates a new data structure.
     */
    public static DataStructure of(String name1, Component.Role role1, Class<?> type1) {
        Builder builder = builder();
        builder.put(name1, role1, type1);
        return builder.build();
    }

    /**
     * Creates a new data structure.
     */
    public static DataStructure of(String name1, Component.Role role1, Class<?> type1,
                                   String name2, Component.Role role2, Class<?> type2) {
        return builder()
                .put(name1, role1, type1).put(name2, role2, type2)
                .build();

    }

    /**
     * Creates a new data structure.
     */
    public static DataStructure of(String name1, Component.Role role1, Class<?> type1,
                                   String name2, Component.Role role2, Class<?> type2,
                                   String name3, Component.Role role3, Class<?> type3) {
        return builder()
                .put(name1, role1, type1).put(name2, role2, type2).put(name3, role3, type3)
                .build();
    }

    /**
     * Creates a new data structure.
     */
    public static DataStructure of(String name1, Component.Role role1, Class<?> type1,
                                   String name2, Component.Role role2, Class<?> type2,
                                   String name3, Component.Role role3, Class<?> type3,
                                   String name4, Component.Role role4, Class<?> type4) {
        return builder()
                .put(name1, role1, type1).put(name2, role2, type2).put(name3, role3, type3).put(name4, role4, type4)
                .build();

    }

    /**
     * Creates a new data structure.
     */
    public static DataStructure of(String name1, Component.Role role1, Class<?> type1,
                                   String name2, Component.Role role2, Class<?> type2,
                                   String name3, Component.Role role3, Class<?> type3,
                                   String name4, Component.Role role4, Class<?> type4,
                                   String name5, Component.Role role5, Class<?> type5) {
        return builder()
                .put(name1, role1, type1).put(name2, role2, type2).put(name3, role3, type3).put(name4, role4, type4)
                .put(name5, role5, type5)
                .build();
    }

    /**
     * Creates a new data structure.
     */
    public static DataStructure of(String name1, Component.Role role1, Class<?> type1,
                                   String name2, Component.Role role2, Class<?> type2,
                                   String name3, Component.Role role3, Class<?> type3,
                                   String name4, Component.Role role4, Class<?> type4,
                                   String name5, Component.Role role5, Class<?> type5,
                                   String name6, Component.Role role6, Class<?> type6) {
        return builder()
                .put(name1, role1, type1).put(name2, role2, type2).put(name3, role3, type3).put(name4, role4, type4)
                .put(name5, role5, type5).put(name6, role6, type6)
                .build();
    }

    private static IdentityHashMap<Component, String> computeInverseCache(ImmutableMap<String, Component> delegate) {
        IdentityHashMap<Component, String> map = Maps.newIdentityHashMap();
        for (Entry<String, Component> entry : delegate.entrySet()) {
            map.put(entry.getValue(), entry.getKey());
        }
        return map;
    }

    public String getName(Component component) {
        return this.inverseCache.get(component);
    }

    public Map<String, Component.Role> getRoles() {
        return this.roleCache;
    }

    public Map<String, Class<?>> getTypes() {
        return this.typeCache;
    }

    public BiFunction<Object, Class<?>, ?> converter() {
        return this.converter;
    }

    /**
     * Return a {@link Map<Component, VTLObject>} view of the {@link DataPoint}.
     * <p>
     * The returned map can be used to edit the DataPoint using the component references.
     *
     * @param dataPoint the datapoint to wrap
     * @return a modifiable map backed by the datatpoint and this structure.
     */
    public Map<Component, VTLObject> asMap(DataPoint dataPoint) {
        checkArgument(
                dataPoint.size() >= this.size(),
                "inconsistent data point size %s, expected %s",
                dataPoint.size(), this.size()
        );
        return new AbstractMap<Component, VTLObject>() {

            @Override
            public VTLObject put(Component key, VTLObject value) {
                int index = indexListCache.indexOf(key);
                return index < 0 ? null : dataPoint.set(index, value);
            }

            @Override
            public VTLObject get(Object key) {
                int index = indexListCache.indexOf(key);
                return index < 0 ? null : dataPoint.get(index);
            }

            @Override
            public VTLObject remove(Object key) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Set<Entry<Component, VTLObject>> entrySet() {
                return new AbstractSet<Entry<Component, VTLObject>>() {
                    @Override
                    public Iterator<Entry<Component, VTLObject>> iterator() {
                        return new Iterator<Entry<Component, VTLObject>>() {

                            int index = 0;

                            @Override
                            public boolean hasNext() {
                                return index < size();
                            }

                            @Override
                            public Entry<Component, VTLObject> next() {
                                return new SimpleEntry<>(
                                        indexListCache.get(index),
                                        dataPoint.get(index++)
                                );
                            }
                        };
                    }

                    @Override
                    public int size() {
                        return indexListCache.size();
                    }
                };
            }
        };
    }

    public Map<VTLObject, Component> asInverseMap(DataPoint dataPoint) {
        Map<VTLObject, Component> map = new HashMap<>();
        for (int i = 0; i < indexListCache.size(); i++) {
            map.put(dataPoint.get(i), indexListCache.get(i));
        }
        return map;
    }

    /**
     * Creates a new {@link DataPoint} for the given names and values.
     * <p>
     * The ordering of VTLObjects is determined by the order of components in {@link #delegate}
     *
     * @param map a map of name and values
     * @return the corresponding tuple (row)
     */
    public DataPoint wrap(Map<String, Object> map) {

        List<VTLObject> components = Lists.newArrayList();

        for (String key : delegate.keySet())
            components.add(VTLObject.of(map.get(key)));

        return DataPoint.create(components);

    }

    /**
     * Creates a new {@link DataPoint} for the given names and values.
     * <p>
     * This method uses the {@link #asMap(DataPoint)} method to convert each value and returns
     * a {@link DataPoint}.
     *
     * @param map a map of name and values
     * @return the corresponding DataPoint (row)
     */
    public DataPoint fromStringMap(Map<String, Object> map) {
        DataPoint dataPoint = DataPoint.create(map.size());
        Map<Component, VTLObject> dataPointAsMap = asMap(dataPoint);
        for (Entry<String, Object> entry : map.entrySet()) {
            dataPointAsMap.put(
                    get(entry.getKey()),
                    VTLObject.of(entry.getValue())
            );
        }
        return dataPoint;
    }

    @Override
    protected Map<String, Component> delegate() {
        return delegate;
    }

    public DataPoint fromMap(Map<Component, VTLObject> map) {
        DataPoint point = DataPoint.create(this.size());
        asMap(point).putAll(map);
        return point;
    }

    public static class Builder {

        private final ImmutableMap.Builder<String, Component> builder = ImmutableMap.builder();
        private final BiFunction<Object, Class<?>, ?> converter;

        private Builder(BiFunction<Object, Class<?>, ?> converter) {
            this.converter = checkNotNull(converter);
        }

        private Builder() {
            this.converter = (o, aClass) -> o;
        }

        /**
         * Associates {@code key} with {@code value} in the built map. Duplicate
         * keys are not allowed, and will cause {@link #build} to fail.
         *
         * @param key
         * @param value
         */
        public Builder put(String key, Component value) {
            builder.put(key, value);
            return this;
        }

        public Builder put(String key, Component.Role role, Class<?> type) {
            return put(key, new Component(type, role));
        }

        /**
         * Adds the given {@code entry} to the map, making it immutable if
         * necessary. Duplicate keys are not allowed, and will cause {@link #build}
         * to fail.
         *
         * @param entry
         */
        public Builder put(Entry<? extends String, ? extends Component> entry) {
            builder.put(entry);
            return this;
        }

        /**
         * Associates all of the given map's keys and values in the built map.
         * Duplicate keys are not allowed, and will cause {@link #build} to fail.
         *
         * @param map
         * @throws NullPointerException if any key or value in {@code map} is null
         */
        public Builder putAll(Map<? extends String, ? extends Component> map) {
            builder.putAll(map);
            return this;
        }

        /**
         * Adds all of the given entries to the built map.  Duplicate keys are not
         * allowed, and will cause {@link #build} to fail.
         *
         * @param entries
         * @throws NullPointerException if any key, value, or entry is null
         */
        @Beta
        public Builder putAll(Iterable<? extends Entry<? extends String, ? extends Component>> entries) {
            builder.putAll(entries);
            return this;
        }

        /**
         * Configures this {@code Builder} to order entries by value according to the specified
         * comparator.
         * <p>
         * <p>The sort order is stable, that is, if two entries have values that compare
         * as equivalent, the entry that was inserted first will be first in the built map's
         * iteration order.
         *
         * @param valueComparator
         * @throws IllegalStateException if this method was already called
         */
        @Beta
        public Builder orderEntriesByValue(Comparator<? super Component> valueComparator) {
            builder.orderEntriesByValue(valueComparator);
            return this;
        }

        public DataStructure build() {
            return new DataStructure(this.converter, this.builder.build());
        }
    }
}
