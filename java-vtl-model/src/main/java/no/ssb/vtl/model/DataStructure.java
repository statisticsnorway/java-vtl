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

import com.google.common.collect.BiMap;
import com.google.common.collect.ForwardingMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Data structure of a {@link Dataset}.
 * <p>
 * The data structure defines the role and type of the columns of a data set and
 * serves as a {@link DataPoint}s and {@link kohl.hadrien.vtl.model.Dataset.Tuple}s factory.
 */
public class DataStructure extends ForwardingMap<String, Component> {

    private final BiMap<String, Component> delegate;
    private final BiFunction<Object, Class<?>, ?> converter;

    protected DataStructure(BiFunction<Object, Class<?>, ?> converter) {
        delegate = HashBiMap.create();
        this.converter = checkNotNull(converter);
    }

    public static DataStructure copyOf(
            BiFunction<Object, Class<?>, ?> converter,
            Map<String, Component> newComponents
    ) {
        DataStructure instance = new DataStructure(converter);
        instance.putAll(newComponents);
        return instance;
    }

    public static DataStructure of(BiFunction<Object, Class<?>, ?> converter, Map<String, Class<?>> types, Map<String, Component.Role> roles) {
        checkArgument(types.keySet().equals(roles.keySet()));
        DataStructure instance = new DataStructure(converter);
        for (String name : types.keySet()) {
            instance.put(name, new Component(types.get(name), roles.get(name), name));
        }
        return instance;
    }

    /**
     * Creates a new data structure.
     */
    public static DataStructure of(BiFunction<Object, Class<?>, ?> converter,
                                   String name1, Component.Role role1, Class<?> type1) {
        DataStructure instance = new DataStructure(converter);
        instance.put(name1, new Component(type1, role1, name1));
        return instance;
    }

    /**
     * Creates a new data structure.
     */
    public static DataStructure of(BiFunction<Object, Class<?>, ?> converter,
                                   String name1, Component.Role role1, Class<?> type1,
                                   String name2, Component.Role role2, Class<?> type2) {
        DataStructure instance = new DataStructure(converter);
        instance.put(name1, new Component(type1, role1, name1));
        instance.put(name2, new Component(type2, role2, name2));
        return instance;
    }

    /**
     * Creates a new data structure.
     */
    public static DataStructure of(BiFunction<Object, Class<?>, ?> converter,
                                   String name1, Component.Role role1, Class<?> type1,
                                   String name2, Component.Role role2, Class<?> type2,
                                   String name3, Component.Role role3, Class<?> type3) {
        DataStructure instance = new DataStructure(converter);
        instance.put(name1, new Component(type1, role1, name1));
        instance.put(name2, new Component(type2, role2, name2));
        instance.put(name3, new Component(type3, role3, name3));
        return instance;
    }

    /**
     * Creates a new data structure.
     */
    public static DataStructure of(BiFunction<Object, Class<?>, ?> converter,
                                   String name1, Component.Role role1, Class<?> type1,
                                   String name2, Component.Role role2, Class<?> type2,
                                   String name3, Component.Role role3, Class<?> type3,
                                   String name4, Component.Role role4, Class<?> type4) {
        DataStructure instance = new DataStructure(converter);
        instance.put(name1, new Component(type1, role1, name1));
        instance.put(name2, new Component(type2, role2, name2));
        instance.put(name3, new Component(type3, role3, name3));
        instance.put(name4, new Component(type4, role4, name4));
        return instance;
    }

    /**
     * Creates a new data structure.
     */
    public static DataStructure of(BiFunction<Object, Class<?>, ?> converter,
                                   String name1, Component.Role role1, Class<?> type1,
                                   String name2, Component.Role role2, Class<?> type2,
                                   String name3, Component.Role role3, Class<?> type3,
                                   String name4, Component.Role role4, Class<?> type4,
                                   String name5, Component.Role role5, Class<?> type5) {
        DataStructure instance = new DataStructure(converter);
        instance.put(name1, new Component(type1, role1, name1));
        instance.put(name2, new Component(type2, role2, name2));
        instance.put(name3, new Component(type3, role3, name3));
        instance.put(name4, new Component(type4, role4, name4));
        instance.put(name5, new Component(type5, role5, name5));
        return instance;
    }

    /**
     * Creates a new data structure.
     */
    public static DataStructure of(BiFunction<Object, Class<?>, ?> converter,
                                   String name1, Component.Role role1, Class<?> type1,
                                   String name2, Component.Role role2, Class<?> type2,
                                   String name3, Component.Role role3, Class<?> type3,
                                   String name4, Component.Role role4, Class<?> type4,
                                   String name5, Component.Role role5, Class<?> type5,
                                   String name6, Component.Role role6, Class<?> type6) {
        DataStructure instance = new DataStructure(converter);
        instance.put(name1, new Component(type1, role1, name1));
        instance.put(name2, new Component(type2, role2, name2));
        instance.put(name3, new Component(type3, role3, name3));
        instance.put(name4, new Component(type4, role4, name4));
        instance.put(name5, new Component(type5, role5, name5));
        instance.put(name6, new Component(type6, role6, name6));
        return instance;
    }

    public String getName(Component component) {
        return delegate.inverse().get(component);
    }

    public Component addComponent(String name, Component.Role role, Class<?> type) {
        Component component = new Component(type, role, name);
        put(name, component);
        return component;
    }

    public Map<String, Component.Role> getRoles() {
        // TODO: Cache.
        return this.entrySet().stream()
                .collect(Collectors.toMap(
                        Entry::getKey,
                        entry -> entry.getValue().getRole()
                ));
    }

    public Map<String, Class<?>> getTypes() {
        // TODO: Cache.
        return this.entrySet().stream()
                .collect(Collectors.toMap(
                        Entry::getKey,
                        entry -> entry.getValue().getType()
                ));
    }


    public BiFunction<Object, Class<?>, ?> converter() {
        return this.converter;
    }

    ;

    /**
     * Creates a new {@link Component} for the given column and value.
     *
     * @param name  the name of the column.
     * @param value the value of the resulting component.
     * @return a component
     */
    public DataPoint wrap(String name, Object value) {
        checkArgument(
                containsKey(name),
                "could not find %s in data structure %s",
                name, this
        );

        Component component = get(name);
        return new DataPoint(component) {
            @Override
            public Object get() {
                return converter().apply(value, component.getType());
            }
        };
    }

    /**
     * Creates a new {@link Dataset.Tuple} for the given names and values.
     * <p>
     * This method uses the {@link #wrap(String, Object)} method to convert each value and returns
     * a {@link Dataset.Tuple}.
     *
     * @param map a map of name and values
     * @return the corresponding tuple (row)
     */
    public Dataset.Tuple wrap(Map<String, Object> map) {

        List<DataPoint> components = Lists.newArrayList();
        for (Map.Entry<String, Object> entry : map.entrySet())
            components.add(wrap(entry.getKey(), entry.getValue()));

        return Dataset.Tuple.create(components);

    }

    @Override
    protected Map<String, Component> delegate() {
        return delegate;
    }
}
