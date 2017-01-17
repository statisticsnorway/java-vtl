package kohl.hadrien.vtl.script.operations;

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

import com.google.common.base.MoreObjects;
import com.google.common.collect.*;
import kohl.hadrien.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Rename operation.
 *
 * TODO: Implement {@link Dataset}
 */
public class RenameOperation implements Dataset {

    private final Dataset dataset;
    private final ImmutableMap<String, String> names;
    private final ImmutableMap<String, Component.Role> roles;
    Map<String, String> mapping = HashBiMap.create();
    private DataStructure cache;

    public RenameOperation(Dataset dataset, Map<String, String> names, Map<String, Component.Role> roles) {
        this.dataset = checkNotNull(dataset, "dataset was null");

        // Checks that names key and values are unique.
        HashBiMap.create(names);

        checkArgument(names.keySet().containsAll(roles.keySet()), "keys %s not present in %s",
                Sets.difference(roles.keySet(), names.keySet()), names.keySet());

        this.names = ImmutableMap.copyOf(names);
        this.roles = ImmutableMap.copyOf(roles);
    }

    @Override
    public DataStructure getDataStructure() {

        if (cache == null) {
            Set<String> oldNames = dataset.getDataStructure().keySet();
            checkArgument(oldNames.containsAll(names.keySet()),
                    "the data set %s did not contain components with names %s", dataset,
                    Sets.difference(names.keySet(), oldNames)
            );

            // Copy.
            final Map<String, Component.Role> oldRoles, newRoles;
            oldRoles = dataset.getDataStructure().getRoles();
            newRoles = Maps.newHashMap();

            final Map<String, Class<?>> oldTypes, newTypes;
            oldTypes = dataset.getDataStructure().getTypes();
            newTypes = Maps.newHashMap();



            for (String oldName : oldNames) {
                String newName = names.getOrDefault(oldName, oldName);
                newTypes.put(newName, oldTypes.get(oldName));
                newRoles.put(newName, roles.getOrDefault(oldName, oldRoles.get(oldName)));
                mapping.put(oldName, newName);
            }

            BiFunction<Object, Class<?>, ?> converter = dataset.getDataStructure().converter();
            cache = DataStructure.of(converter, newTypes, newRoles);
        }

        return cache;
    }

    @Override
    public Stream<Tuple> get() {
        return dataset.get().map(components -> {

            components.replaceAll(component -> new DataPoint(getDataStructure().get(mapping.get(component.getName()))) {
                @Override
                public Object get() {
                    return component.get();
                }
            });
            return components;
        });
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        Integer limit = 5;
        Iterables.limit(Iterables.concat(
                mapping.entrySet(),
                Collections.singletonList("and " + (mapping.entrySet().size() - limit) + " more")
        ), Math.min(limit, mapping.entrySet().size())).forEach(
                helper::addValue
        );
        return helper.toString();
    }
}
