package no.ssb.vtl.script.operations.join;

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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VTLBoolean;
import no.ssb.vtl.model.VTLDate;
import no.ssb.vtl.model.VTLFloat;
import no.ssb.vtl.model.VTLInteger;
import no.ssb.vtl.model.VTLNumber;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.model.VTLString;
import no.ssb.vtl.model.VTLTyped;

import javax.script.SimpleBindings;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A bindings that transforms the components of a dataset as {@link no.ssb.vtl.model.VTLTyped}.
 */
public class ComponentBindings extends SimpleBindings {

    private static final Map<Class<?>, Class<? extends VTLObject>> javaToVTL;

    static {
        javaToVTL = ImmutableMap.<Class<?>, Class<? extends VTLObject>>builder()
                .put(String.class, VTLString.class)
                .put(Long.class, VTLInteger.class)
                .put(Double.class, VTLFloat.class)
                .put(Instant.class, VTLDate.class)
                .put(Boolean.class, VTLBoolean.class)
                .put(Number.class, VTLNumber.class)
                .build();
    }

    @Override
    public Object put(String name, Object value) {
        Object wrapped;
        if (value instanceof Component)
            wrapped = new ComponentReference((Component) value);
        else
            wrapped = value;
        return super.put(name, wrapped);
    }

    /**
     * Creates a Bindings that contains the unique components of this join operation and the
     * datasets.
     */
    public ComponentBindings(Map<String, Dataset> namedDatasets) {
        List<ComponentBindings> bindingsList = Lists.newArrayList();
        for (String datasetName : namedDatasets.keySet()) {
            ComponentBindings componentBindings = new ComponentBindings(namedDatasets.get(datasetName));
            bindingsList.add(componentBindings);
            this.put(datasetName, componentBindings);
        }

        // Figure out the component that are
        Set<String> unique = Sets.newHashSet();
        Set<String> ambiguous = Sets.newHashSet();
        for (ComponentBindings componentBindings : bindingsList) {
            Set<String> identifiers = componentBindings.keySet();
            for (String identifier : identifiers) {
                    if (ambiguous.contains(identifier))
                    continue;

                if (unique.contains(identifier)) {
                    unique.remove(identifier);
                    ambiguous.add(identifier);
                } else {
                    unique.add(identifier);
                }
            }
        }

        // Add all components that can be accessed without ambiguity.
        for (ComponentBindings componentBindings : bindingsList) {
            for (String identifier : componentBindings.keySet()) {
                if (unique.contains(identifier) && !this.containsKey(identifier)) {
                    this.put(identifier, componentBindings.get(identifier));
                }
            }
        }
    }

    public ComponentBindings(Dataset dataset) {
        checkNotNull(dataset);
        for (Entry<String, Component> entry : dataset.getDataStructure().entrySet()) {
            VTLTyped<?> type = new ComponentReference(entry.getValue());
            put(entry.getKey(), type);
        }
    }

    ComponentBindings() {
        // used by copyOf.
    }

    public static ComponentBindings copyOf(ComponentBindings componentBindings) {
        ComponentBindings binding = new ComponentBindings();
        for (Entry<String, Object> entry : componentBindings.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof ComponentBindings) {
                binding.put(entry.getKey(), copyOf((ComponentBindings) value));
            } else {
                binding.put(entry.getKey(), value);
            }
        }
        return binding;
    }

    public static class ComponentReference implements VTLTyped {
        private final Component component;
        private final Class<? extends VTLObject> type;

        public ComponentReference(Component component) {
            this.component = checkNotNull(component);

            Class<?> type = component.getType();
            checkArgument(javaToVTL.containsKey(type),
                    "unknown type %s", type);

            this.type = javaToVTL.get(type);

        }

        public Component getComponent() {
            return component;
        }

        @Override
        public Class<? extends VTLObject> getVTLType() {
            return this.type;
        }

        @Override
        public String toString() {
            return "ComponentReference{" +
                    "component=" + component +
                    ", type=" + type +
                    '}';
        }
    }
}
