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

import com.google.common.collect.Lists;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Dataset;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A bindings that transforms common identifier components across datasets as {@link no.ssb.vtl.model.VTLTyped}.
 */
public class CommonIdentifierBindings extends ComponentBindings {

    public CommonIdentifierBindings(Map<String, Dataset> namedDatasets) {
        List<ComponentBindings> bindingsList = Lists.newArrayList();
        for (Map.Entry<String, Dataset> entry : namedDatasets.entrySet()) {
            ComponentBindings componentBindings = new ComponentBindings(entry.getValue());
            bindingsList.add(componentBindings);
        }
        Set<String> commonIdentifiers = null;
        for (ComponentBindings componentBindings : bindingsList) {
            final Set<String> identifiers = componentBindings.entrySet().stream()
                    .filter(entry -> ((ComponentReference) entry.getValue()).getComponent().isIdentifier())
                    .map(Entry::getKey).collect(Collectors.toSet());
            if (commonIdentifiers == null) {
                commonIdentifiers = identifiers;
            } else {
                commonIdentifiers = commonIdentifiers.stream()
                        .filter(key -> identifiers.contains(key)).collect(Collectors.toSet());
            }
        }
        // Add all components that can be accessed via common identifiers.
        for (ComponentBindings componentBindings : bindingsList) {
            for (String identifier : componentBindings.keySet()) {
                if (commonIdentifiers.contains(identifier) && !this.containsKey(identifier)) {
                    this.put(identifier, componentBindings.get(identifier));
                }
            }
        }
    }

    public Map<String, Component> getComponentReferences() {
        return this.entrySet().stream().filter(e -> e.getValue() instanceof ComponentReference)
                .collect(Collectors.toMap(Map.Entry::getKey, e -> ((ComponentReference) e.getValue()).getComponent()));
    }

}
