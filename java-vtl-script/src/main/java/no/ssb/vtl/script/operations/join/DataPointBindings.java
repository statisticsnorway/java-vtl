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

import com.google.common.collect.Iterators;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.VTLObject;

import javax.script.Bindings;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static no.ssb.vtl.script.operations.join.ComponentBindings.*;

/**
 * A Bindings view that wraps a DataPoint object.
 *
 * TODO: The join operations should only operate on this.
 */
public class DataPointBindings implements Bindings {

    private final ComponentBindings references;
    private final DataStructure structure;
    private Map<Component, VTLObject> mapView;
    private DataPoint dataPoint;

    public DataPointBindings(ComponentBindings references, DataStructure structure) {
        this.references = checkNotNull(references);
        this.structure = checkNotNull(structure);
    }

    private Object extractComponent(Object object) {
        if (object instanceof ComponentReference) {
            return ((ComponentReference) object).getComponent();
        }
        if (object instanceof ComponentBindings) {
            DataPointBindings subBindings = new DataPointBindings((ComponentBindings) object, structure);
            return subBindings.setDataPoint(dataPoint);
        }
        return object;
    }

    public DataPoint getDataPoint() {
        return dataPoint;
    }

    public DataPointBindings setDataPoint(DataPoint dataPoint) {
        // TODO: Avoid instantiating an object.
        this.mapView = structure.asMap(dataPoint);
        this.dataPoint = dataPoint;
        return this;
    }

    @Override
    public Object put(String name, Object value) {
        return mapView.put((Component) extractComponent(references.get(name)), (VTLObject) value);
    }

    @Override
    public void putAll(Map<? extends String, ?> toMerge) {
        for (Entry<? extends String, ?> entry : toMerge.entrySet())
            put(entry.getKey(), entry.getValue());
    }

    @Override
    public void clear() {
        mapView.clear();
    }

    @Override
    public Set<String> keySet() {
        return references.keySet();
    }

    @Override
    public Collection<Object> values() {
        return (Collection) mapView.values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return new AbstractSet<Entry<String, Object>>() {
            @Override
            public Iterator<Entry<String, Object>> iterator() {
                Iterator<Entry<String, Object>> iterator = references.entrySet().iterator();
                return Iterators.transform(iterator, input -> {
                    Object value = extractComponent(input.getValue());
                    if (value instanceof Component)
                        value = mapView.get(value);
                    return new AbstractMap.SimpleImmutableEntry<>(
                            input.getKey(), value
                    );
                });
            }

            @Override
            public int size() {
                return mapView.size();
            }
        };
    }

    @Override
    public int size() {
        return references.size();
    }

    @Override
    public boolean isEmpty() {
        return references.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return references.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return references.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        Object reference = extractComponent(references.get(key));
        if (reference instanceof DataPointBindings)
            return reference;
        return mapView.get(reference);
    }

    @Override
    public Object remove(Object key) {
        return mapView.remove(extractComponent(references.get(key)));
    }
}
