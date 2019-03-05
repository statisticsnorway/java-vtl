package no.ssb.vtl.script.operations.foreach;

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

import com.google.common.collect.ForwardingMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.UnmodifiableListIterator;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.VTLObject;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Use {@link no.ssb.vtl.script.operations.DataPointMap}
 */
@Deprecated
public final class DataPointMap {

    private final ImmutableList<String> names;

    class View extends ForwardingMap<String, VTLObject> {

        private final DataPoint point;
        private final Map<String, VTLObject> delegate;

        protected View(DataPoint point) {
            this.point = point;
            ImmutableMap.Builder<String, Supplier<VTLObject>> builder = ImmutableMap.builder();
            UnmodifiableListIterator<String> it = names.listIterator();
            while (it.hasNext()) {
                int index = it.nextIndex();
                String name = it.next();
                builder.put(name, () -> this.point.get(index));
            }
            this.delegate = Maps.transformValues(builder.build(), Supplier::get);
        }

        DataPoint unwrap() {
            return point;
        }

        @Override
        protected Map<String, VTLObject> delegate() {
            return this.delegate;
        }
    }

    public View wrap(DataPoint dataPoint) {
        return new View(dataPoint);
    }

    DataPointMap(DataStructure structure) {
        names = ImmutableSet.copyOf(structure.keySet()).asList();
    }
}
