package no.ssb.vtl.script.support;

/*
 * -
 *  * ========================LICENSE_START=================================
 * * Java VTL
 *  *
 * %%
 * Copyright (C) 2017 Hadrien Kohl
 *  *
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
 *
 */

import com.google.common.collect.ForwardingObject;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.Ordering;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public abstract class DatasetCloseWatcher extends ForwardingObject implements Dataset {

    private int counter = 0;

    private DatasetCloseWatcher() {
        // disable
    }

    protected abstract Dataset delegate();

    public boolean allStreamWereClosed() {
        return counter == 0;
    }

    public static DatasetCloseWatcher wrap(Dataset dataset) {
        return new DatasetCloseWatcher() {

            @Override
            protected Dataset delegate() {
                return dataset;
            }
        };
    }

    @Override
    public Stream<DataPoint> getData() {
        return wrap(delegate().getData());
    }

    private void increase() {
        this.counter++;
    }

    private void decrease() {
        this.counter--;
    }

    private Stream<DataPoint> wrap(Stream<DataPoint> stream) {
        this.increase();
        return stream.onClose(this::decrease);
    }

    @Override
    public Optional<Stream<DataPoint>> getData(Ordering orders, Filtering filtering, Set<String> components) {
        return delegate().getData(orders, filtering, components).map(this::wrap);
    }

    @Override
    public Optional<Stream<DataPoint>> getData(Ordering order) {
        return delegate().getData(order).map(this::wrap);
    }

    @Override
    public Optional<Stream<DataPoint>> getData(Filtering filtering) {
        return delegate().getData(filtering).map(this::wrap);
    }

    @Override
    public Optional<Stream<DataPoint>> getData(Set<String> components) {
        return delegate().getData(components).map(this::wrap);
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return delegate().getDistinctValuesCount();
    }

    @Override
    public Optional<Long> getSize() {
        return delegate().getSize();
    }

    @Override
    public DataStructure getDataStructure() {
        return delegate().getDataStructure();
    }
}
