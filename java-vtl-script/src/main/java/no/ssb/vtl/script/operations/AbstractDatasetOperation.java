package no.ssb.vtl.script.operations;

/*
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import com.google.common.collect.ImmutableList;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class for DatasetOperations.
 */
public abstract class AbstractDatasetOperation implements Dataset, AutoCloseable {

    private final ImmutableList<Dataset> children;
    private DataStructure cache;

    protected AbstractDatasetOperation(List<Dataset> children) {
        this.children = ImmutableList.copyOf(checkNotNull(children));
    }

    @Override
    public final DataStructure getDataStructure() {
        return cache = (cache == null ? computeDataStructure() : cache);
    }

    @Override
    public final Optional<Stream<DataPoint>> getData(Order orders, Filtering filtering, Set<String> components) {
        try {
            return computeData(orders, filtering, components)
                    .map(data -> data.onClose(this::closeUnsafe));
        } catch (RuntimeException re) {
            closeUnsafe(re);
            throw re;
        }
    }


    @Override
    public final Optional<Stream<DataPoint>> getData(Order order) {
        try {
            return computeData(
                    order,
                    Filtering.ALL,
                    getDataStructure().keySet()
            ).map(data -> data.onClose(this::closeUnsafe));
        } catch (RuntimeException re) {
            closeUnsafe(re);
            throw re;
        }
    }

    @Override
    public final Optional<Stream<DataPoint>> getData(Filtering filtering) {
        try {
            return computeData(
                    Order.createDefault(getDataStructure()),
                    Filtering.ALL,
                    getDataStructure().keySet()
            ).map(data -> data.onClose(this::closeUnsafe));
        } catch (RuntimeException re) {
            closeUnsafe(re);
            throw re;
        }
    }

    @Override
    public final Optional<Stream<DataPoint>> getData(Set<String> components) {
        try {
            return computeData(
                    Order.createDefault(getDataStructure()),
                    Filtering.ALL,
                    getDataStructure().keySet()
            ).map(data -> data.onClose(this::closeUnsafe));
        } catch (RuntimeException re) {
            closeUnsafe(re);
            throw re;
        }
    }

    @Override
    public void close() throws Exception {
        // TODO.
    }

    private void closeUnsafe() {
        try {
            this.close();
        } catch (Exception e) {
            // TODO: warn about this.
        }
    }

    private void closeUnsafe(RuntimeException re) {
        try {
            this.close();
        } catch (Exception e) {
            re.addSuppressed(e);
        }
    }


    protected abstract Optional<Stream<DataPoint>> computeData(Order orders, Filtering filtering, Set<String> components);

    protected final Stream<DataPoint> sortData(Order orders, Filtering filtering, Set<String> components) {
        return getData().sorted(orders).filter(filtering).map(o -> {
            // TODO
            return o;
        });
    }

    protected abstract DataStructure computeDataStructure();

    public final ImmutableList<Dataset> getChildren() {
        return children;
    }

}
