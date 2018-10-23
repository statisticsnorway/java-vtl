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

import com.google.common.collect.Streams;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * A Dataset that returns its data based on an iterator.
 */
public class IteratorDataset implements Dataset {

    private final Iterator<DataPoint> source;
    private final DataStructure structure;

    public IteratorDataset(
            DataStructure structure,
            Iterator<DataPoint> source
    ) {
        this.source = source;
        this.structure = structure;
    }

    @Override
    public Stream<DataPoint> getData() {
        return Streams.stream(this.source);
    }

    @Override
    public Optional<Map<String, Integer>> getDistinctValuesCount() {
        return Optional.empty();
    }

    @Override
    public Optional<Long> getSize() {
        return Optional.empty();
    }

    @Override
    public DataStructure getDataStructure() {
        return structure;
    }
}
