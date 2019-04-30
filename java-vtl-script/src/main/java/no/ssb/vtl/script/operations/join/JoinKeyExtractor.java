package no.ssb.vtl.script.operations.join;

/*-
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2018 Hadrien Kohl
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Ordering;

import java.util.ArrayList;
import java.util.function.UnaryOperator;

/**
 * A key extractor used in join operations.
 */
public class JoinKeyExtractor implements UnaryOperator<DataPoint> {

    private final DataPoint buffer;
    private final int[] indices;

    public JoinKeyExtractor(
            DataStructure childStructure,
            Ordering order
    ) {

        ImmutableList<String> fromList = ImmutableList.copyOf(childStructure.keySet());
        ImmutableList<String> toList = ImmutableList.copyOf(order.columns());

        ArrayList<Integer> indices = Lists.newArrayList();

        // For each component in order, find the child index.
        for (String column : order.columns()) {
            indices.add(
                    toList.indexOf(column),
                    fromList.indexOf(column)
            );
        }

        this.indices = Ints.toArray(indices);
        this.buffer = DataPoint.create(toList.size());
    }

    @Override
    public DataPoint apply(DataPoint dataPoint) {
        for (int i = 0; i < indices.length; i++) {
            buffer.set(i, dataPoint.get(indices[i]));
        }
        return (DataPoint) buffer.clone();
    }
}
