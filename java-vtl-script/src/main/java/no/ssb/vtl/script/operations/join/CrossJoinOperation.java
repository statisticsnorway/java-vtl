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

import com.google.common.collect.Streams;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.Order;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * TODO(hk): Finish this.
 */
public class CrossJoinOperation extends OuterJoinOperation {

    CrossJoinOperation(Map<String, Dataset> namedDatasets) {
        super(namedDatasets, Collections.emptySet());
    }

    public CrossJoinOperation(Map<String, Dataset> namedDatasets, Set<Component> identifiers) {
        super(namedDatasets, identifiers);
    }

    @Override
    protected Optional<Stream<DataPoint>> computeData(Order requestedOrder, Filtering filtering, Set<String> components) {
        Iterator<Dataset> iterator = getChildren().iterator();

        // Optimization
        if (getChildren().size() == 1)
            return Optional.of(order(requestedOrder, filtering, components, iterator.next()));

        Dataset left = iterator.next();
        Dataset right = left;

        // Create the resulting data points.
        final DataStructure joinStructure = getDataStructure();
        final DataStructure structure = left.getDataStructure();
        Stream<DataPoint> result = order(requestedOrder, filtering, components, left)
                .map(dataPoint -> joinStructure.fromMap(
                        structure.asMap(dataPoint)
                ));

        while (iterator.hasNext()) {
            left = right;
            right = iterator.next();
            result = Streams.zip(
                    result,
                    order(requestedOrder, filtering, components, right),
                    getMerger(left, right)
            );
        }

        return Optional.of(result);
    }

    private Stream<DataPoint> order(Order requestedOrder, Filtering filtering, Set<String> components, Dataset first) {
        return first.getData(requestedOrder, filtering, components).orElse(
                first.getData().sorted(requestedOrder).filter(filtering));
    }
}
