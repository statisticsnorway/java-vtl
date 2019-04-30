package no.ssb.vtl.script.operations.union;

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

import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.script.error.VTLRuntimeException;

import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class DuplicateChecker implements UnaryOperator<DataPoint> {

    private  final Ordering order;
    private final DataStructure structure;
    private DataPoint last;

    public DuplicateChecker(Ordering order, DataStructure structure) {
        this.order = checkNotNull(order);
        this.structure = checkNotNull(structure);
    }

    @Override
    public DataPoint apply(DataPoint dataPoint) {
        if (last != null && order.compare(last, dataPoint) == 0)
                throwDuplicateError(dataPoint);
        return last = dataPoint;
    }

    private void throwDuplicateError(DataPoint o) {
        //TODO: define an error code encoding. See VTL User Manuel "Constraints and errors"
        Map<Component, VTLObject> row = structure.asMap(o);
        String rowAsString = row.keySet().stream()
                .map(k -> k.getRole() + ":" + row.get(k))
                .collect(Collectors.joining("\n"));
        throw new VTLRuntimeException(String.format("The resulting dataset from a union contains duplicates. Duplicate row: %s", rowAsString), "VTL-1xxx", o);
    }
}
