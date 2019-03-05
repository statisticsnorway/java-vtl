package no.ssb.vtl.script.operations;

/*
 * -
 *   ========================LICENSE_START=================================
 *   Java VTL
 *   %%
 *   Copyright (C) 2019 Arild Johan Takvam-Borge
 *   %%
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *   =========================LICENSE_END==================================
 */

import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.model.VtlOrdering;

import java.util.Comparator;

public class DataPointMapComparator implements Comparator<DataPointMap> {

    private final VtlOrdering vtlOrdering;

    @SuppressWarnings("unchecked")
    private final static Comparator<VTLObject> vtlObjectComparator = Comparator.comparing((VTLObject object) ->
            (Comparable) object.get(), Comparator.nullsLast(Comparator.naturalOrder()));

    public DataPointMapComparator(VtlOrdering vtlOrdering) {
        this.vtlOrdering = vtlOrdering;
    }

    @Override
    public int compare(DataPointMap o1, DataPointMap o2) {
        int result = 0;
        for (String column : vtlOrdering.columns()) {
            result = vtlObjectComparator.compare(o1.get(column), o2.get(column));
            if (result != 0) {
                return vtlOrdering.getDirection(column) == Ordering.Direction.ASC ? result : -result;
            }
        }
        return result;
    }
}
