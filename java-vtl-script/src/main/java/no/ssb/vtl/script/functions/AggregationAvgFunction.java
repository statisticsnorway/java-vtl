package no.ssb.vtl.script.functions;

/*
 * -
 *  * ========================LICENSE_START=================================
 * * Java VTL
 *  *
 * %%
 * Copyright (C) 2017 Arild Johan Takvam-Borge
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

import no.ssb.vtl.model.VTLFloat;
import no.ssb.vtl.model.VTLNumber;

import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.function.Function;

public class AggregationAvgFunction implements Function<List<VTLNumber>, VTLNumber> {

    @Override
    public VTLNumber apply(List<VTLNumber> vtlNumbers) {
        OptionalDouble average = vtlNumbers
                .stream()
                .filter(Objects::nonNull)
                .filter(n -> n.get() != null)
                .mapToDouble(n -> n.get().doubleValue())
                .average();

        if (average.isPresent()) {
            return VTLFloat.of(average.getAsDouble());
        } else {
            return VTLFloat.of((Double) null);
        }
    }
}

