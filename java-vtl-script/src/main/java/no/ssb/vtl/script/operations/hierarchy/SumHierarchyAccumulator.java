package no.ssb.vtl.script.operations.hierarchy;

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

import com.google.common.collect.ImmutableMap;
import no.ssb.vtl.model.VTLNumber;
import no.ssb.vtl.model.VTLObject;

import java.util.Map;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.*;

/**
 * Sum accumulator.
 */
public class SumHierarchyAccumulator implements HierarchyAccumulator {

    private static Map<Class<?>, Object> ZEROS = ImmutableMap.<Class<?>, Object>builder()
            .put(Long.class, 0L)
            .put(Double.class, 0D)
            .put(Number.class, 0D)
            .build();

    private final Number identity;

    public SumHierarchyAccumulator(Class<?> type) {
        // TODO: Change when the type system supports more.
        identity = checkNotNull((Number) ZEROS.get(type));
    }

    @Override
    public VTLObject identity() {
        return VTLNumber.of(identity);
    }

    @Override
    public BiFunction<? super VTLObject, ? super VTLObject, ? extends VTLObject> accumulator(Composition sign) {
        switch (sign) {
            case UNION:
                return (left, right) -> {
                    VTLNumber leftNumber = (VTLNumber) left; // Always identity.
                    VTLNumber rightNumber =
                            right.get() == null ? VTLNumber.of(0) : (VTLNumber) right;
                    return leftNumber.add(rightNumber);
                };
            case COMPLEMENT:
                return (left, right) -> {
                    VTLNumber leftNumber = (VTLNumber) left; // Always identity.
                    VTLNumber rightNumber =
                            right.get() == null ? VTLNumber.of(0) : (VTLNumber) right;
                    return leftNumber.subtract(rightNumber.get());
                };
            default:
                throw new IllegalArgumentException(String.format("unknown sign %s", sign));
        }
    }
}
