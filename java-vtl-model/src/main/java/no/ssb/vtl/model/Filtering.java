package no.ssb.vtl.model;

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

import com.google.common.base.MoreObjects;

import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;

import static no.ssb.vtl.model.FilteringSpecification.Operator.TRUE;

/**
 * Represent the filtering of the {@link DataPoint}s in a Dataset.
 */
public interface Filtering extends Predicate<DataPoint>, FilteringSpecification {
    Filtering ALL = new Filtering() {

        @Override
        public boolean test(DataPoint dataPoint) {
            return true;
        }

        @Override
        public Collection<Filtering> getOperands() {
            return Collections.emptyList();
        }

        @Override
        public Operator getOperator() {
            return TRUE;
        }

        @Override
        public String getColumn() {
            return null;
        }

        @Override
        public VTLObject getValue() {
            return null;
        }

        @Override
        public Boolean isNegated() {
            return false;
        }
    };

    @Override
    Collection<? extends Filtering> getOperands();
}
