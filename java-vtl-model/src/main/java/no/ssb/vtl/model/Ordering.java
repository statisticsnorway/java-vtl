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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * The comparator part of VTL order
 */
public interface Ordering extends Comparator<DataPoint>, OrderingSpecification {

    Ordering ANY = new Ordering() {
        @Override
        public int compare(DataPoint o1, DataPoint o2) {
            return 0;
        }

        @Override
        public List<String> columns() {
            return Collections.emptyList();
        }

        @Override
        public Direction getDirection(String column) {
            return Direction.ANY;
        }

        // Any ordering is equal to all ordering.
        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj instanceof Ordering) return true;
            return false;
        }
    };

    enum Direction {
        ASC, DESC, ANY
    }
}
