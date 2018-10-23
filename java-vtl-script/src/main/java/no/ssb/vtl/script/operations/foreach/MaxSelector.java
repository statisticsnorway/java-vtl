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

import com.google.common.base.MoreObjects;
import com.google.common.collect.PeekingIterator;

import java.util.Collection;
import java.util.Comparator;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Returns the maximum value out of PeekingIterators
 */
public class MaxSelector<T> implements Supplier<Optional<T>> {

    private final Comparator<T> comparator;
    private final Collection<PeekingIterator<T>> iterators;
    private T lastMax;

    MaxSelector(Collection<PeekingIterator<T>> iterators, Comparator<T> comparator) {
        this.iterators = checkNotNull(iterators);
        this.comparator = checkNotNull(comparator);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("lastMax", lastMax)
                .add("comparator", comparator)
                .toString();
    }

    @Override
    public Optional<T> get() {
        this.lastMax = null;
        for (PeekingIterator<T> iterator : iterators) {
            if (iterator.hasNext()) {
                T peek = iterator.peek();
                max(peek);
            }
        }
        return Optional.ofNullable(lastMax);
    }

    private void max(T peek) {
        if (lastMax == null) {
            lastMax = peek;
        } else if (comparator.compare(peek, lastMax) > 0) {
            lastMax = peek;
        }
    }
}
