package no.ssb.vtl.script.operations.foreach;

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

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class MaxSelectorTest {

    @Test
    public void testToString() {

        MaxSelector<String> selector = new MaxSelector<>(
                Arrays.asList(
                        Iterators.peekingIterator(Iterators.forArray("a")),
                        Iterators.peekingIterator(Iterators.forArray("b")),
                        Iterators.peekingIterator(Iterators.forArray("c"))
                ),
                Comparator.naturalOrder()
        );
        assertThat(selector.toString()).isEqualTo("MaxSelector{lastMax=null, comparator=INSTANCE}");
        selector.get();
        assertThat(selector.toString()).isEqualTo("MaxSelector{lastMax=c, comparator=INSTANCE}");

    }

    @Test
    public void testSelector() {

        List<String> strings1 = new ArrayList<>();
        List<String> strings2 = new ArrayList<>();
        List<String> strings3 = new ArrayList<>();
        Lists.cartesianProduct(
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "b", "c"),
                Arrays.asList("a", "b", "c")
        ).forEach(strings -> {
            strings1.add(strings.get(0));
            strings2.add(strings.get(1));
            strings3.add(strings.get(2));
        });

        PeekingIterator<String> it1 = Iterators.peekingIterator(strings1.iterator());
        PeekingIterator<String> it2 = Iterators.peekingIterator(strings2.iterator());
        PeekingIterator<String> it3 = Iterators.peekingIterator(strings3.iterator());
        MaxSelector<String> selector = new MaxSelector<>(
                Arrays.asList(it1, it2, it3),
                Comparator.naturalOrder()
        );

        while (it1.hasNext() || it2.hasNext() || it3.hasNext()) {
            String max = Collections.max(Arrays.asList(it1.peek(), it2.peek(), it3.peek()));
            assertThat(selector.get()).contains(max);
            if (it1.hasNext()) {
                it1.next();
            }
            if (it2.hasNext()) {
                it2.next();
            }
            if (it3.hasNext()) {
                it3.next();
            }
        }
    }
}
