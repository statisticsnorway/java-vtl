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

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableMap.of;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.util.Lists.newArrayList;

public class InnerJoinSpliteratorTest {

    private List<List<String>> right;
    private List<List<String>> left;
    private List<Map<String, List<String>>> expected;

    @Before
    public void setUp() throws Exception {
        left = newArrayList(
                asList("2016", null, "null-2016-left"),
                asList("2018", null, "null-2018-left"),

                asList("2016", "1", "1-2016-left"),
                asList("2017", "1", "1-2017-left"),
                asList("2018", "1", "1-2018-left"),

                asList("2016", "2", "2-2016-left"),
                asList("2017", "2", "2-2017-left"),

                asList("2018", "3", "3-2018-left"),

                asList("2015", "4", "4-2015-left"),
                asList(null, "4", "null-2016-left"),
                asList("2017", "4", "4-2017-left")
        );
        Collections.shuffle(left);

        right = newArrayList(
                asList("2017", null, "null-2017-right"),

                asList("2015", "1", "1-2015-right"),
                asList("2018", "1", "1-2018-right"),

                asList("2017", "2", "2-2017-right"),

                asList("2016", "3", "3-2016-right"),
                asList("2018", "3", "3-2018-right"),

                asList(null, "4", "null-2016-right"),
                asList("2015", "4", "4-2015-right"),

                asList("2016", "5", "5-2016-right"),
                asList("2017", "5", "5-2017-right")
        );
        Collections.shuffle(right);

        expected = newArrayList(
                of("left", asList("2016", null, "null-2016-left"), "right", asList("2017", null, "null-2017-right")),
                of("left", asList("2018", null, "null-2018-left"), "right", asList("2017", null, "null-2017-right")),

                of("left", asList("2016", "1", "1-2016-left"), "right", asList("2015", "1", "1-2015-right")),
                of("left", asList("2016", "1", "1-2016-left"), "right", asList("2018", "1", "1-2018-right")),

                of("left", asList("2017", "1", "1-2017-left"), "right", asList("2015", "1", "1-2015-right")),
                of("left", asList("2017", "1", "1-2017-left"), "right", asList("2018", "1", "1-2018-right")),

                of("left", asList("2018", "1", "1-2018-left"), "right", asList("2015", "1", "1-2015-right")),
                of("left", asList("2018", "1", "1-2018-left"), "right", asList("2018", "1", "1-2018-right")),

                of("left", asList("2016", "2", "2-2016-left"), "right", asList("2017", "2", "2-2017-right")),

                of("left", asList("2017", "2", "2-2017-left"), "right", asList("2017", "2", "2-2017-right")),

                of("left", asList("2018", "3", "3-2018-left"), "right", asList("2016", "3", "3-2016-right")),
                of("left", asList("2018", "3", "3-2018-left"), "right", asList("2018", "3", "3-2018-right")),

                of("left", asList("2015", "4", "4-2015-left"), "right", asList(null, "4", "null-2016-right")),
                of("left", asList("2015", "4", "4-2015-left"), "right", asList("2015", "4", "4-2015-right")),

                of("left", asList(null, "4", "null-2016-left"), "right", asList(null, "4", "null-2016-right")),
                of("left", asList(null, "4", "null-2016-left"), "right", asList("2015", "4", "4-2015-right")),

                of("left", asList("2017", "4", "4-2017-left"), "right", asList(null, "4", "null-2016-right")),
                of("left", asList("2017", "4", "4-2017-left"), "right", asList("2015", "4", "4-2015-right"))

        );
    }

    @Test
    public void testCharacteristics() {
        Comparator<List<String>> predicate = Comparator.comparing(
                list -> list.get(1),
                Comparator.nullsFirst(Comparator.naturalOrder())
        );

        InnerJoinSpliterator<List<String>, List<String>, List<String>, Map<String, List<String>>> innerJoinSpliterator = join(predicate);

        assertThat(innerJoinSpliterator.characteristics() & Spliterator.SORTED).isEqualTo(0);
        assertThat(innerJoinSpliterator.characteristics() & Spliterator.ORDERED).isEqualTo(Spliterator.ORDERED);
    }

    @Test
    public void testEstimateSize() {
        Comparator<List<String>> predicate = Comparator.comparing(
                list -> list.get(1),
                Comparator.nullsFirst(Comparator.naturalOrder())
        );

        InnerJoinSpliterator<List<String>, List<String>, List<String>, Map<String, List<String>>> innerJoinSpliterator = join(predicate);

        assertThat(innerJoinSpliterator.estimateSize()).isEqualTo(left.size() + right.size());
    }

    @Test
    public void testTrySplitNotSupported() {
        Comparator<List<String>> predicate = Comparator.comparing(
                list -> list.get(1),
                Comparator.nullsFirst(Comparator.naturalOrder())
        );

        InnerJoinSpliterator<List<String>, List<String>, List<String>, Map<String, List<String>>> innerJoinSpliterator = join(predicate);

        assertThat(innerJoinSpliterator.trySplit()).isNull();
    }

    @Test
    public void testForEachRemainingSubsetPredicate() {

        Comparator<List<String>> predicate = Comparator.comparing(
                list -> list.get(1),
                Comparator.nullsFirst(Comparator.naturalOrder())
        );

        InnerJoinSpliterator<List<String>, List<String>, List<String>, Map<String, List<String>>> innerJoinSpliterator = join(predicate);

        List<Map<String, List<String>>> result = newArrayList();
        innerJoinSpliterator.forEachRemaining(result::add);

        checkResult(result, predicate);
    }

    @Test
    public void testForEachRemaining() {

        Comparator<String> order = Comparator.nullsFirst(Comparator.naturalOrder());
        Comparator<List<String>> predicate = Comparator.<List<String>, String>comparing(
                list -> list.get(0),
                order
        ).thenComparing(
                list -> list.get(1),
                order
        );

        InnerJoinSpliterator<List<String>, List<String>, List<String>, Map<String, List<String>>> innerJoinSpliterator = join(predicate);

        List<Map<String, List<String>>> result = newArrayList();
        innerJoinSpliterator.forEachRemaining(result::add);

        checkResult(result,predicate);
    }

    @Test
    public void testTryAdvanceSubsetPredicate() {

        Comparator<String> order = Comparator.nullsFirst(Comparator.naturalOrder());
        Comparator<List<String>> predicate = Comparator.<List<String>, String>comparing(
                list -> list.get(0),
                order
        ).thenComparing(
                list -> list.get(1),
                order
        );

        InnerJoinSpliterator<List<String>, List<String>, List<String>, Map<String, List<String>>> innerJoinSpliterator = join(predicate);

        List<Map<String, List<String>>> result = checkTryAdvance(innerJoinSpliterator);

        checkResult(result, predicate);

    }

    private List<Map<String, List<String>>> checkTryAdvance(InnerJoinSpliterator<List<String>, List<String>, List<String>, Map<String, List<String>>> innerJoinSpliterator) {
        List<Map<String, List<String>>> result = newArrayList();

        for(Iterator<Map<String, List<String>>> it = Spliterators.iterator(innerJoinSpliterator); it.hasNext();)
            result.add(it.next());

        AtomicBoolean consumed = new AtomicBoolean(false);
        while (innerJoinSpliterator.tryAdvance(stringListMap -> {
            consumed.set(true);

        })) {
            assertThat(consumed.get()).isTrue();
            consumed.set(false);
        }
        return result;
    }

    @Test
    public void testTryAdvance() {

        Comparator<List<String>> predicate = Comparator.comparing(
                list -> list.get(1),
                Comparator.nullsFirst(Comparator.naturalOrder())
        );

        InnerJoinSpliterator<List<String>, List<String>, List<String>, Map<String, List<String>>> innerJoinSpliterator = join(predicate);

        List<Map<String, List<String>>> result = checkTryAdvance(innerJoinSpliterator);

        checkResult(result, predicate);

    }

    private InnerJoinSpliterator<List<String>, List<String>, List<String>, Map<String, List<String>>> join(Comparator<List<String>> predicate) {
        return new InnerJoinSpliterator<>(
                Function.identity(), Function.identity(), predicate,
                    (lefts, rights) -> of("left", lefts, "right", rights),
                    left.stream().sorted(predicate).spliterator(),
                    right.stream().sorted(predicate).spliterator()
            );
    }

    private void checkResult(List<Map<String, List<String>>> result, Comparator<List<String>> predicate) {
        assertThat(result).isNotEmpty();
        assertThat(result).containsOnlyElementsOf(
            expected.stream().filter(stringListMap -> predicate.compare(stringListMap.get("left"), stringListMap.get("right")) == 0 ).collect(Collectors.toList())
        );
        assertThat(result).extracting(map -> map.get("left")).isSortedAccordingTo(predicate);
    }
}
