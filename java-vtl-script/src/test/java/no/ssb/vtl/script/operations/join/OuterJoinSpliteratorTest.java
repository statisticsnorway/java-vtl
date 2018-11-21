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

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class OuterJoinSpliteratorTest {

    private List<List<String>> right;
    private List<List<String>> left;
    private List<Map<String, List<String>>> expected;

    private static Map<String, List<String>> of(String k1, List<String> v1, String k2, List<String> v2) {
        HashMap<String, List<String>> map = new HashMap<>();
        map.put(k1, v1);
        map.put(k2, v2);
        return map;
    }

    @Before
    public void setUp() throws Exception {
        left = newArrayList(
                asList(null, "1", "null-1-left"),
                asList("A", "2", "A-2-left"),
                asList("A", "3", "A-3-left"),
                asList("B", "4", "B-4-left"),
                asList("C", "5", "C-5-left"),
                asList("D", "6", "D-6-left"),
                asList("D", "7", "D-7-left"),
                asList("E", "8", "E-8-left"),
                asList("E", "9", "E-9-left"),
                asList("F", "10", "F-10-left")
        );

        right = newArrayList(
                asList("D", "1", "D-1-right"),
                asList("E", "2", "E-2-right"),
                asList("E", "3", "E-3-right"),
                asList("F", "4", "F-4-right"),
                asList("F", "5", "F-5-right"),
                asList("G", "6", "G-6-right"),
                asList("H", "7", "H-7-right"),
                asList("I", "8", "I-8-right"),
                asList("I", "9", "I-9-right"),
                asList(null, "10", "null-10-right")
        );

        // @formatter:off
        expected = asList(
                of("left", asList(null, "1", "null-1-left"), "right", null),
                of("left", asList("A", "2", "A-2-left"), "right", null),
                of("left", asList("A", "3", "A-3-left"), "right", null),
                of("left", asList("B", "4", "B-4-left"), "right", null),
                of("left", asList("C", "5", "C-5-left"), "right", null),

                of("left", asList("D", "6", "D-6-left"), "right", asList("D", "1", "D-1-right")),
                of("left", asList("D", "7", "D-7-left"), "right", asList("D", "1", "D-1-right")),

                of("left", asList("E", "8", "E-8-left"), "right", asList("E", "2", "E-2-right")),
                of("left", asList("E", "8", "E-8-left"), "right", asList("E", "3", "E-3-right")),
                of("left", asList("E", "9", "E-9-left"), "right", asList("E", "2", "E-2-right")),
                of("left", asList("E", "9", "E-9-left"), "right", asList("E", "3", "E-3-right")),

                of("left", asList("F", "10", "F-10-left"), "right", asList("F", "4", "F-4-right")),
                of("left", asList("F", "10", "F-10-left"), "right", asList("F", "5", "F-5-right")),
                of("left", null, "right", asList("G", "6", "G-6-right")),
                of("left", null, "right", asList("H", "7", "H-7-right")),
                of("left", null, "right", asList("I", "8", "I-8-right")),
                of("left", null, "right", asList("I", "9", "I-9-right")),
                of("left", null, "right", asList(null, "10", "null-10-right"))
        );
        // @formatter:on
    }

    @Test
    public void testEstimateSize() {

        Spliterator<Map<String, List<String>>> spliterator = outerJoin(left.spliterator(), right.spliterator());

        assertThat(spliterator.estimateSize()).isEqualTo(left.size() + right.size());

    }

    @Test
    public void testTrySplitNotSupported() {
        Spliterator<Map<String, List<String>>> spliterator = outerJoin(left.spliterator(), right.spliterator());
        assertThat(spliterator.trySplit()).isNull();
    }

    @Test
    public void testTryAdvance() {

        ArrayDeque<List<String>> leftQueue = new ArrayDeque<>(left);
        ArrayDeque<List<String>> rightQueue = new ArrayDeque<>(right);

        OuterJoinSpliterator<List<String>, List<String>, String, Map<String, List<String>>> spliterator = outerJoin(
                Iterables.consumingIterable(leftQueue).spliterator(),
                Iterables.consumingIterable(rightQueue).spliterator()
        );

        List<Map<String, List<String>>> result = new ArrayList<>();
        while (spliterator.tryAdvance(result::add)) ;

        assertThat(result).containsExactlyElementsOf(expected);

        assertThat(leftQueue).isEmpty();
        assertThat(rightQueue).isEmpty();

    }

    private OuterJoinSpliterator<List<String>, List<String>, String, Map<String, List<String>>> outerJoin(Spliterator<List<String>> leftSpliterator, Spliterator<List<String>> rightSpliterator) {
        Comparator<String> comparator = Comparator.nullsFirst(Comparator.naturalOrder());
        return new OuterJoinSpliterator<>(
                list -> list.get(0),
                list -> list.get(0),
                comparator,
                (left, right) -> {
                    LinkedHashMap<String, List<String>> map = new LinkedHashMap<>();
                    map.put("left", left);
                    map.put("right", right);
                    return map;
                },
                leftSpliterator,
                rightSpliterator
        );
    }

    @Test
    public void testForEachRemaining() {

        ArrayDeque<List<String>> leftQueue = new ArrayDeque<>(left);
        ArrayDeque<List<String>> rightQueue = new ArrayDeque<>(right);

        OuterJoinSpliterator<List<String>, List<String>, String, Map<String, List<String>>> spliterator = outerJoin(
                Iterables.consumingIterable(leftQueue).spliterator(),
                Iterables.consumingIterable(rightQueue).spliterator()
        );

        List<Map<String, List<String>>> result = new ArrayList<>();
        spliterator.forEachRemaining(result::add);

        assertThat(result).containsExactlyElementsOf(expected);

        assertThat(leftQueue).isEmpty();
        assertThat(rightQueue).isEmpty();

    }

    @Test
    public void testImbrication() {
        List<List<String>> l1 = asList(asList("A", "1"), asList("B", "2"));
        List<List<String>> l2 = asList(asList("A", "4"), asList("C", "8"));
        List<List<String>> l3 = asList(asList("A", "16"), asList("B", "32"));

        Function<List<String>, String> getId = list -> list.get(0);
        OuterJoinSpliterator<List<String>, List<String>, String, List<String>> l1l2 =
                outerJoin(
                        l1.spliterator(),
                        l2.spliterator(),
                        getId,
                        (left, right) -> {
                            List<String> result = new ArrayList<>(3);
                            result.add(0, firstNonNull(left, right).get(0));
                            if (left != null) {
                                result.add(1, left.get(1));
                            } else {
                                result.add(1, null);
                            }
                            if (right != null) {
                                result.add(2, right.get(1));
                            } else {
                                result.add(2, null);
                            }
                            return result;
                        }
                );

        OuterJoinSpliterator<List<String>, List<String>, String, List<String>> l1l2l3 = outerJoin(
                l1l2,
                l3.spliterator(), getId, (left, right) -> {
                    List<String> result = new ArrayList<>(4);
                    result.add(0, firstNonNull(left, right).get(0));
                    if (left != null) {
                        result.add(1, left.get(1));
                        result.add(2, left.get(2));
                    } else {
                        result.add(1, null);
                        result.add(2, null);
                    }
                    if (right != null) {
                        result.add(3, right.get(1));
                    } else {
                        result.add(3, null);
                    }
                    return result;
                }
        );

        l1l2l3.forEachRemaining(System.out::println);

    }

    @Test
    public void testImbrication2() {
        List<List<String>> l1 = asList(asList("A", "1"), asList("B", "2"));
        List<List<String>> l2 = asList(asList("A", "4"), asList("C", "8"));
        List<List<String>> l3 = asList(asList("A", "16"), asList("B", "32"));

        Function<List<String>, String> getId = list -> list.get(0);
        OuterJoinSpliterator<List<String>, List<String>, String, List<String>> l1l2 =
                outerJoin(
                        l1.spliterator(),
                        l2.spliterator(),
                        getId,
                        (left, right) -> {
                            List<String> result = new ArrayList<>(3);
                            result.add(0, firstNonNull(left, right).get(0));
                            if (left != null) {
                                result.add(1, left.get(1));
                            } else {
                                result.add(1, null);
                            }
                            if (right != null) {
                                result.add(2, right.get(1));
                            } else {
                                result.add(2, null);
                            }
                            return result;
                        }
                );

        OuterJoinSpliterator<List<String>, List<String>, String, List<String>> l1l2l3 = outerJoin(
                l1l2,
                l3.spliterator(),
                getId,
                (l, r) -> {
                    List<String> result = new ArrayList<>(4);
                    result.add(0, firstNonNull(l, r).get(0));
                    if (l != null) {
                        result.add(1, l.get(1));
                        result.add(2, l.get(2));
                    } else {
                        result.add(1, null);
                        result.add(2, null);
                    }
                    if (r != null) {
                        result.add(3, r.get(1));
                    } else {
                        result.add(3, null);
                    }
                    return result;
                }
        );

        while (l1l2l3.tryAdvance(System.out::println)) ;

    }

    private OuterJoinSpliterator<List<String>, List<String>, String, List<String>> outerJoin(
            Spliterator<List<String>> left,
            Spliterator<List<String>> right,
            Function<List<String>, String> keyExtractor,
            BiFunction<List<String>, List<String>, List<String>> merger
    ) {
        return new OuterJoinSpliterator<>(
                keyExtractor,
                keyExtractor,
                Comparator.nullsFirst(Comparator.naturalOrder()),
                merger,
                left, right
        );
    }
}