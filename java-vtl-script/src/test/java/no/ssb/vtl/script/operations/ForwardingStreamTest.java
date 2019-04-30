package no.ssb.vtl.script.operations;

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

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class ForwardingStreamTest {

    @Test
    public void filter() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.filter(integer -> integer > 2)).containsExactly(3);
    }

    @Test
    public void map() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.map(integer -> integer * 2)).containsExactly(2, 4, 6);
    }

    @Test
    public void mapToInt() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.mapToInt(integer -> integer).sum()).isEqualTo(6);
    }

    @Test
    public void mapToLong() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.mapToLong(integer -> integer).sum()).isEqualTo(6);
    }

    @Test
    public void mapToDouble() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.mapToDouble(integer -> integer * 0.5).sum()).isEqualTo(3);
    }

    @Test
    public void flatMap() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.flatMap(integer -> Collections.nCopies(integer, integer).stream())).containsExactly(
                1, 2, 2, 3, 3, 3
        );
    }

    @Test
    public void flatMapToInt() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.flatMapToInt(
                integer -> Collections.nCopies(integer, integer).stream().mapToInt(value -> value)).sum()).isEqualTo(
                14
        );
    }

    @Test
    public void flatMapToLong() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.flatMapToLong(
                integer -> Collections.nCopies(integer, integer).stream().mapToLong(value -> value)).sum()).isEqualTo(
                14
        );
    }

    @Test
    public void flatMapToDouble() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.flatMapToDouble(
                integer -> Collections.nCopies(integer, integer).stream().mapToDouble(value -> value * 0.5)).sum()).isEqualTo(
                7
        );
    }

    @Test
    public void distinct() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 1, 2, 3);
        assertThat(forwardedStream.distinct()).containsExactly(
                1, 2, 3
        );
    }

    private ForwardingStream<Integer> forwardedStreamOf(Integer... integers) {
        Stream<Integer> stream = Stream.of(integers);
        return new ForwardingStream<Integer>() {

            @Override
            protected Stream<Integer> delegate() {
                return stream;
            }
        };
    }

    @Test
    public void sorted() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(3, 2, 1);
        assertThat(forwardedStream.sorted()).containsExactly(
                1, 2, 3
        );
    }

    @Test
    public void peek() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        AtomicInteger integer = new AtomicInteger(0);
        Stream<Integer> peekedStream = forwardedStream.peek(integer::getAndAdd);
        assertThat(integer.get()).isEqualTo(0);
        assertThat(peekedStream).containsExactly(1, 2, 3);
        assertThat(integer.get()).isEqualTo(6);
    }

    @Test
    public void limit() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.limit(2)).containsExactly(1, 2);
    }

    @Test
    public void skip() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.skip(2)).containsExactly(3);
    }

    @Test
    public void forEach() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        AtomicInteger integer = new AtomicInteger(0);
        forwardedStream.forEach(integer::getAndAdd);
        assertThat(integer.get()).isEqualTo(6);
    }

    @Test
    public void forEachOrdered() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        AtomicInteger integer = new AtomicInteger(0);
        forwardedStream.forEachOrdered(integer::getAndAdd);
        assertThat(integer.get()).isEqualTo(6);
    }

    @Test
    public void toArray() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.toArray()).isEqualTo(new Object[]{
                1, 2, 3
        });
    }

    @Test
    public void toArray1() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        Integer[] dest = new Integer[3];
        assertThat(forwardedStream.toArray(value -> dest)).isEqualTo(new Object[]{
                1, 2, 3
        });
        assertThat(dest).isEqualTo(new Object[]{
                1, 2, 3
        });
    }

    @Test
    public void reduce() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.reduce(-1, (a, b) -> a + b)).isEqualTo(5);
    }

    @Test
    public void reduce1() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.reduce((a, b) -> a + b)).contains(6);
    }

    @Test
    public void reduce2() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.reduce(-1, (a, b) -> a * b, (a, b) -> a + b)).isEqualTo(-6);

    }

    @Test
    public void collect() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.collect(Collectors.toList())).containsExactly(
                1, 2, 3
        );
    }

    @Test
    public void collect1() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.collect(
                (Supplier<ArrayList<Object>>) ArrayList::new,
                ArrayList::add,
                ArrayList::addAll))
                .containsExactly(
                        1, 2, 3
                );
    }

    @Test
    public void min() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.min(Integer::compareTo)).contains(1);
    }

    @Test
    public void max() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.max(Integer::compareTo)).contains(3);
    }

    @Test
    public void count() {
        ForwardingStream<Integer> forwardedStream = forwardedStreamOf(1, 2, 3);
        assertThat(forwardedStream.count()).isEqualTo(3);
    }

    @Test
    public void anyMatch() {
        assertThat(forwardedStreamOf(1, 2, 3).anyMatch(integer -> integer.equals(1))).isTrue();
        assertThat(forwardedStreamOf(1, 2, 3).anyMatch(integer -> integer.equals(-1))).isFalse();
    }

    @Test
    public void allMatch() {
        assertThat(forwardedStreamOf(1, 2, 3).allMatch(integer -> integer > 0)).isTrue();
        assertThat(forwardedStreamOf(1, 2, 3).allMatch(integer -> integer < 0)).isFalse();
    }

    @Test
    public void noneMatch() {
        assertThat(forwardedStreamOf(1, 2, 3).noneMatch(integer -> integer == 0)).isTrue();
        assertThat(forwardedStreamOf(1, 2, 3).noneMatch(integer -> integer != 0)).isFalse();
    }

    @Test
    public void findFirst() {
        ForwardingStream<Integer> forwardedStream = new ForwardingStream<Integer>() {

            @Override
            protected Stream<Integer> delegate() {
                return Stream.of(1, 2, 3);
            }
        };
        assertThat(forwardedStream.findFirst()).contains(1);
    }

    @Test
    public void findAny() {
        ForwardingStream<Integer> forwardedStream = new ForwardingStream<Integer>() {

            @Override
            protected Stream<Integer> delegate() {
                return Stream.of(1, 2, 3);
            }
        };
        assertThat(forwardedStream.findAny()).isPresent();
    }

    @Test
    public void iterator() {
        ForwardingStream<Integer> forwardedStream = new ForwardingStream<Integer>() {

            @Override
            protected Stream<Integer> delegate() {
                return Stream.of(1, 2, 3);
            }
        };
        assertThat(forwardedStream.iterator()).containsExactly(
                1, 2, 3
        );
    }

    @Test
    public void spliterator() {
        ForwardingStream<Integer> forwardedStream = new ForwardingStream<Integer>() {

            @Override
            protected Stream<Integer> delegate() {
                return Stream.of(1, 2, 3);
            }
        };
        AtomicInteger integer = new AtomicInteger(0);
        forwardedStream.spliterator().forEachRemaining(integer::getAndAdd);
        assertThat(integer.get()).isEqualTo(6);
    }

    @Test
    public void isParallel() {
        ForwardingStream<Integer> forwardedStream = new ForwardingStream<Integer>() {

            @Override
            protected Stream<Integer> delegate() {
                return Stream.of(1, 2, 3).parallel();
            }
        };
        assertThat(forwardedStream.isParallel()).isTrue();
    }

    @Test
    public void sequential() {
        ForwardingStream<Integer> forwardedStream = new ForwardingStream<Integer>() {

            @Override
            protected Stream<Integer> delegate() {
                return Stream.of(1, 2, 3).parallel();
            }
        };
        assertThat(forwardedStream.sequential().isParallel()).isFalse();
    }

    @Test
    public void parallel() {
        ForwardingStream<Integer> forwardedStream = new ForwardingStream<Integer>() {

            @Override
            protected Stream<Integer> delegate() {
                return Stream.of(1, 2, 3).sequential();
            }
        };
        assertThat(forwardedStream.parallel().isParallel()).isTrue();
    }

    @Test
    public void unordered() {
        ForwardingStream<Integer> forwardedStream = new ForwardingStream<Integer>() {

            @Override
            protected Stream<Integer> delegate() {
                return Stream.of(1, 2, 3).sequential();
            }
        };
        assertThat(forwardedStream.unordered()).containsExactlyInAnyOrder(
                1, 2, 3
        );
    }

    @Test
    public void onClose() {
        ForwardingStream<Integer> forwardedStream = new ForwardingStream<Integer>() {

            @Override
            protected Stream<Integer> delegate() {
                return Stream.of(1, 2, 3).sequential();
            }
        };
        AtomicBoolean closed = new AtomicBoolean(false);
        Stream<Integer> onClose = forwardedStream.onClose(() -> closed.set(true));
        assertThat(closed.get()).isFalse();
        onClose.close();
        assertThat(closed.get()).isTrue();
    }

}
