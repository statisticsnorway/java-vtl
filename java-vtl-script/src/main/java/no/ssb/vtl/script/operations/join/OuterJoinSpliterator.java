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

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Queues;
import com.google.common.math.LongMath;

import java.util.Comparator;
import java.util.Deque;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class OuterJoinSpliterator<L, R, K, O> implements Spliterator<O> {

    private final Comparator<K> predicate;
    private final BiFunction<L, R, O> merger;

    private final Function<L, K> leftKeyExtractor;
    private final Function<R, K> rightKeyExtractor;
    private final PeekingIterator<L> leftIterator;
    private final PeekingIterator<R> rightIterator;
    private final Spliterator<L> leftSpliterator;
    private final Spliterator<R> rightSpliterator;

    private Deque<L> leftBuffer = Queues.newArrayDeque();
    private Deque<R> rightBuffer = Queues.newArrayDeque();

    private Deque<O> outputBuffer = Queues.newArrayDeque();

    public OuterJoinSpliterator(
            Function<L, K> leftKeyExtractor,
            Function<R, K> rightKeyExtractor,
            Comparator<K> predicate,
            BiFunction<L, R, O> merger,
            Spliterator<L> leftSpliterator,
            Spliterator<R> rightSpliterator
    ) {
        this.predicate = Comparator.nullsFirst(predicate);
        this.merger = merger;
        this.leftKeyExtractor = leftKeyExtractor;
        this.rightKeyExtractor = rightKeyExtractor;

        this.leftSpliterator = leftSpliterator;
        this.rightSpliterator = rightSpliterator;
        this.leftIterator = Iterators.peekingIterator(Spliterators.iterator(this.leftSpliterator));
        this.rightIterator = Iterators.peekingIterator(Spliterators.iterator(this.rightSpliterator));
    }

    private <I> K advance(PeekingIterator<I> source, Deque<I> buffer, Function<I, K> keyExtractor, Comparator<K> predicate) {
        buffer.clear();

        if (!source.hasNext())
            return null;

        buffer.addLast(source.next());
        K key = keyExtractor.apply(buffer.getFirst());
        while (source.hasNext() && predicate.compare(key, keyExtractor.apply(source.peek())) == 0) {
            buffer.addLast(source.next());
        }
        return key;
    }

    private K advanceRight() {
        while (!rightBuffer.isEmpty()) {
            outputBuffer.addLast(merger.apply(null, rightBuffer.removeFirst()));
        }
        return advance(rightIterator, rightBuffer, rightKeyExtractor, predicate);
    }

    private K advanceLeft() {
        while (!leftBuffer.isEmpty()) {
            outputBuffer.addLast(merger.apply(leftBuffer.removeFirst(), null));
        }
        return advance(leftIterator, leftBuffer, leftKeyExtractor, predicate);
    }

    @Override
    public void forEachRemaining(Consumer<? super O> action) {
        K leftKey = advanceLeft();
        K rightKey = advanceRight();

        while (!leftBuffer.isEmpty() || !rightBuffer.isEmpty()) {

            // TODO: Use always bigger placeholders?
            // empty source should always be bigger.
            if (leftBuffer.isEmpty()) {
                rightKey = advanceRight();
            } else if (rightBuffer.isEmpty()) {
                leftKey = advanceLeft();
            }

            int compare = predicate.compare(leftKey, rightKey);
            if (0 < compare || leftBuffer.isEmpty()) {
                // left > right (right is behind)
                rightKey = advanceRight();
            } else if (compare < 0 || rightBuffer.isEmpty()) {
                // left < right (left is behind)
                leftKey = advanceLeft();
            } else {
                // Add cartesian product to output.
                CartesianIterator<L, R, O> hit = new CartesianIterator<>(leftBuffer, rightBuffer, merger);
                hit.forEachRemaining(outputBuffer::addLast);
                leftBuffer.clear();
                rightBuffer.clear();

                outputBuffer.forEach(action);
                outputBuffer.clear();

                leftKey = advanceLeft();
                rightKey = advanceRight();
            }
        }
        outputBuffer.forEach(action);
    }

    @Override
    public boolean tryAdvance(Consumer<? super O> action) {
        if (!outputBuffer.isEmpty()) {
            action.accept(outputBuffer.removeFirst());
            return true;
        }

        K leftKey = advanceLeft();
        K rightKey = advanceRight();

        while (!leftBuffer.isEmpty() || !rightBuffer.isEmpty()) {

            // TODO: Use always bigger placeholders?
            // empty source should always be bigger.
            if (leftBuffer.isEmpty()) {
                rightKey = advanceRight();
            } else if (rightBuffer.isEmpty()) {
                leftKey = advanceLeft();
            }

            int compare = predicate.compare(leftKey, rightKey);
            if (0 < compare || leftBuffer.isEmpty()) {
                // left > right (right is behind)
                rightKey = advanceRight();
            } else if (compare < 0 || rightBuffer.isEmpty()) {
                // left < right (left is behind)
                leftKey = advanceLeft();
            } else {
                // output hit
                CartesianIterator<L, R, O> hit = new CartesianIterator<>(leftBuffer, rightBuffer, merger);
                hit.forEachRemaining(outputBuffer::addLast);
                leftBuffer.clear();
                rightBuffer.clear();

                return tryAdvance(action);
            }
        }

        if (!outputBuffer.isEmpty()) {
            return tryAdvance(action);
        }

        return false;
    }

    @Override
    public Spliterator<O> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        try {
            return LongMath.checkedAdd(leftSpliterator.estimateSize(), rightSpliterator.estimateSize());
        } catch (ArithmeticException ae) {
            return Long.MAX_VALUE;
        }
    }

    @Override
    public int characteristics() {
        return Spliterator.ORDERED | Spliterator.IMMUTABLE;
    }
}
