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

import com.google.common.collect.ForwardingObject;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public abstract class ForwardingStream<T> extends ForwardingObject implements Stream<T> {

    @Override
    public Stream<T> filter(Predicate<? super T> predicate) {
        return delegate().filter(predicate);
    }

    @Override
    public <R> Stream<R> map(Function<? super T, ? extends R> mapper) {
        return delegate().map(mapper);
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super T> mapper) {
        return delegate().mapToInt(mapper);
    }

    @Override
    public LongStream mapToLong(ToLongFunction<? super T> mapper) {
        return delegate().mapToLong(mapper);
    }

    @Override
    public DoubleStream mapToDouble(ToDoubleFunction<? super T> mapper) {
        return delegate().mapToDouble(mapper);
    }

    @Override
    public <R> Stream<R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return delegate().flatMap(mapper);
    }

    @Override
    public IntStream flatMapToInt(Function<? super T, ? extends IntStream> mapper) {
        return delegate().flatMapToInt(mapper);
    }

    @Override
    public LongStream flatMapToLong(Function<? super T, ? extends LongStream> mapper) {
        return delegate().flatMapToLong(mapper);
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super T, ? extends DoubleStream> mapper) {
        return delegate().flatMapToDouble(mapper);
    }

    @Override
    public Stream<T> distinct() {
        return delegate().distinct();
    }

    @Override
    public Stream<T> sorted() {
        return delegate().sorted();
    }

    @Override
    public Stream<T> sorted(Comparator<? super T> comparator) {
        return delegate().sorted(comparator);
    }

    @Override
    public Stream<T> peek(Consumer<? super T> action) {
        return delegate().peek(action);
    }

    @Override
    public Stream<T> limit(long maxSize) {
        return delegate().limit(maxSize);
    }

    @Override
    public Stream<T> skip(long n) {
        return delegate().skip(n);
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        delegate().forEach(action);
    }

    @Override
    public void forEachOrdered(Consumer<? super T> action) {
        delegate().forEachOrdered(action);
    }

    @Override
    public Object[] toArray() {
        return delegate().toArray();
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> generator) {
        return delegate().toArray(generator);
    }

    @Override
    public T reduce(T identity, BinaryOperator<T> accumulator) {
        return delegate().reduce(identity, accumulator);
    }

    @Override
    public Optional<T> reduce(BinaryOperator<T> accumulator) {
        return delegate().reduce(accumulator);
    }

    @Override
    public <U> U reduce(U identity, BiFunction<U, ? super T, U> accumulator, BinaryOperator<U> combiner) {
        return delegate().reduce(identity, accumulator, combiner);
    }

    @Override
    public <R> R collect(Supplier<R> supplier, BiConsumer<R, ? super T> accumulator, BiConsumer<R, R> combiner) {
        return delegate().collect(supplier, accumulator, combiner);
    }

    @Override
    public <R, A> R collect(Collector<? super T, A, R> collector) {
        return delegate().collect(collector);
    }

    @Override
    public Optional<T> min(Comparator<? super T> comparator) {
        return delegate().min(comparator);
    }

    @Override
    public Optional<T> max(Comparator<? super T> comparator) {
        return delegate().max(comparator);
    }

    @Override
    public long count() {
        return delegate().count();
    }

    @Override
    public boolean anyMatch(Predicate<? super T> predicate) {
        return delegate().anyMatch(predicate);
    }

    @Override
    public boolean allMatch(Predicate<? super T> predicate) {
        return delegate().allMatch(predicate);
    }

    @Override
    public boolean noneMatch(Predicate<? super T> predicate) {
        return delegate().noneMatch(predicate);
    }

    @Override
    public Optional<T> findFirst() {
        return delegate().findFirst();
    }

    @Override
    public Optional<T> findAny() {
        return delegate().findAny();
    }

    @Override
    public Iterator<T> iterator() {
        return delegate().iterator();
    }

    @Override
    public Spliterator<T> spliterator() {
        return delegate().spliterator();
    }

    @Override
    public boolean isParallel() {
        return delegate().isParallel();
    }

    @Override
    public Stream<T> sequential() {
        return delegate().sequential();
    }

    @Override
    public Stream<T> parallel() {
        return delegate().parallel();
    }

    @Override
    public Stream<T> unordered() {
        return delegate().unordered();
    }

    @Override
    public Stream<T> onClose(Runnable closeHandler) {
        return delegate().onClose(closeHandler);
    }

    @Override
    public void close() {
        delegate().close();
    }

    @Override
    protected abstract Stream<T> delegate();


}
