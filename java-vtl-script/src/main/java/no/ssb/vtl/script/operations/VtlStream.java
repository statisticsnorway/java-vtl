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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.netflix.spectator.api.Clock;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.spectator.api.Spectator;
import com.netflix.spectator.api.Tag;
import com.netflix.spectator.api.Timer;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.Filtering;
import no.ssb.vtl.model.FilteringSpecification;
import no.ssb.vtl.model.Ordering;
import no.ssb.vtl.script.VtlConfiguration;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A wrapper around {@link Stream}<{@link DataPoint}> that includes its
 * relations with other streams and operations.
 */
public class VtlStream extends ForwardingStream<DataPoint> {

    // Used by printPlan.
    private static final String H_TEE = "─┬─";
    private static final String V_BAR = " │ ";
    private static final String V_TEE = " ├─";
    private static final String V_END = " └─";
    private static final String H_BAR = "───";
    private static final String SPACE = "   ";
    private final AbstractDatasetOperation operation;
    private final ImmutableList<Stream<DataPoint>> parents;
    private final Stream<DataPoint> delegate;
    private final Ordering requestedOrdering;
    private final Filtering requestedFiltering;
    private VtlStream child;
    private Ordering actualOrdering;
    private Filtering actualFiltering;
    private Statistics statistics;

    public VtlStream(
            AbstractDatasetOperation operation,
            Stream<DataPoint> delegate,
            Stream<DataPoint> parent,
            Ordering requestedOrdering,
            Filtering requestedFiltering,
            Ordering actualOrdering,
            Filtering actualFiltering
    ) {
        this(operation, delegate, Collections.singletonList(parent), requestedOrdering, requestedFiltering,
                actualOrdering, actualFiltering);
    }

    public VtlStream(
            AbstractDatasetOperation operation,
            Stream<DataPoint> delegate,
            Collection<Stream<DataPoint>> parents,
            Ordering requestedOrdering,
            Filtering requestedFiltering,
            Ordering actualOrdering,
            Filtering actualFiltering
    ) {

        this.parents = ImmutableList.copyOf(parents);

        for (Stream<DataPoint> parent : parents) {
            if (parent instanceof VtlStream) {
                ((VtlStream) parent).child = this;
            }
        }

        this.operation = operation;
        this.requestedOrdering = requestedOrdering;
        this.requestedFiltering = requestedFiltering;
        this.actualOrdering = actualOrdering;
        this.actualFiltering = actualFiltering;

        this.statistics = new Statistics(this, Spectator.globalRegistry());
        this.delegate = decorateStream(delegate);
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public VtlStream getChild() {
        return child;
    }

    public Ordering getOrdering() {
        return requestedOrdering;
    }

    public Ordering getActualOrdering() {
        return actualOrdering;
    }

    public Filtering getFiltering() {
        return requestedFiltering;
    }

    public AbstractDatasetOperation getOperation() {
        return operation;
    }

    public ImmutableList<Stream<DataPoint>> getParents() {
        return parents;
    }

    @Override
    public void close() {
        delegate.close();
        RuntimeException exception = null;
        for (Stream<DataPoint> parent : parents) {
            try {
                parent.close();
            } catch (RuntimeException ex) {
                if (exception != null) {
                    ex.addSuppressed(ex);
                } else {
                    exception = ex;
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    private Stream<DataPoint> measureStream(Stream<DataPoint> stream, Timer timer, Clock clock) {
        return StreamSupport.stream(() -> {
            Spliterator<DataPoint> spliterator = stream.spliterator();
            return new Spliterators.AbstractSpliterator<DataPoint>(spliterator.estimateSize(), spliterator.characteristics()) {

                @Override
                public boolean tryAdvance(Consumer<? super DataPoint> action) {
                    AtomicLong end = new AtomicLong(clock.monotonicTime());
                    AtomicLong start = new AtomicLong(clock.monotonicTime());
                    return spliterator.tryAdvance(dp -> {
                        end.set(clock.monotonicTime());
                        action.accept(dp);
                        timer.record(end.get() - start.get(), TimeUnit.NANOSECONDS);
                        start.set(clock.monotonicTime());
                    });
                }
            };
        }, 0, false).onClose(stream::close);
    }

    private Stream<DataPoint> measureStartStream(Stream<DataPoint> stream, Timer timer, Clock clock) {
        return StreamSupport.stream(() -> {
            long start = clock.monotonicTime();
            Spliterator<DataPoint> spliterator = stream.spliterator();
            long end = clock.monotonicTime();
            timer.record(end - start, TimeUnit.NANOSECONDS);
            return spliterator;
        }, 0, false).onClose(stream::close);
    }

    Stream<DataPoint> decorateStream(Stream<DataPoint> stream) {

        VtlConfiguration configuration = VtlConfiguration.getConfig();

        final Clock clock = statistics.registry.clock();
        if (configuration.isProfilingEnabled()) {
            stream = measureStream(stream, statistics.time, clock);
        }


        // Post filter
        if (requestedFiltering.getOperator() != FilteringSpecification.Operator.TRUE
                && !requestedFiltering.equals(actualFiltering)) {
            stream = stream.filter(requestedFiltering);
            if (configuration.isProfilingEnabled()) {
                stream = measureStream(
                        stream,
                        statistics.filterTime,
                        clock
                );
            }
        }

        // Post ordering
        if (configuration.isForceSortEnabled() || !requestedOrdering.equals(actualOrdering)) {
            stream = stream.sorted(requestedOrdering);
            if (configuration.isProfilingEnabled()) {
                stream = measureStartStream(
                        stream,
                        statistics.sortTime,
                        clock
                );
            }
        }

        // Order assertion
        if (configuration.isSortAssertionEnabled()) {
            AtomicReference<DataPoint> previous = new AtomicReference<>();
            stream = stream.peek(dataPoint -> {
                if (previous.get() != null) {
                    int compare;
                    try {
                        compare = requestedOrdering.compare(previous.get(), dataPoint);
                    } catch (Exception e) {
                        throw new RuntimeException(String.format("Order compare(%s,%s) failed (%s) (%s)",
                                previous.get(),
                                dataPoint, requestedOrdering,
                                operation
                        ), e);
                    }
                    if (compare > 0) {
                        throw new RuntimeException(String.format("Order assertion failed: %s was > than %s (%s)",
                                previous.get(),
                                dataPoint,
                                operation.getDataStructure()
                        ));
                    }
                }
                // Copy since the reference can be mutated.
                previous.set((DataPoint) dataPoint.clone());
            });
        }

        if (configuration.isProfilingEnabled()) {
            stream = stream.peek(dataPoint -> {
                statistics.rows.increment();
                statistics.cells.increment(dataPoint.size());
            });
        }
        return stream;
    }

    @Override
    protected Stream<DataPoint> delegate() {
        return delegate;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("operation", operation)
                .add("delegate", delegate)
                .toString();
    }

    /**
     * Prints the execution plan.
     */
    public String printPlan() {
        return printPlan("");
    }

    String printPlan(String prefix) {

        // TODO: Use StringBuilder or StringBuffer.
        String start = parents.isEmpty() ? H_BAR : H_TEE;

        String opType = String.format(" %s (stream %h)\n", operation.getClass().getSimpleName(), delegate.hashCode());
        String opString = String.format(" %s\n", operation.toString());
        String filter = String.format("  filter  : %s\n", requestedFiltering.toString());
        String aFilter = String.format("  - actual: %s\n", actualFiltering.toString());
        String order = String.format("  order   : %s\n", requestedOrdering.toString());
        String aOrder = String.format("  - actual: %s\n", actualOrdering.toString());

        String result = start + opType;
        result = result + prefix + V_BAR + opString;
        result = result + prefix + V_BAR + filter;
        result = result + prefix + V_BAR + aFilter;
        result = result + prefix + V_BAR + order;
        result = result + prefix + V_BAR + aOrder;

        ListIterator<Stream<DataPoint>> parentIterator = parents.listIterator();
        while (parentIterator.hasNext()) {
            Stream<DataPoint> parent = parentIterator.next();
            String newPrefix = parentIterator.hasNext() ? V_TEE : V_END;
            String newSpace = parentIterator.hasNext() ? V_BAR : SPACE;
            if (parent instanceof VtlStream) {
                result = result + prefix + newPrefix + ((VtlStream) parent).printPlan(prefix + newSpace);
            } else {
                // TODO: Wrap connector as well.
                result = result + prefix + newPrefix + " Connector\n";
            }
        }
        return result;
    }

    /**
     * Statistics used for profiling.
     * <p>
     * DataStructure creation time.
     * Time spent post filtering.
     * Time spent post ordering.
     * Cell count.
     * Row count.
     */
    public class Statistics {

        private final Registry registry;
        private final Counter rows;
        private final Counter cells;
        private final Timer time;
        private final Timer sortTime;
        private final Timer filterTime;

        private Statistics(VtlStream stream, Registry registry) {
            List<Tag> tags = Arrays.asList(
                    Tag.of("operation", stream.operation.getClass().getSimpleName()),
                    Tag.of("id", Integer.toString(stream.hashCode()))
            );

            rows = registry.counter("rows", tags);
            cells = registry.counter("cells", tags);
            time = registry.timer("time", tags);
            filterTime = registry.timer("filter", tags);
            sortTime = registry.timer("sort", tags);
            this.registry = registry;
        }

        public Timer getSortTime() {
            return sortTime;
        }

        public Timer getFilterTime() {
            return filterTime;
        }

        public Counter getRows() {
            return rows;
        }

        public Counter getCells() {
            return cells;
        }

        public Timer getTime() {
            return time;
        }
    }
}
