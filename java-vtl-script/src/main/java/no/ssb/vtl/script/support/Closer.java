package no.ssb.vtl.script.support;

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Stolen from {@link com.google.common.io.Closer} to support {@link AutoCloseable}.
 */
public final class Closer implements AutoCloseable {

    static final Logger logger = Logger.getLogger(Closer.class.getName());

    /**
     * The suppressor implementation to use for the current Java version.
     */
    private static final Suppressor SUPPRESSOR =
            SuppressingSuppressor.isAvailable()
                    ? SuppressingSuppressor.INSTANCE
                    : LoggingSuppressor.INSTANCE;

    /**
     * Creates a new {@link Closer}.
     */
    public static Closer create() {
        return new Closer(SUPPRESSOR);
    }

    @VisibleForTesting
    final Suppressor suppressor;

    // only need space for 2 elements in most cases, so try to use the smallest array possible
    private final Deque<AutoCloseable> stack = new ArrayDeque<AutoCloseable>(4);
    private Throwable thrown;

    @VisibleForTesting
    Closer(Suppressor suppressor) {
        this.suppressor = checkNotNull(suppressor); // checkNotNull to satisfy null tests
    }

    /**
     * Registers the given {@code closeable} to be closed when this {@code Closer} is
     * {@linkplain #close closed}.
     *
     * @return the given {@code closeable}
     */
    // close. this word no longer has any meaning to me.
    public <C extends AutoCloseable> C register(C closeable) {
        if (closeable != null) {
            stack.addFirst(closeable);
        }

        return closeable;
    }

    /**
     * Closes all {@code Closeable} instances that have been added to this {@code Closer}. If an
     * exception was thrown in the try block and passed to one of the {@code exceptionThrown} methods,
     * any exceptions thrown when attempting to close a closeable will be suppressed. Otherwise, the
     * <i>first</i> exception to be thrown from an attempt to close a closeable will be thrown and any
     * additional exceptions that are thrown after that will be suppressed.
     */
    @Override
    public void close() throws IOException {
        Throwable throwable = thrown;

        // close closeables in LIFO order
        while (!stack.isEmpty()) {
            AutoCloseable closeable = stack.removeFirst();
            try {
                closeable.close();
            } catch (Throwable e) {
                if (throwable == null) {
                    throwable = e;
                } else {
                    suppressor.suppress(closeable, throwable, e);
                }
            }
        }

        if (thrown == null && throwable != null) {
            Throwables.propagateIfPossible(throwable, IOException.class);
            throw new AssertionError(throwable); // not possible
        }
    }

    /**
     * Suppression strategy interface.
     */
    @VisibleForTesting
    interface Suppressor {
        /**
         * Suppresses the given exception ({@code suppressed}) which was thrown when attempting to close
         * the given closeable. {@code thrown} is the exception that is actually being thrown from the
         * method. Implementations of this method should not throw under any circumstances.
         */
        void suppress(AutoCloseable closeable, Throwable thrown, Throwable suppressed);
    }

    /**
     * Suppresses exceptions by logging them.
     */
    @VisibleForTesting
    static final class LoggingSuppressor implements Suppressor {

        static final LoggingSuppressor INSTANCE = new LoggingSuppressor();

        @Override
        public void suppress(AutoCloseable closeable, Throwable thrown, Throwable suppressed) {
            // log to the same place as Closeables
            logger.log(
                    Level.WARNING, "Suppressing exception thrown when closing " + closeable, suppressed);
        }
    }

    /**
     * Suppresses exceptions by adding them to the exception that will be thrown using JDK7's
     * addSuppressed(Throwable) mechanism.
     */
    @VisibleForTesting
    static final class SuppressingSuppressor implements Suppressor {

        static final SuppressingSuppressor INSTANCE =  new SuppressingSuppressor();

        static boolean isAvailable() {
            return addSuppressed != null;
        }

        static final Method addSuppressed = getAddSuppressed();

        private static Method getAddSuppressed() {
            try {
                return Throwable.class.getMethod("addSuppressed", Throwable.class);
            } catch (Throwable e) {
                return null;
            }
        }

        @Override
        public void suppress(AutoCloseable closeable, Throwable thrown, Throwable suppressed) {
            // ensure no exceptions from addSuppressed
            if (thrown == suppressed) {
                return;
            }
            try {
                addSuppressed.invoke(thrown, suppressed);
            } catch (Throwable e) {
                // if, somehow, IllegalAccessException or another exception is thrown, fall back to logging
                LoggingSuppressor.INSTANCE.suppress(closeable, thrown, suppressed);
            }
        }
    }
}
