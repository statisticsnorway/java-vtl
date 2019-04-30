package no.ssb.vtl.script;

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

/**
 * Configuration object for vtl.
 * <p>
 * The configuration is wrapped inside a ThreadLocal for now. Use {@link #getConfig()} to
 * access the instances.
 */
public final class VtlConfiguration {

    private static final ThreadLocal<VtlConfiguration> localConfiguration = new ThreadLocal<>();

    private boolean filterOptimization = true;
    private boolean filterPropagation = true;
    private boolean profiling = false;
    private boolean sortAssertion = false;
    private boolean forceSort = false;

    private VtlConfiguration() {
        // prevent instantiation.
    }

    public static VtlConfiguration getConfig() {
        VtlConfiguration configuration = localConfiguration.get();
        if (configuration == null) {
            localConfiguration.set(new VtlConfiguration());
        }
        return localConfiguration.get();
    }

    static void setConfig(VtlConfiguration config) {
        localConfiguration.set(config);
    }

    /**
     * Some operations add filters down the execution tree in order to optimize the execution.
     * {@link no.ssb.vtl.script.operations.unfold.UnfoldOperation} adds a filter on the values that are unfolded.
     * {@link no.ssb.vtl.script.operations.join.InnerJoinOperation} add a range filter based on the first and last
     * identifiers of the datasets it joins.
     */
    public void setFilterOptimization(boolean filterOptimization) {
        this.filterOptimization = filterOptimization;
    }

    /**
     * @see #setFilterOptimization(boolean)
     */
    public void enableFilterOptimization() {
        setFilterOptimization(true);
    }

    /**
     * @see #setFilterOptimization(boolean)
     */
    public void disableFilterOptimization() {
        setFilterOptimization(false);
    }

    /**
     * When filter propagation is enabled, dataset operations will try to
     * push back part of the filters they received back to the operations
     * behind them.
     */
    public void setFilterPropagation(boolean filterPropagation) {
        this.filterPropagation = filterPropagation;
    }

    /**
     * @see #setFilterPropagation(boolean)
     */
    public void enableFilterPropagation() {
        setFilterPropagation(true);
    }

    /**
     * @see #setFilterPropagation(boolean)
     */
    public void disableFilterPropagation() {
        setFilterPropagation(false);
    }

    /**
     * Profiling enables timing and counters. Note that profiling will
     * reduce performances.
     *
     * @see no.ssb.vtl.script.operations.VtlStream.Statistics
     */
    public void setProfiling(boolean profiling) {
        this.profiling = profiling;
    }

    /**
     * @see #setProfiling(boolean)
     */
    public void enableProfiling() {
        setProfiling(true);
    }

    /**
     * @see #setProfiling(boolean)
     */
    public void disableProfiling() {
        setProfiling(false);
    }

    /**
     * Adds a verification step after each operation that checks that the order of the data it sees
     * is consistent with the required order. Note that this will reduce performances.
     */
    public void setSortAssertion(boolean sortAssertion) {
        this.sortAssertion = sortAssertion;
    }

    /**
     * @see #setSortAssertion(boolean)
     */
    public void enableSortAssertion() {
        setSortAssertion(true);
    }

    /**
     * @see #setSortAssertion(boolean)
     */
    public void disableSortAssertion() {
        setSortAssertion(false);
    }

    /**
     * Force post sort after each operation. Note that this will reduce performances.
     */
    public void setForceSort(boolean forceSort) {
        this.forceSort = forceSort;
    }

    /**
     * @see #setForceSort(boolean)
     */
    public void enableForceSort() {
        setForceSort(true);
    }

    /**
     * @see #setForceSort(boolean)
     */
    public void disableForceSort() {
        setForceSort(false);
    }

    /**
     * @see #setFilterOptimization(boolean)
     */
    public boolean isFilterOptimizationEnabled() {
        return this.filterOptimization;
    }

    /**
     * @see #setFilterPropagation(boolean)
     */
    public boolean isFilterPropagationEnabled() {
        return this.filterPropagation;
    }

    /**
     * @see #setProfiling(boolean)
     */
    public boolean isProfilingEnabled() {
        return profiling;
    }

    /**
     * @see #setSortAssertion(boolean)
     */
    public boolean isSortAssertionEnabled() {
        return sortAssertion;
    }

    /**
     * @see #setForceSort(boolean)
     */
    public boolean isForceSortEnabled() {
        return forceSort;
    }
}
