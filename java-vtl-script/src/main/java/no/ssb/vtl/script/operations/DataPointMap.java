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

import com.google.common.collect.ImmutableList;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VTLObject;

import java.util.function.ToIntFunction;

/**
 * Wrapper class to access values of a DataPoint by column name.
 */
public class DataPointMap {

    private final ToIntFunction<String> hash;
    private DataPoint dataPoint;

    public DataPointMap(ToIntFunction<String> hash) {
        this.hash = hash;
    }

    public DataPointMap(DataStructure dataStructure) {
        ImmutableList<String> indices = ImmutableList.copyOf(dataStructure.keySet());
        this.hash = indices::indexOf;
    }

    public DataPointMap(Dataset dataset) {
        DataStructure dataStructure = dataset.getDataStructure();
        ImmutableList<String> indices = ImmutableList.copyOf(dataStructure.keySet());
        this.hash = indices::indexOf;
    }

    /**
     * Returns the current underlying Datapoint value.
     */
    public DataPoint getDataPoint() {
        return this.dataPoint;
    }

    /**
     * Update the underlying Datapoint value.
     */
    public DataPointMap withDataPoint(DataPoint dataPoint) {
        this.dataPoint = dataPoint;
        return this;
    }

    /**
     * Update the underlying Datapoint value.
     */
    public void setDataPoint(DataPoint dataPoint) {
        this.dataPoint = dataPoint;
    }

    /**
     * Returns the element at the specified column in this list.
     *
     * @param column column of the element to return
     * @return the element at the specified column
     */
    public VTLObject get(String column) {
        return this.dataPoint.get(hash.applyAsInt(column));
    }

    /**
     * Replaces the element at the specified column with the specified element.
     *
     * @param column  column of the element to replace.
     * @param element element to be stored at the specified column
     * @return the element previously at the specified position
     */
    public VTLObject set(String column, VTLObject element) {
        return this.dataPoint.set(hash.applyAsInt(column), element);
    }

}
