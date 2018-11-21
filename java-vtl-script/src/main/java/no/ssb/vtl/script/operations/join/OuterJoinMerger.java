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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Table;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.model.VTLObject;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.ToIntFunction;

public class OuterJoinMerger implements BiFunction<DataPoint, DataPoint, DataPoint> {

    private final Map<String, String> rightMapping;

    private final DataPointView rightView;
    private final DataPointView resultView;
    private final int size;

    public OuterJoinMerger(AbstractJoinOperation joinOperation, Dataset right) {

        Table<String, String, String> mapping = AbstractJoinOperation.getColumnMapping(joinOperation.datasets);
        ImmutableBiMap<Dataset, String> datasetNames = ImmutableBiMap.copyOf(joinOperation.datasets).inverse();

        size = mapping.rowKeySet().size();

        rightMapping = mapping.column(datasetNames.get(right));
        rightView = new DataPointView(right.getDataStructure());
        resultView = new DataPointView(joinOperation.getDataStructure());
    }

    @Override
    public DataPoint apply(DataPoint left, DataPoint right) {

        resultView.setDataDoint(DataPoint.create(size));
        rightView.setDataDoint(right);

        if (left != null) {
            resultView.setDataDoint(DataPoint.create(left));
        } else {
            resultView.setDataDoint(DataPoint.create(size));
        }
        if (right != null) {
            for (Map.Entry<String, String> mapping : rightMapping.entrySet()) {
                resultView.put(mapping.getKey(), rightView.get(mapping.getValue()));
            }
        }
        return resultView.getDataDoint();
    }

    private final class DataPointView implements Map<String, VTLObject> {

        private final ToIntFunction<String> hash;
        private DataPoint dp = DataPoint.create(0);


        public DataPointView(DataStructure structure) {
            ImmutableList<String> list = ImmutableSet.copyOf(structure.keySet()).asList();
            this.hash = list::indexOf;
        }

        public VTLObject get(String key) {
            return dp.get(hash.applyAsInt(key));
        }

        public VTLObject remove(String key) {
            return dp.set(hash.applyAsInt(key), VTLObject.NULL);
        }

        public DataPoint getDataDoint() {
            return dp;
        }

        public void setDataDoint(DataPoint dp) {
            this.dp = dp;
        }

        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsKey(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public VTLObject get(Object key) {
            return this.get((String) key);
        }

        @Override
        public VTLObject put(String key, VTLObject value) {
            return dp.set(hash.applyAsInt(key), value);
        }

        @Override
        public VTLObject remove(Object key) {
            return this.remove((String) key);
        }

        @Override
        public void putAll(Map<? extends String, ? extends VTLObject> m) {
            for (Entry<? extends String, ? extends VTLObject> entry : m.entrySet()) {
                this.put(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void clear() {
            dp.clear();
        }

        @Override
        public Set<String> keySet() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<VTLObject> values() {
            return dp;
        }

        @Override
        public Set<Entry<String, VTLObject>> entrySet() {
            throw new UnsupportedOperationException();
        }
    }
}
