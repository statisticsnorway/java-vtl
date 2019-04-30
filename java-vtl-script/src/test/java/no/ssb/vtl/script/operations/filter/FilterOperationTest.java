package no.ssb.vtl.script.operations.filter;

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

import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.StaticDataset;
import no.ssb.vtl.model.VTLBoolean;
import no.ssb.vtl.model.VTLExpression;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.script.operations.join.ComponentBindings;
import no.ssb.vtl.script.support.DatasetCloseWatcher;
import org.junit.Before;
import org.junit.Test;

import javax.script.Bindings;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterOperationTest {

    private static VTLExpression NULL = new VTLExpression() {
        @Override
        public VTLObject resolve(Bindings bindings) {
            return VTLObject.of((VTLObject) null);
        }

        @Override
        public Class getVTLType() {
            return VTLBoolean.class;
        }
    };
    private static VTLExpression FALSE = new VTLExpression() {
        @Override
        public VTLObject resolve(Bindings bindings) {
            return VTLBoolean.of(false);
        }

        @Override
        public Class getVTLType() {
            return VTLBoolean.class;
        }
    };
    private static VTLExpression TRUE = new VTLExpression() {
        @Override
        public VTLObject resolve(Bindings bindings) {
            return VTLBoolean.of(true);
        }

        @Override
        public Class getVTLType() {
            return VTLBoolean.class;
        }
    };
    private DatasetCloseWatcher dataset;
    private ComponentBindings componentBindings;

    @Before
    public void setUp() {
        dataset = DatasetCloseWatcher.wrap(StaticDataset.create()
                .addComponent("id", Component.Role.IDENTIFIER, String.class)
                .addComponent("mc", Component.Role.MEASURE, String.class)
                .addPoints("idValue", null)
                .build());
        componentBindings = new ComponentBindings(dataset);
    }

    @Test
    public void testPredicateReturnsNull() {
        FilterOperation resultBooleanNull = new FilterOperation(dataset, NULL, componentBindings);
        try (Stream<DataPoint> data = resultBooleanNull.getData()) {
            assertThat(data).isEmpty();
        } finally {
            assertThat(dataset.allStreamWereClosed()).isTrue();
        }
    }

    @Test
    public void testPredicateReturnsFalse() {

        FilterOperation result = new FilterOperation(dataset, FALSE, componentBindings);
        try (Stream<DataPoint> data = result.getData()) {
            assertThat(data).isEmpty();
        } finally {
            assertThat(dataset.allStreamWereClosed()).isTrue();
        }
    }


    @Test
    public void testPredicateReturnsTrue() {

        FilterOperation result = new FilterOperation(dataset, TRUE, componentBindings);
        try (Stream<DataPoint> data = result.getData()) {
            assertThat(data).isNotEmpty();
        } finally {
            assertThat(dataset.allStreamWereClosed()).isTrue();
        }
    }
}

