package no.ssb.vtl.script.expressions.arithmetic;

/*
 * ========================LICENSE_START=================================
 * Java VTL
 * %%
 * Copyright (C) 2016 - 2017 Hadrien Kohl
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import com.codepoetics.protonpack.StreamUtils;
import no.ssb.vtl.model.VTLExpression;
import no.ssb.vtl.model.VTLFloat;
import no.ssb.vtl.model.VTLInteger;
import no.ssb.vtl.model.VTLNumber;
import no.ssb.vtl.model.VTLObject;
import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.*;

public class AbstractArithmeticExpressionTest {

    @Rule
    public JUnitSoftAssertions softly = new JUnitSoftAssertions();
    private VTLExpression floatExpr;
    private VTLExpression integerExpr;
    private VTLExpression numberExpr;

    private static VTLExpression createExpression(Class<? extends VTLObject> type, VTLObject value) {
        return new VTLExpression() {
            @Override
            public VTLObject resolve(Bindings bindings) {
                return value;
            }

            @Override
            public Class getVTLType() {
                return type;
            }
        };
    }

    @Before
    public void setUp() {
        integerExpr = mock(VTLExpression.class);
        when(integerExpr.getVTLType()).thenReturn(VTLInteger.class);

        floatExpr = mock(VTLExpression.class);
        when(floatExpr.getVTLType()).thenReturn(VTLFloat.class);

        numberExpr = mock(VTLExpression.class);
        when(numberExpr.getVTLType()).thenReturn(VTLNumber.class);
    }

    @Test
    public void testFailsFast() {
        VTLExpression mock = mock(VTLExpression.class);

        softly.assertThatThrownBy(() -> new TestableExpression(null, null))
                .isInstanceOf(NullPointerException.class);

        softly.assertThatThrownBy(() -> new TestableExpression(mock, null))
                .isInstanceOf(NullPointerException.class);

        softly.assertThatThrownBy(() -> new TestableExpression(null, mock))
                .isInstanceOf(NullPointerException.class);

        new TestableExpression(mock, mock);

    }

    @Test
    public void testTypes() {
        softly.assertThat(new TestableExpression(integerExpr, integerExpr).getVTLType())
                .as("type returned by abstract arithmetic expression")
                .isAssignableFrom(VTLInteger.class);

        softly.assertThat(new TestableExpression(floatExpr, integerExpr).getVTLType())
                .as("type returned by abstract arithmetic expression with float operand")
                .isAssignableFrom(VTLFloat.class);

        softly.assertThat(new TestableExpression(numberExpr, integerExpr).getVTLType())
                .as("type returned by abstract arithmetic expression with float operand")
                .isAssignableFrom(VTLNumber.class);

        softly.assertThat(new TestableExpression(integerExpr, floatExpr).getVTLType())
                .as("type returned by abstract arithmetic expression with float operand")
                .isAssignableFrom(VTLFloat.class);

        softly.assertThat(new TestableExpression(floatExpr, floatExpr).getVTLType())
                .as("type returned by abstract arithmetic expression with float operand")
                .isAssignableFrom(VTLFloat.class);

        softly.assertThat(new TestableExpression(numberExpr, floatExpr).getVTLType())
                .as("type returned by abstract arithmetic expression with float operand")
                .isAssignableFrom(VTLNumber.class);

        softly.assertThat(new TestableExpression(integerExpr, numberExpr).getVTLType())
                .as("type returned by abstract arithmetic expression with float operand")
                .isAssignableFrom(VTLNumber.class);

        softly.assertThat(new TestableExpression(floatExpr, numberExpr).getVTLType())
                .as("type returned by abstract arithmetic expression with float operand")
                .isAssignableFrom(VTLNumber.class);

        softly.assertThat(new TestableExpression(numberExpr, numberExpr).getVTLType())
                .as("type returned by abstract arithmetic expression with float operand")
                .isAssignableFrom(VTLNumber.class);
    }

    @Test
    public void testNulls() {

        List<Class<? extends VTLObject>> expressionTypes = Arrays.asList(
                VTLInteger.class, VTLInteger.class,
                VTLFloat.class, VTLInteger.class,
                VTLNumber.class, VTLInteger.class,
                VTLInteger.class, VTLFloat.class,
                VTLFloat.class, VTLFloat.class,
                VTLNumber.class, VTLFloat.class,
                VTLInteger.class, VTLNumber.class,
                VTLFloat.class, VTLNumber.class,
                VTLNumber.class, VTLNumber.class
        );

        List<VTLObject> expressionValues = Arrays.asList(
                VTLInteger.of((Integer) null), VTLInteger.of((Integer) null),
                VTLObject.of((Integer) null), VTLObject.of(0),
                VTLObject.of(0), VTLObject.of((Integer) null)
        );

        StreamUtils.windowed(expressionTypes.stream(), 2, 2).forEach(types -> {
            StreamUtils.windowed(expressionValues.stream(), 2, 2).forEach(values -> {

                VTLExpression left = createExpression(types.get(0), values.get(0));
                VTLExpression right = createExpression(types.get(1), values.get(1));

                TestableExpression expression = new TestableExpression(left, right);
                VTLObject result = expression.resolve(new SimpleBindings());
                softly.assertThat(result.get())
                        .as("result of %s,%s", values.get(0), values.get(1)).isNull();
            });
        });

        softly.assertThatThrownBy(() -> new TestableExpression(
                        createExpression(VTLInteger.class, VTLInteger.of(0)),
                        createExpression(VTLInteger.class, VTLInteger.of(0))
                ).resolve(new SimpleBindings())
        ).hasMessage("should not have been called");
    }

    private class TestableExpression extends AbstractArithmeticExpression {

        TestableExpression(VTLExpression leftOperand, VTLExpression rightOperand) {
            super(leftOperand, rightOperand);
        }

        @Override
        protected VTLNumber compute(VTLNumber leftOperand, VTLNumber rightOperand) {
            throw new AssertionError("should not have been called");
        }

    }
}
