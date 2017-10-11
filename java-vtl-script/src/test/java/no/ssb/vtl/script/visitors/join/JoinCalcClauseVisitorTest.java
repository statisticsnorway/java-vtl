package no.ssb.vtl.script.visitors.join;

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

import com.google.common.collect.Maps;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.VTLExpression;
import no.ssb.vtl.model.VTLObject;
import no.ssb.vtl.parser.VTLLexer;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.visitors.ReferenceVisitor;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("PointlessArithmeticExpression")
public class JoinCalcClauseVisitorTest {

    @Test
    public void testNumericalLiteralsSum() throws Exception {
        String test = "1 + 2 + 3 + 4 + 5 - 6 - 7 - 8 - 9";
        VTLExpression result = getSimpleCalcResult(test);

        assertThat(result.getType()).isEqualTo(Long.class);
        assertThat(result.apply(null).get()).isEqualTo((1L + 2L + 3L + 4L + 5L - 6L - 7L - 8L - 9L));
    }

    @Test
    public void testNumericalLiteralsSumWithParenthesis() throws Exception {
        String test = "1 + 2 + ( 3 + 4 + 5 - 6 - 7 ) - 8 - 9";
        VTLExpression result = getSimpleCalcResult(test);

        assertThat(result.getType()).isEqualTo(Long.class);
        assertThat(result.apply(null).get()).isEqualTo(1L + 2L + (3L + 4L + 5L - 6L - 7L) - 8L - 9L);
    }

    @Test
    public void testNumericalLiteralsProduct() throws Exception {
        String test = "1 * 2 * 3 * 4 * 5 / 6 / 7 / 8 / 9";
        VTLExpression result = getSimpleCalcResult(test);

        assertThat(result.getType()).isEqualTo(Double.class);
        //noinspection PointlessArithmeticExpression
        assertThat(result.apply(null).get()).isEqualTo(1L * 2L * 3L * 4L * 5L / 6d / 7d / 8d / 9d);
    }

    @Test
    public void testNumericalVariableReference() {
        String test = "1 * 2 + a * (b - c) / d - 10";
        VTLParser parser = createParser(test);

        DataStructure ds = DataStructure.of(
                "a", Component.Role.MEASURE, Long.class,
                "b", Component.Role.MEASURE, Long.class,
                "c", Component.Role.MEASURE, Long.class,
                "d", Component.Role.MEASURE, Long.class
        );

        Map<String, Object> variables = Maps.newHashMap();
        variables.put("a", 20L);
        variables.put("b", 15L);
        variables.put("c", 10L);
        variables.put("d", 5L);

        DataPoint dataPoint = ds.wrap(variables);

        Map<String, Component> scope = Maps.newHashMap();
        scope.putAll(ds);

        JoinCalcClauseVisitor visitor = new JoinCalcClauseVisitor(new ReferenceVisitor(scope), ds);

        VTLExpression result = visitor.visit(parser.joinCalcExpression());

        assertThat(result.getType()).isEqualTo(Double.class);
        // TODO: Set variables.
        assertThat(result.apply(dataPoint).get()).isEqualTo(1L * 2L + 20L * (15L - 10L) / 5d - 10L);

    }

    @Test
    public void testNumericalLiteralsProductWithParenthesis() throws Exception {
        String test = "1 * 2 * ( 3 * 4 * 5 / 6 / 7 ) / 8 / 9";
        VTLExpression result = getSimpleCalcResult(test);

        assertThat(result.getType()).isEqualTo(Double.class);
        assertThat(result.apply(null).get()).isEqualTo(1L * 2L * (3L * 4L * 5L / 6d / 7d) / 8d / 9d);

    }

    @Test
    public void testNumericalReferenceNotFound() throws Exception {
        String test = "notFoundVariable + 1";
        VTLParser parser = createParser(test);

        JoinCalcClauseVisitor visitor = new JoinCalcClauseVisitor(new ReferenceVisitor(Collections.emptyMap()), null);

        assertThatThrownBy(() -> {
            // TODO: This should happen during execution (when data is computed).
            VTLExpression result = visitor.visit(parser.joinCalcExpression());
            result.apply(DataPoint.create(Collections.emptyList()));
        }).hasStackTraceContaining("variable")
                .hasStackTraceContaining("notFoundVariable");
    }

    @Test
    public void testSignedIntegerConstant() throws Exception {
        String expression = "10 * -1";
        Function<DataPoint, VTLObject> result = getSimpleCalcResult(expression);

        assertThat(result.apply(null).get()).isEqualTo(-10L);
    }

    @Test
    public void testSignedFloatConstant() throws Exception {
        String expression = "10 * -1.5";
        Function<DataPoint, VTLObject> result = getSimpleCalcResult(expression);

        assertThat(result.apply(null).get()).isEqualTo(-15d);
    }

    private VTLParser createParser(String test) {
        VTLLexer lexer = new VTLLexer(new ANTLRInputStream(test));
        VTLParser parser = new VTLParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new BailErrorStrategy());
        return parser;
    }

    private VTLExpression getSimpleCalcResult(String expression) {
        VTLParser parser = createParser(expression);

        JoinCalcClauseVisitor visitor = new JoinCalcClauseVisitor();
        return visitor.visit(parser.joinCalcExpression());

    }
}
