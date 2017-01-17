package no.ssb.vtl.script.join;

import com.google.common.collect.Maps;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.DataPoint;
import no.ssb.vtl.model.DataStructure;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.parser.VTLLexer;
import no.ssb.vtl.parser.VTLParser;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenStream;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

public class JoinCalcClauseVisitorTest {

    @Test
    public void testNumericalLiteralsSum() throws Exception {

        String test = "1 + 2 + 3 + 4 + 5 - 6 - 7 - 8 - 9";
        VTLLexer lexer = new VTLLexer(new ANTLRInputStream(test));
        VTLParser parser = new VTLParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new BailErrorStrategy());

        JoinCalcClauseVisitor visitor = new JoinCalcClauseVisitor();

        Function<Dataset.Tuple, Object> result = visitor.visit(parser.joinCalcExpression());

        assertThat(result.apply(null)).isEqualTo(1 + 2 + 3 + 4 + 5 - 6 - 7 - 8 - 9);

    }

    @Test
    public void testNumericalLiteralsSumWithParenthesis() throws Exception {

        String test = "1 + 2 + ( 3 + 4 + 5 - 6 - 7 ) - 8 - 9";
        VTLLexer lexer = new VTLLexer(new ANTLRInputStream(test));
        VTLParser parser = new VTLParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new BailErrorStrategy());

        JoinCalcClauseVisitor visitor = new JoinCalcClauseVisitor();

        Function<Dataset.Tuple, Object> result = visitor.visit(parser.joinCalcExpression());

        assertThat(result.apply(null)).isEqualTo(1 + 2 + (3 + 4 + 5 - 6 - 7) - 8 - 9);

    }

    @Test
    public void testNumericalLiteralsProduct() throws Exception {

        String test = "1 * 2 * 3 * 4 * 5 / 6 / 7 / 8 / 9";
        VTLLexer lexer = new VTLLexer(new ANTLRInputStream(test));
        VTLParser parser = new VTLParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new BailErrorStrategy());

        JoinCalcClauseVisitor visitor = new JoinCalcClauseVisitor();

        Function<Dataset.Tuple, Object> result = visitor.visit(parser.joinCalcExpression());

        assertThat(result.apply(null)).isEqualTo(1 * 2 * 3 * 4 * 5 / 6 / 7 / 8 / 9);

    }

    @Test
    public void testNumericalVariableReference() {
        String test = "1 * 2 + a * (b - c) / d - 10";

        VTLLexer lexer = new VTLLexer(new ANTLRInputStream(test));
        VTLParser parser = new VTLParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new BailErrorStrategy());

        DataStructure ds = DataStructure.of((o, aClass) -> o,
                "a", Component.Role.MEASURE, Integer.class,
                "b", Component.Role.MEASURE, Integer.class,
                "c", Component.Role.MEASURE, Integer.class,
                "d", Component.Role.MEASURE, Integer.class
        );

        Map<String, Object> variables = Maps.newHashMap();
        variables.put("a", 20);
        variables.put("b", 15);
        variables.put("c", 10);
        variables.put("d", 5);

        Dataset.Tuple tuple = ds.wrap(variables);

        JoinCalcClauseVisitor visitor = new JoinCalcClauseVisitor();

        Function<Dataset.Tuple, Object> result = visitor.visit(parser.joinCalcExpression());

        // TODO: Set variables.
        assertThat(result.apply(tuple)).isEqualTo(1 * 2 + 20 * (15 - 10) / 5 - 10);

    }

    @Test
    public void testNumericalLiteralsProductWithParenthesis() throws Exception {

        String test = "1 * 2 * ( 3 * 4 * 5 / 6 / 7 ) / 8 / 9";
        VTLLexer lexer = new VTLLexer(new ANTLRInputStream(test));
        VTLParser parser = new VTLParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new BailErrorStrategy());

        // Setup fake map.

        JoinCalcClauseVisitor visitor = new JoinCalcClauseVisitor();

        Function<Dataset.Tuple, Object> result = visitor.visit(parser.joinCalcExpression());

        assertThat(result.apply(null)).isEqualTo(1 * 2 * (3 * 4 * 5 / 6 / 7) / 8 / 9);

    }

    @Test
    public void testNumericalReferenceNotFound() throws Exception {

        String test = "notFoundVariable + 1";
        VTLLexer lexer = new VTLLexer(new ANTLRInputStream(test));
        VTLParser parser = new VTLParser(new CommonTokenStream(lexer));
        parser.setErrorHandler(new BailErrorStrategy());
        JoinCalcClauseVisitor visitor = new JoinCalcClauseVisitor();


        Throwable ex = null;
        try {
            // TODO: This should happen during execution (when data is computed).
            Function<Dataset.Tuple, Object> result = visitor.visit(parser.joinCalcExpression());
            result.apply(new Dataset.AbstractTuple() {
                @Override
                protected List<DataPoint> delegate() {
                    return Collections.emptyList();
                }
            });
        } catch (Throwable t) {
            ex = t;
        }
        assertThat(ex).hasMessageContaining("variable")
                .hasMessageContaining("notFoundVariable");

    }

    private DataPoint createNumericalDataPoint(Integer value) {
        DataStructure structure = DataStructure.of(
                (o, aClass) -> o,
                "value",
                Component.Role.MEASURE,
                Integer.class
        );
        return structure.wrap("value", value);
    }
}
