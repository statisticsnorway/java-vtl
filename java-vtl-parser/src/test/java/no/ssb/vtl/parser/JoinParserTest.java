package kohl.hadrien.vtl.parser;

import com.google.common.io.Resources;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.tool.Grammar;
import org.antlr.v4.tool.GrammarParserInterpreter;
import org.antlr.v4.tool.Rule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import java.net.URL;
import java.nio.charset.Charset;
import java.util.BitSet;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;

public class JoinParserTest {

    private static Grammar grammar;
    @ClassRule
    public static ExternalResource grammarResource = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            URL grammarURL = getResource(this.getClass(), "/imports/Relational.g4");
            String grammarString = Resources.toString(grammarURL, Charset.defaultCharset());
            grammar = new Grammar(checkNotNull(grammarString));
        }
    };

    @Test
    public void testJoin() throws Exception {
        String expression = "" +
                "[varID1, varID2]{\n" +
                "  varID2 = varID2.varID2 + 1 * 2 / varID2.varID2 - 2,\n" +
                "  varID2 = varID2.varID2 + 3 * 4 / varID2.varID2 - 5\n" +
                "}";
        String parseTree = parse(expression);

        // TODO: Check this.
        assertThat(parseTree).isEqualTo("(joinExpression:1 [ " +
                "(joinDefinition:1 (joinParam:1 (varID:1 varID 1) , (varID:1 varID 2))) " +
                "] { " +
                "(joinBody:1 (joinClause:1 (varID:1 varID 2) = (joinCalcExpression:2 (joinCalcExpression:2 (joinCalcExpression:4 (joinCalcRef:1 (varID:1 varID 2) . (varID:1 varID 2))) + (joinCalcExpression:1 (joinCalcExpression:1 (joinCalcExpression:5 (constant:1 1)) * (joinCalcExpression:5 (constant:1 2))) / (joinCalcExpression:4 (joinCalcRef:1 (varID:1 varID 2) . (varID:1 varID 2))))) - (joinCalcExpression:5 (constant:1 2)))) , (joinClause:1 (varID:1 varID 2) = (joinCalcExpression:2 (joinCalcExpression:2 (joinCalcExpression:4 (joinCalcRef:1 (varID:1 varID 2) . (varID:1 varID 2))) + (joinCalcExpression:1 (joinCalcExpression:1 (joinCalcExpression:5 (constant:1 3)) * (joinCalcExpression:5 (constant:1 4))) / (joinCalcExpression:4 (joinCalcRef:1 (varID:1 varID 2) . (varID:1 varID 2))))) - (joinCalcExpression:5 (constant:1 5))))) })");
    }

    @Test
    public void testJoinWithOn() throws Exception {
        String expression = "" +
                "[outer varID1, varID2 on dimensionExpr1, dimensionExpr2]{\n" +
                "  varID2 = varID2.varID2 + 1 * 2 / varID2.varID2 - 2,\n" +
                "  varID2 = varID2.varID2 + 3 * 4 / varID2.varID2 - 5\n" +
                "}";
        String parseTree = parse(expression);

        // TODO: Check this.
        assertThat(parseTree).isEqualTo("(joinExpression:1 [ (joinDefinition:2 outer (joinParam:1 (varID:1 varID 1) ," +
                " (varID:1 varID 2) on (dimensionExpression:1 dimensionExpr 1) , (dimensionExpression:1 dimensionExpr 2))) " +
                "] { (joinBody:1 (joinClause:1 (varID:1 varID 2) = (joinCalcExpression:2 (joinCalcExpression:2 (joinCalcExpression:4 " +
                "(joinCalcRef:1 (varID:1 varID 2) . (varID:1 varID 2))) + (joinCalcExpression:1 (joinCalcExpression:1 (joinCalcExpression:5" +
                " (constant:1 1)) * (joinCalcExpression:5 (constant:1 2))) / (joinCalcExpression:4 (joinCalcRef:1 (varID:1 varID 2) ." +
                " (varID:1 varID 2))))) - (joinCalcExpression:5 (constant:1 2)))) , (joinClause:1 (varID:1 varID 2) = " +
                "(joinCalcExpression:2 (joinCalcExpression:2 (joinCalcExpression:4 (joinCalcRef:1 (varID:1 varID 2) . " +
                "(varID:1 varID 2))) + (joinCalcExpression:1 (joinCalcExpression:1 (joinCalcExpression:5 (constant:1 3)) " +
                "* (joinCalcExpression:5 (constant:1 4))) / (joinCalcExpression:4 (joinCalcRef:1 (varID:1 varID 2) . " +
                "(varID:1 varID 2))))) - (joinCalcExpression:5 (constant:1 5))))) })");
    }

    // TODO: Build a more robust way to test.
    private String parse(String expression) {
        LexerInterpreter lexerInterpreter = grammar.createLexerInterpreter(
                new ANTLRInputStream(expression)
        );
        GrammarParserInterpreter parserInterpreter = grammar.createGrammarParserInterpreter(
                new CommonTokenStream(lexerInterpreter)
        );

        FailingErrorListener listener = new FailingErrorListener();
        parserInterpreter.addErrorListener(listener);
        lexerInterpreter.addErrorListener(listener);

        Rule clause = grammar.getRule("joinExpression");
        ParserRuleContext parse = parserInterpreter.parse(clause.index);
        return parse.toStringTree(parserInterpreter);
    }

    private class FailingErrorListener implements ANTLRErrorListener {

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
            throw new RuntimeException(msg, e);
        }

        @Override
        public void reportAmbiguity(Parser recognizer, DFA dfa, int startIndex, int stopIndex, boolean exact, BitSet ambigAlts, ATNConfigSet configs) {
            //throw new RuntimeException("ambiguity");
        }

        @Override
        public void reportAttemptingFullContext(Parser recognizer, DFA dfa, int startIndex, int stopIndex, BitSet conflictingAlts, ATNConfigSet configs) {
            //throw new RuntimeException("full context");
        }

        @Override
        public void reportContextSensitivity(Parser recognizer, DFA dfa, int startIndex, int stopIndex, int prediction, ATNConfigSet configs) {
            //throw new RuntimeException("context sensitivity");
        }
    }
}
