package kohl.hadrien.vtl.parser;

import com.google.common.io.Resources;
import org.antlr.v4.runtime.*;
import org.antlr.v4.tool.Grammar;
import org.antlr.v4.tool.GrammarParserInterpreter;
import org.antlr.v4.tool.Rule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import java.net.URL;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;

public class KeepClauseTest {

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
    public void testJoinKeep() throws Exception {
        String expression = "" +
                "keep varID1, varID2, varID3.varID4";
        String parseTree = parse(expression, "joinKeepExpression");

        // TODO: Check this.
        assertThat(parseTree).isEqualTo("(joinKeepExpression:1 keep (joinKeepRef:2 (varID:1 varID 1)) , (joinKeepRef:2 (varID:1 varID 2)) , (joinKeepRef:1 (varID:1 varID 3) . (varID:1 varID 4)))");
    }

    // TODO: Build a more robust way to test.
    private String parse(String expression, String rule) {
        LexerInterpreter lexerInterpreter = grammar.createLexerInterpreter(
                new ANTLRInputStream(expression)
        );
        GrammarParserInterpreter parserInterpreter = grammar.createGrammarParserInterpreter(
                new CommonTokenStream(lexerInterpreter)
        );

        //JoinParserTest.FailingErrorListener listener = new JoinParserTest.FailingErrorListener();
        //parserInterpreter.addErrorListener(listener);
        //lexerInterpreter.addErrorListener(listener);

        parserInterpreter.setErrorHandler(new BailErrorStrategy());

        Rule clause = grammar.getRule(rule);
        ParserRuleContext parse = parserInterpreter.parse(clause.index);
        return parse.toStringTree(parserInterpreter);
    }

}
