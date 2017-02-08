package no.ssb.vtl.test.junit;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Resources;
import org.antlr.runtime.RecognitionException;
import org.antlr.v4.runtime.*;
import org.antlr.v4.tool.Grammar;
import org.antlr.v4.tool.GrammarParserInterpreter;
import org.antlr.v4.tool.Rule;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import java.net.URL;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A grammar rule that can be used to simplify grammar testing.
 * <p>
 * Parsing errors are reported using the {@link ErrorCollector}.
 */
public class GrammarRule implements TestRule {

    private String startRule;
    private URL grammarURL;
    private Grammar grammar;

    public GrammarRule() {
    }

    public GrammarRule(URL grammarURL, String startRule) {
        this.grammarURL = checkNotNull(grammarURL);
        this.startRule = checkNotNull(startRule);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                before();
                base.evaluate();
            }
        };
    }

    private void before() {
        try {
            if (grammarURL == null) {
                try {
                    Class<?> vtlParserClass = ClassLoader.getSystemClassLoader()
                            .loadClass("no.ssb.vtl.parser.VTLParser");
                    grammarURL = Resources.getResource(vtlParserClass, "VTL.g4");
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(
                            "could not find no.ssb.vtl.parser.VTLParser.class"
                    );
                }
            }
            String grammarString = Resources.toString(grammarURL, Charset.defaultCharset());
            grammar = new Grammar(grammarString);
        } catch (IOException | RecognitionException e) {
            throw new IllegalArgumentException("could not create grammar", e);
        }
    }

    public Rule withRule(String ruleName) {
        return checkNotNull(
                grammar.getRule(ruleName),
                "could not find rule %s in grammar %s",
                ruleName, grammarURL
        );
    }

    /**
     * Parse an expression starting from the default rule.
     *
     * @param expression the expression to parse.
     * @return the resulting parse tree.
     * @throws Exception if the expression failed to parse.
     */
    public ParserRuleContext parse(String expression) throws Exception {
        return parse(expression, withRule(checkNotNull(startRule)));
    }

    /**
     * Parse an expression starting from the given <b>ANTLR rule</b>
     * <p>
     * In order to get the Rule, use the {@link #withRule(String)} method.
     *
     * @param expression the expression to parse.
     * @param rule       the rule to start from.
     * @return the resulting parse tree.
     * @throws Exception if the expression failed to parse.
     */
    public ParserRuleContext parse(String expression, Rule rule) throws Exception {

        Multimap<Integer, String> messages = LinkedListMultimap.create();

        LexerInterpreter lexerInterpreter = grammar.createLexerInterpreter(
                new ANTLRInputStream(expression)
        );
        GrammarParserInterpreter parserInterpreter = grammar.createGrammarParserInterpreter(
                new CommonTokenStream(lexerInterpreter)
        );

        DiagnosticErrorListener diagnosticErrorListener = new DiagnosticErrorListener();
        BaseErrorListener ruleErrorReporter = new BaseErrorListener() {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, org.antlr.v4.runtime.RecognitionException e) {
                int startLine = line, stopLine = line;
                int startColumn = charPositionInLine, stopColumn = charPositionInLine;
                if (offendingSymbol instanceof Token) {
                    Token symbol = (Token) offendingSymbol;
                    int start = symbol.getStartIndex();
                    int stop = symbol.getStopIndex();
                    if (start >= 0 && stop >= 0) {
                        stopColumn = startColumn + (stop - start) + 1;
                    }
                }
                messages.put(stopLine,
                        String.format("at [%4s:%6s]:\t%s (%s)\n",
                                String.format("%d,%d", startLine, stopLine),
                                String.format("%d,%d", startColumn, stopColumn),
                                msg, e.getClass().getSimpleName())
                );
            }
        };

        parserInterpreter.setErrorHandler(new GrammarParserInterpreter.BailButConsumeErrorStrategy());
        lexerInterpreter.removeErrorListeners();
        parserInterpreter.removeErrorListeners();

        lexerInterpreter.addErrorListener(diagnosticErrorListener);
        parserInterpreter.addErrorListener(diagnosticErrorListener);
        lexerInterpreter.addErrorListener(ruleErrorReporter);
        parserInterpreter.addErrorListener(ruleErrorReporter);

        ParserRuleContext parse = parserInterpreter.parse(rule.index);

        if (!messages.isEmpty()) {

            StringBuilder expressionWithErrors = new StringBuilder();
            LineNumberReader expressionReader = new LineNumberReader(new StringReader(expression));
            String line;
            while ((line = expressionReader.readLine()) != null) {
                int lineNumber = expressionReader.getLineNumber();
                expressionWithErrors.append(
                        String.format("\t%d:%s%n", lineNumber, line)
                );
                if (messages.containsKey(lineNumber)) {
                    expressionWithErrors.append(String.format("%n"));
                    for (String message : messages.get(lineNumber)) {
                        expressionWithErrors.append(message);
                    }
                }
            }
            throw new Exception(String.format("errors parsing expression:%n%n%s%n", expressionWithErrors.toString()));
        }

        return parse;
    }

}
