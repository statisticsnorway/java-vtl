package kohl.hadrien.vtl.parser;

/*-
 * #%L
 * java-vtl-parser
 * %%
 * Copyright (C) 2016 Hadrien Kohl
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
 * #L%
 */

import com.google.common.io.Resources;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.LexerInterpreter;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.tool.Grammar;
import org.antlr.v4.tool.GrammarParserInterpreter;
import org.antlr.v4.tool.Rule;
import org.assertj.core.api.SoftAssertions;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;

import java.net.URL;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.io.Resources.getResource;
import static org.assertj.core.api.Assertions.assertThat;

public class ClausesParserTest {

    private static Grammar grammar;
    @ClassRule
    public static ExternalResource grammarResource = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            URL grammarURL = getResource(this.getClass(), "/imports/Clauses.g4");
            String grammarString = Resources.toString(grammarURL, Charset.defaultCharset());
            grammar = new Grammar(checkNotNull(grammarString));
        }
    };

    @Test
    public void testRenameWithRole() throws Exception {

        SoftAssertions softly = new SoftAssertions();

        String expectedTree;
        String actualTree;

        String renameAsIdentifier = "[rename varId as varId role = IDENTIFIER]";
        softly.assertThat(filterWhiteSpaces(parse(renameAsIdentifier)))
                .isEqualTo(filterWhiteSpaces("" +
                        "(  " +
                        "   clauseExpression:1 [ (" +
                        "       clause:1 rename (" +
                        "           renameParam:1(varID:1varId)as(varID:1varId)role=(role:1IDENTIFIER))" +
                        "                       ) ]" +
                        ")"));

        String renameAsMeasure = "[rename varId as varId role = MEASURE]";
        softly.assertThat(filterWhiteSpaces(parse(renameAsMeasure)))
                .isEqualTo(filterWhiteSpaces("" +
                        "(  " +
                        "   clauseExpression:1 [ (" +
                        "       clause:1 rename (" +
                        "           renameParam:1(varID:1varId)as(varID:1varId)role=(role:1 MEASURE))" +
                        "                       ) ]" +
                        ")"));

        String renameAsAttribute = "[rename varId as varId role = ATTRIBUTE]";
        softly.assertThat(filterWhiteSpaces(parse(renameAsAttribute)))
                .isEqualTo(filterWhiteSpaces("" +
                        "(  " +
                        "   clauseExpression:1 [ (" +
                        "       clause:1 rename (" +
                        "           renameParam:1(varID:1varId)as(varID:1varId)role=(role:1 ATTRIBUTE))" +
                        "                       ) ]" +
                        ")"));

        softly.assertAll();
    }

    @Test
    public void testMultipleRenames() throws Exception {
        String expression = "[rename "
                + "varId as varId, "
                + "varId as varId, "
                + "varId as varId"
                + "]";

        String actualTree = parse(expression);
        String expectedTree = "" +
                "  (" +
                "    clauseExpression:1[(" +
                "      clause:1rename (renameParam:1(varID:1varId)as(varID:1varId))," +
                "                     (renameParam:1(varID:1varId)as(varID:1varId))," +
                "                     (renameParam:1(varID:1varId)as(varID:1varId))" +
                "    )]" +
                "  )";
        assertThat(filterWhiteSpaces(actualTree)).isEqualTo(
                filterWhiteSpaces(expectedTree));
    }

    @Test
    public void testMultipleRenamesWithRoles() throws Exception {

        String expression = "[rename "
                + "varId as varId role = IDENTIFIER, "
                + "varId as varId role = MEASURE, "
                + "varId as varId role = ATTRIBUTE"
                + "]";
        assertThat(filterWhiteSpaces(parse(expression)))
                .isEqualTo(filterWhiteSpaces("" +
                        "   (" +
                        "       clauseExpression:1 [(" +
                        "           clause:1 rename (renameParam:1(varID:1varId)as(varID:1varId)role=(role:1IDENTIFIER))," +
                        "                           (renameParam:1(varID:1varId)as(varID:1varId)role=(role:1MEASURE))," +
                        "                           (renameParam:1(varID:1varId)as(varID:1varId)role=(role:1ATTRIBUTE))" +
                        "                           )]" +
                        "   )"));
    }

    // TODO: Build a more robust way to test.
    private String parse(String expression) {
        LexerInterpreter lexerInterpreter = grammar.createLexerInterpreter(
                new ANTLRInputStream(expression)
        );
        GrammarParserInterpreter parserInterpreter = grammar.createGrammarParserInterpreter(
                new CommonTokenStream(lexerInterpreter)
        );

        Rule clause = grammar.getRule("clauseExpression");
        parserInterpreter.setErrorHandler(new GrammarParserInterpreter.BailButConsumeErrorStrategy());
        ParserRuleContext parse = parserInterpreter.parse(clause.index);
        return parse.toStringTree(parserInterpreter);
    }

    String filterWhiteSpaces(String string) {
        return string.replaceAll("\\s+", "");
    }
}
