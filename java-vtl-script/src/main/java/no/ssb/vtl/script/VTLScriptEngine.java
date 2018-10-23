package no.ssb.vtl.script;

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import no.ssb.vtl.connectors.Connector;
import no.ssb.vtl.parser.VTLLexer;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.error.ContextualRuntimeException;
import no.ssb.vtl.script.error.VTLCompileException;
import no.ssb.vtl.script.error.VTLScriptException;
import no.ssb.vtl.script.support.SyntaxErrorListener;
import no.ssb.vtl.script.visitors.AssignmentVisitor;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Consumer;

/**
 * A VTL {@link ScriptEngine} implementation.
 */
public class VTLScriptEngine extends AbstractScriptEngine {

    private final ImmutableList<Connector> connectors;
    private TimeZone timeZone = TimeZone.getDefault();

    /**
     * Create a new engine instance.
     *
     * @param connectors connectors that will be used to retrieve data.
     */
    public VTLScriptEngine(Connector... connectors) {
        this.connectors = ImmutableList.copyOf(connectors);
        context = new VTLScriptContext();
    }

    /**
     * Create a new engine instance.
     *
     * @param n          the bindings to use.
     * @param connectors connectors that will be used to retrieve data.
     */
    public VTLScriptEngine(Bindings n, Connector... connectors) {
        super(n);
        this.connectors = ImmutableList.copyOf(connectors);
        context = new VTLScriptContext();
    }

    /**
     * Returns the default time zone of the JVM.
     * //TODO use non-static method to be able to set up time zone per VTLScriptEngine instance. Need to also expose in Visitors
     *
     * @return the default time zone of the JVM.
     */
    public static TimeZone getTimeZone() {
        return TimeZone.getDefault();
    }

    /**
     * Sets a different global timezone.
     * <p>
     * The engine will use a different global time zone in situations no implicit time
     * zone is defined in date conversion operations. For example:
     * <pre>
     *     osloTz = TimeZone.getTimeZone("Europe/Oslo");
     *     vtlEngine.setTimeZone(osloTz);
     *
     *     // date will be 1999-12-31T23:00:00.000Z
     *     vtlEngine.eval("date := date_from_string(\"2000\", \"YYYY\")");
     *
     * </pre>
     *
     * @param tz the time zone
     */
    public void setTimeZone(TimeZone tz) {
        timeZone = tz;
    }

    @Override
    public Object eval(String script, ScriptContext context) throws ScriptException {
        return eval(new StringReader(script), context);
    }

    public VTLParser.StartContext parse(Reader reader, Consumer<VTLScriptException> errorConsumer) throws IOException {
        // TODO: Change to CharStreams.fromString() when #1977 makes it to release.
        VTLLexer lexer = new VTLLexer(new ANTLRInputStream(reader));
        VTLParser parser = new VTLParser(new CommonTokenStream(lexer));

        lexer.removeErrorListeners();
        parser.removeErrorListeners();

        BaseErrorListener errorListener = new SyntaxErrorListener(errorConsumer);

        lexer.addErrorListener(errorListener);
        parser.addErrorListener(errorListener);

        return parser.start();
    }

    @Override
    public Object eval(Reader reader, ScriptContext context) throws ScriptException {
        try {
            ArrayList<VTLScriptException> errors = Lists.newArrayList();
            VTLParser.StartContext start = parse(reader, errors::add);
            Object returnValue = run(start, errors::add, context);
            if (!errors.isEmpty()) {
                throw new VTLCompileException(errors);
            } else {
                return returnValue;
            }
        } catch (IOException | RuntimeException unknownException) {
            throw new ScriptException(unknownException);
        }
    }

    /**
     * Returns a collection of all keywords/reserved words in VTL, divided into the following categories:
     * - implementedVtlKeywords - implemented functions from the specifications
     * - builtinFunctions - implemented custom functions
     * - dataTypes - the data types used in VTL
     * - notImplementedKeywordsAndFunctions - functions from the specifications that are not yet implemented
     * - builtInConstants - VTL constants
     * @return Collection of VTL keywords/reserved words
     */
    public Map<String, Set<String>> getVTLKeywords() {

        // TODO can we generate these lists automatically from the grammar file?
        Map<String, Set<String>> allKeywords = new HashMap<>();
        allKeywords.put("implementedVtlKeywords", new HashSet<>(Arrays.asList("get", "put", "and", "or", "join",
                "xor", "not", "is null", "is not null",
                "inner", "outer", "cross", "on", "rename",
                "fold", "unfold", "keep", "drop", "filter", "to", "union", "nvl", "as", "isnull", "check",

                "hierarchy",

                "abs", "ceil", "date_from_string", "exp", "floor", "ln", "log", "mod", "nroot",

                "power", "round", "sqrt", "string_from_number", "substr", "trunc",

                "trim", "ltrim", "lower", "rtrim", "upper",

                "foreach", "in", "do", "done",

                "sum", "avg", "along", "group by", "if", "then", "else", "elseif")));
        allKeywords.put("builtinFunctions", new HashSet<>(Arrays.asList("integer_from_string",
                "float_from_string", "string_from_number")));
        allKeywords.put("dataTypes", new HashSet<>(Arrays.asList("identifier", "measure", "attribute")));
        allKeywords.put("notImplementedKeywordsAndFunctions", new HashSet<>(Arrays.asList("exists_in", "not_exists_in",
                "exists_in_all", "not_exists_in_all",
                "match_characters", "all", "any", "unique", "func_dep", "extract", "string_from_date", "current_date",
                "listsum", "alterdataset", "eval", "lenght", "concatenation", "instr", "replace", "intersect",
                "symdiff", "setdiff", "subscript", "transcode", "aggregate", "aggregatefunctions", "time_aggregate",
                "fill_time_series", "flow_to_stock", "stock_to_flow", "timeshift", "calc", "attrcalc")));
        allKeywords.put("builtinConstants", new HashSet<>(Arrays.asList("true", "false", "null")));

        return allKeywords;
    }

    /**
     * Run loop
     */
    protected Object run(VTLParser.StartContext start, Consumer<VTLScriptException> errorConsumer, ScriptContext context) throws VTLScriptException {
        AssignmentVisitor assignmentVisitor = new AssignmentVisitor(context, connectors);
        Object last = null;
        for (VTLParser.StatementContext statementContext : start.statement()) {
            try {
                last = assignmentVisitor.visit(statementContext);
            } catch (ContextualRuntimeException cre) {
                ParserRuleContext ctx = cre.getContext();
                if (cre.getCause() != null) {
                    errorConsumer.accept(new VTLScriptException((Exception) cre.getCause(), ctx));
                } else {
                    errorConsumer.accept(new VTLScriptException(cre.getMessage(), ctx));
                }
            }
        }
        return last;
    }

    @Override
    public Bindings createBindings() {
        return new SimpleBindings(Maps.newLinkedHashMap());
    }

    @Override
    public ScriptEngineFactory getFactory() {
        return null;
    }
}
