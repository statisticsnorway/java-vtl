package no.ssb.vtl.script.visitors;

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

import com.google.common.collect.Queues;
import no.ssb.vtl.model.Component;
import no.ssb.vtl.model.Dataset;
import no.ssb.vtl.parser.VTLBaseVisitor;
import no.ssb.vtl.parser.VTLParser;
import no.ssb.vtl.script.error.SyntaxException;
import no.ssb.vtl.script.error.TypeException;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.Deque;
import java.util.Map;

import static com.google.common.base.Preconditions.*;
import static java.lang.String.*;

/**
 * The reference visitor tries to find references to object in a Bindings.
 */
public class ReferenceVisitor extends VTLBaseVisitor<Object> {

    private Deque<Map<String, ?>> stack = Queues.newArrayDeque();

    public ReferenceVisitor(Map<String, ?> scope) {
        this.stack.push(checkNotNull(scope, "scope cannot be empty"));
    }

    protected ReferenceVisitor() {
    }

    private static String removeQuoteIfNeeded(String key) {
        if (!key.isEmpty() && key.length() > 3) {
            if (key.charAt(0) == '\'' && key.charAt(key.length() - 1) == '\'') {
                return key.substring(1, key.length() - 1);
            }
        }
        return key;
    }

    private static <T> T checkFound(String expression, T instance) {
        if (instance != null) {
            return instance;
        }
        throw new ParseCancellationException(new SyntaxException(
                format(
                        "variable [%s] not found",
                        expression
                ), "VTL-01xx")
        );
    }

    private static <T> T checkType(String expression, Object instance, Class<T> clazz) {
        if (clazz.isAssignableFrom(instance.getClass())) {
            //noinspection unchecked
            return (T) instance;
        }
        throw new ParseCancellationException(new TypeException(
                format(
                        "wrong type for [%s], expected %s, got %s",
                        expression,
                        clazz,
                        instance.getClass()
                ), "VTL-02xx")
        );
    }

    @Override
    public Object visitVariable(VTLParser.VariableContext ctx) {
        // TODO: Would be nice to handle quote removal in ANTLR
        String key = removeQuoteIfNeeded(ctx.getText());
        return checkFound(ctx.getText(), stack.peek().get(key));
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public Object visitComponentRef(VTLParser.ComponentRefContext ctx) {
        // Ensure data type component.
        Component component;
        if (ctx.datasetRef() != null) {
            Dataset ds = (Dataset) visit(ctx.datasetRef());
            this.stack.push(ds.getDataStructure());
            component = checkType(ctx.getText(), visit(ctx.variable()), Component.class);
            this.stack.pop();
        } else {
            component = checkType(ctx.getText(), visit(ctx.variable()), Component.class);
        }
        return component;
    }

    /**
     * {@inheritDoc}
     * <p>
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     *
     * @param ctx
     */
    @Override
    public Object visitDatasetRef(VTLParser.DatasetRefContext ctx) {
        // Ensure data type dataset.
        Dataset dataset = checkType(ctx.getText(), visit(ctx.variable()), Dataset.class);
        return dataset;
    }
}
