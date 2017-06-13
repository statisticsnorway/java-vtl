package no.ssb.vtl.script.visitors;

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
    public Object visitVariableRef(VTLParser.VariableRefContext ctx) {
        // TODO: Would be nice to handle quote removal in ANTLR
        String key = removeQuoteIfNeeded(ctx.identifier().getText());
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
            component = checkType(ctx.getText(), visit(ctx.variableRef()), Component.class);
            this.stack.pop();
        } else {
            component = checkType(ctx.getText(), visit(ctx.variableRef()), Component.class);
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
        Dataset dataset = checkType(ctx.getText(), visit(ctx.variableRef()), Dataset.class);
        return dataset;
    }
}
